var EventEmitter = require('events').EventEmitter
var _            = require('lodash')
var murmurHash   = require('murmurhash-js').murmur3
var sqlParser    = require('sql-parser')

var common     = require('./common')

// Number of milliseconds between refreshes
const THROTTLE_INTERVAL = 500

class LiveSQL extends EventEmitter {
	constructor(connStr, channel) {
		this.connStr         = connStr
		this.channel         = channel
		this.notifyHandle    = null
		this.updateInterval  = null
		this.waitingToUpdate = []
		this.selectBuffer    = []
		this.tablesUsed      = []
		this.queryDetailsCache = []
		// DEBUG HELPER
		this.refreshCount    = 0
		this.notifyCount     = 0

		this.ready = this.init()
	}

	getQueryBuffer(queryHash) {
		var queryBuffer = this.selectBuffer.filter(buffer =>
			buffer.hash === queryHash)

		if(queryBuffer.length !== 0)
			return queryBuffer[0]

		return null
	}

	getDetailsCache(query) {
		var detailsCache = this.queryDetailsCache.filter(cache =>
			cache.query === query)

		if(detailsCache.length !== 0)
			return detailsCache[0]

		return null
	}

	getTableQueries(table) {
		var tableQueries = this.tablesUsed.filter(item =>
			item.table === table)

		if(tableQueries.length !== 0)
			return tableQueries[0]

		return null
	}

	async init() {
		this.notifyHandle = await common.getClient(this.connStr)

		await common.performQuery(this.notifyHandle.client,
			`LISTEN "${this.channel}"`)

		this.notifyHandle.client.on('notification', info => {
			if(info.channel === this.channel) {
				this.notifyCount++

				try {
					// See common.createTableTrigger() for payload definition
					var payload = JSON.parse(info.payload)
				} catch(error) {
					return this.emit('error',
						new Error('INVALID_NOTIFICATION ' + info.payload))
				}

				let tableQueries = this.getTableQueries(payload.table)
				if(tableQueries !== null) {
					for(let queryHash of tableQueries.queries) {
						let queryBuffer = this.getQueryBuffer(queryHash)
						if((queryBuffer.triggers
								// Check for true response from manual trigger
								&& payload.table in queryBuffer.triggers
								&& (payload.op === 'UPDATE'
									? queryBuffer.triggers[payload.table](payload.new_data[0])
										|| queryBuffer.triggers[payload.table](payload.old_data[0])
									: queryBuffer.triggers[payload.table](payload.data[0])))
							|| (queryBuffer.triggers
								// No manual trigger for this table
								&& !(payload.table in  queryBuffer.triggers))
							|| !queryBuffer.triggers) {

							if(queryBuffer.parsed !== null) {
								queryBuffer.notifications.push(payload)
							}

							this.waitingToUpdate.push(queryHash)
						}
					}
				}
			}
		})

		this.updateInterval = setInterval(() => {
			let queriesToUpdate =
				_.uniq(this.waitingToUpdate.splice(0, this.waitingToUpdate.length))
			this.refreshCount += queriesToUpdate.length

			for(let queryHash of queriesToUpdate) {
				this._updateQuery(queryHash)
			}
		}.bind(this), THROTTLE_INTERVAL)
	}

	async select(query, params, onUpdate, triggers) {
		// Allow omission of params argument
		if(typeof params === 'function' && typeof onUpdate === 'undefined') {
			triggers = onUpdate
			onUpdate = params
			params = []
		}

		if(typeof query !== 'string')
			throw new Error('QUERY_STRING_MISSING')
		if(!(params instanceof Array))
			throw new Error('PARAMS_ARRAY_MISMATCH')
		if(typeof onUpdate !== 'function')
			throw new Error('UPDATE_FUNCTION_MISSING')

		let queryHash = murmurHash(JSON.stringify([ query, params ]))
		let queryBuffer = this.getQueryBuffer(queryHash)

		if(queryBuffer !== null) {
			queryBuffer.handlers.push(onUpdate)

			if(queryBuffer.data.length !== 0) {
				// Initial results from cache
				onUpdate(
					{ removed: null, moved: null, copied: null, added: queryBuffer.data },
					queryBuffer.data)
			}
		}
		else {
			// Initialize result set cache
			let newBuffer = {
				query,
				params,
				triggers,
				hash          : queryHash,
				data          : [],
				handlers      : [ onUpdate ],
				// Queries that have parsed property are simple and may be updated
				//  without re-running the query
				parsed        : null,
				notifications : []
			}

			this.selectBuffer.push(newBuffer)

			let pgHandle = await common.getClient(this.connStr)
			let detailsCache = this.getDetailsCache(query)
			let queryDetails

			if(detailsCache !== null) {
				queryDetails = detailsCache.data
			}
			else {
				queryDetails = await common.getQueryDetails(pgHandle.client, query)

				this.queryDetailsCache.push({
					query,
					data: queryDetails
				})
			}

			if(queryDetails.isUpdatable) {
				// Query parser does not support tab characters
				let cleanQuery = query.replace(/\t/g, ' ')
				try {
					newBuffer.parsed = sqlParser.parse(cleanQuery)
				} catch(error) {
					// Not a serious error, fallback to using full refreshing
				}

				// OFFSET and GROUP BY not supported with simple queries
				if(newBuffer.parsed
					&& ((newBuffer.parsed.limit && newBuffer.parsed.limit.offset)
						|| newBuffer.parsed.group)) {
					newBuffer.parsed = null
				}
			}

			for(let table of queryDetails.tablesUsed) {
				let tableQueries = this.getTableQueries(table)
				if(tableQueries === null) {
					this.tablesUsed.push({
						table,
						queries: [ queryHash ]
					})
					await common.createTableTrigger(pgHandle.client, table, this.channel)
				}
				else if(tableQueries.queries.indexOf(queryHash) === -1) {
					tableQueries.queries.push(queryHash)
				}
			}

			pgHandle.done()

			// Retrieve initial results
			this.waitingToUpdate.push(queryHash)
		}

		let stop = async function() {
			let queryBuffer = this.getQueryBuffer(queryHash)

			if(queryBuffer) {
				_.pull(queryBuffer.handlers, onUpdate)

				if(queryBuffer.handlers.length === 0) {
					// No more query/params like this, remove from buffers
					_.pull(this.selectBuffer, queryBuffer)
					_.pull(this.waitingToUpdate, queryHash)

					for(let item of this.tablesUsed) {
						_.pull(item.queries, queryHash)
					}
				}
			}

		}.bind(this)

		return { stop }
	}

	async _updateQuery(queryHash) {
		let pgHandle = await common.getClient(this.connStr)

		let queryBuffer = this.getQueryBuffer(queryHash)
		let update
		if(queryBuffer.parsed !== null
			// Notifications array will be empty for initial results
			&& queryBuffer.notifications.length !== 0) {

			update = await common.getDiffFromSupplied(
				pgHandle.client,
				queryBuffer.data,
				queryBuffer.notifications.splice(0, queryBuffer.notifications.length),
				queryBuffer.query,
				queryBuffer.parsed,
				queryBuffer.params
			)
		}
		else{
			update = await common.getResultSetDiff(
				pgHandle.client,
				queryBuffer.data,
				queryBuffer.query,
				queryBuffer.params
			)
		}

		pgHandle.done()

		if(update !== null) {
			queryBuffer.data = update.data

			for(let updateHandler of queryBuffer.handlers) {
				updateHandler(
					filterHashProperties(update.diff), filterHashProperties(update.data))
			}
		}
	}

	async cleanup() {
		this.notifyHandle.done()

		clearInterval(this.updateInterval)

		let pgHandle = await common.getClient(this.connStr)

		for(let item of this.tablesUsed) {
			await common.dropTableTrigger(pgHandle.client, item.table, this.channel)
		}

		pgHandle.done()
	}
}

module.exports = LiveSQL

function filterHashProperties(diff) {
	if(diff instanceof Array) {
		return diff.map(event => {
			return _.omit(event, '_hash')
		})
	}
	// Otherwise, diff is object with arrays for keys
	_.forOwn(diff, (rows, key) => {
		diff[key] = filterHashProperties(rows)
	})
	return diff
}
