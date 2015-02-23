var _            = require('lodash');
var deep         = require('deep-diff');
var EventEmitter = require('events').EventEmitter;

var murmurHash    = require('murmurhash-js').murmur3;
var querySequence = require('./querySequence');

// Minimum duration in milliseconds between refreshing results
// TODO: determine based on load
// https://git.focus-sis.com/beng/pg-notify-trigger/issues/6
const THROTTLE_INTERVAL = 1000;

class LiveSelect extends EventEmitter {
	constructor(parent, query, params) {
		var { connect, channel, rowCache } = parent;

		this.connect     = connect;
		this.rowCache    = rowCache;
		this.data        = [];
		this.hashes      = [];
		this.ready       = false;
		this.stopped     = false;
		this.staticQuery = interpolate(query, params);
		this.staticHash  = murmurHash(this.staticQuery);
		this.parent      = parent;
		this.lastUpdate  = null;
		this.params      = params;

		// throttledRefresh method buffers
		this.throttledRefresh = _.debounce(this.refresh, THROTTLE_INTERVAL).bind(this);

		parent.on(`update:${this.staticHash}`, this.throttledRefresh);

		this.connect((error, client, done) => {
			if(error) return this.emit('error', error);

			init.call(this, client, (error, tables) => {
				if(error) return this.emit('error', error);

				this.triggers = tables.map(table => parent.createTrigger(table));

				var sql = `SELECT hash FROM _ls_hashes_${this.staticHash}`;

				client.query(sql, (error, result) => {
					if(error) return this.emit('error', error);

					done();
					this.refresh(new Date(), result.rows.map(row => row.hash));
					this.ready = true;
					this.emit('ready');
				});
			});
		});
	}

	refresh(date, hashes) {
		// Only process this update if it
		// is more recent than the last update
		if(this.lastUpdate && this.lastUpdate >= date) {
			return;
		}

		this.lastUpdate = date;

		var fetch = {};
		var diff  = deep.diff(this.hashes, hashes) || [];

		this.hashes = hashes;

		var changes = diff.map(change => {
			var tmpChange = {};

			if(change.kind === 'E') {
				_.extend(tmpChange, {
					type    : 'changed',
					index   : change.path.pop(),
					oldHash : change.lhs,
					newHash : change.rhs
				});

				if(this.rowCache.get(tmpChange.oldHash) === null) {
					fetch[tmpChange.oldHash] = true;
				}

				if(this.rowCache.get(tmpChange.newHash) === null) {
					fetch[tmpChange.newHash] = true;
				}
			}
			else if(change.kind === 'A') {
				_.extend(tmpChange, {
					index : change.index
				})

				if(change.item.kind === 'N') {
					tmpChange.type = 'added';
					tmpChange.hash  = change.item.rhs;
				}
				else {
					tmpChange.type = 'removed';
					tmpChange.hash  = change.item.lhs;
				}

				if(this.rowCache.get(tmpChange.hash) === null) {
					fetch[tmpChange.hash] = true;
				}
			}
			else {
				throw new Error(`Unrecognized change: ${JSON.stringify(change)}`);
			}

			return tmpChange;
		});

		// If there were no changes, do nothing
		if(!changes.length) {
			return;
		}

		if(_.isEmpty(fetch)) {
			this.update(changes);
		}
		else {
			var sql = `
				WITH tmp AS (
					SELECT
						MD5(CAST(t.* AS TEXT)) AS _hash,
						t.*
					FROM
						_ls_instance_${this.staticHash} t
				)
				SELECT
					DISTINCT *
				FROM
					tmp
				WHERE
					tmp._hash IN ('${_.keys(fetch).join("', '")}')
			`;

			this.connect((error, client, done) => {
				if(error) return this.emit('error', error);

				// Fetch rows that have changed
				client.query(sql, (error, result) => {
					if(error) return this.emit('error', error);

					result.rows.forEach(row =>
						this.rowCache.set(row._hash, _.omit(row, '_hash')));

					done();
					this.update(changes);
				});
			});
		}
	}

	update(changes) {
		var add    = [];
		var remove = [];

		// Emit an update event with the changes
		var changes = changes.map(change => {
			var args = [change.type];

			if(change.type === 'added') {
				var row = this.rowCache.get(change.hash);
				args.push(change.index, row);
				add.push(change.hash);
			}
			else if(change.type === 'changed') {
				var oldRow = this.rowCache.get(change.oldHash);
				var newRow = this.rowCache.get(change.newHash);
				args.push(change.index, oldRow, newRow);
				add.push(change.newHash);
				remove.push(change.oldHash);
			}
			else if(change.type === 'removed') {
				var row = this.rowCache.get(change.hash);
				args.push(change.index, row);
				remove.push(change.hash);
			}

			if(args[2] === null){
				var hash = (args.length === 3 ? change.hash : change.oldHash);
				return this.emit('error',
					new Error(`CACHE_MISS (${args[0]}): ${hash}`));
			}
			if(args.length > 3 && args[3] === null){
				var hash = change.newHash;
				return this.emit('error',
					new Error(`CACHE_MISS (${args[0]}): ${hash}`));
			}

			return args;
		});

		add.forEach(key => this.rowCache.add(key));
		remove.forEach(key => this.rowCache.remove(key));

		this.emit('update', changes);
	}

	stop() {
		this.connect((error, client, done) => {
			deinit.call(this, client, done);
		});

		this.stopped = true;
		this.hashes.forEach(key => this.rowCache.remove(key));
		this.removeAllListeners();
		this.parent.removeListener(
			`update:${this.staticHash}`, this.throttledRefresh);
	}
}

function interpolate(query, params) {
	if(!params || !params.length) return query;

	return query.replace(/\$(\d)/, (match, index) => {
		var param = params[index - 1];

		if(_.isString(param)) {
			// TODO: Need to escape quotes here!
			return `'${param}'`;
		}
		else if(param instanceof Date) {
			return `'${param.toISOString()}'`;
		}
		else {
			return param;
		}
	});
}

function init(client, callback){
	var { query, staticQuery, staticHash } = this;

	var viewName     = `_ls_instance_${staticHash}`;
	var hashViewName = `_ls_hashes_${staticHash}`;

	var sql = [
		`CREATE OR REPLACE VIEW ${viewName} AS (${staticQuery})`,
		`CREATE OR REPLACE VIEW ${hashViewName} AS (
			SELECT
				ROW_NUMBER() OVER () AS row,
				MD5(CAST(tmp.* AS TEXT)) AS hash
			FROM
				(${staticQuery}) tmp
		)`,
		[`SELECT DISTINCT vc.table_name
			FROM information_schema.view_column_usage vc
			WHERE view_name = $1`, [ viewName ] ],
		[`INSERT INTO _ls_table_usage
				(query_id, table_schema, table_name)
			SELECT $1, vc.table_schema, vc.table_name
			FROM information_schema.view_column_usage vc
			WHERE vc.view_name = $2
			GROUP BY vc.table_schema, vc.table_name`, [ staticHash, viewName ] ]
	];

	querySequence(client, sql, (error, result) => {
		if(error) return callback(error);

		var tables = result[2].rows.map(row => row.table_name);

		callback(null, tables);
	});
}

function deinit(client, callback) {
	var { query, staticQuery, staticHash } = this;

	var viewName     = `_ls_instance_${staticHash}`;
	var hashViewName = `_ls_hashes_${staticHash}`;

	var sql = [
		`DROP VIEW ${viewName}`,
		`DROP VIEW ${hashViewName}`,
		[`DELETE FROM _ls_table_usage
			WHERE query_id = $1`, [ staticHash ] ],
	];

	querySequence(client, sql, callback);
};

module.exports = LiveSelect;
