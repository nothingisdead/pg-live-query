const PGUIDs       = require('./pguids');
const EventEmitter = require('events');
const ROW_NUMBER   = Symbol('ROW_NUMBER');

// A counter for naming query tables
let ctr = 0;

// A queue for re-running queries
let queue = [];

// Keep track of which tables we've added triggers to
let triggers = {};

class QueryWatcher {
	constructor(client) {
		this.uid_col = '__id__';
		this.rev_col = '__rev__';
		this.op_col  = '__op__';
		this.rn_col  = '__rn__';
		this.uid     = new PGUIDs(client, this.uid_col, this.rev_col);
		this.client  = client;

		this.client.query('LISTEN __qw__');

		this.client.on('notification', (message) => {
			let key = message.payload;

			queue.forEach((item) => {
				item.tables[key] && ++item.stale;
			});

			this.process();
		});
	}

	// Get the selected columns from a sql statement
	cols(sql) {
		let meta = [
			this.uid_col,
			this.rev_col
		];

		let cols_sql = `
			SELECT
				*
			FROM
				(${sql}) q
			WHERE
				0 = 1
		`;

		return new Promise((resolve, reject) => {
			this.client.query(cols_sql, (error, result) => {
				if(error) {
					reject(error);
				}
				else {
					let cols = result.fields
						.filter(({ name }) => meta.indexOf(name) === -1)
						.map(({ name }) => name);

					resolve(cols);
				}
			});
		});
	}

	// Initialize a temporary table to keep track of state changes
	initializeQuery(sql) {
		return new Promise((resolve, reject) => {
			let table   = `__qw__${ctr++}`;
			let i_table = this.quote(table);

			// Create a table to keep track of state changes
			let table_sql = `
				CREATE TEMP TABLE ${i_table} (
					id TEXT NOT NULL PRIMARY KEY,
					rev BIGINT NOT NULL
				)
			`;

			this.cols(sql).then((cols) => {
				this.client.query(table_sql, (error, result) => {
					error ? reject(error) : resolve([ table, cols ]);
				});
			});
		});
	}

	// Create some triggers
	createTriggers(tables) {
		let promises = [];

		for(let i in tables) {
			if(triggers[i]) {
				promises.push(triggers[i]);
				continue;
			}

			let i_schema  = this.quote(tables[i].schema);
			let i_table   = this.quote(tables[i].table);
			let i_trigger = this.quote(`__qw__${i}`);
			let l_key     = this.quote(i, true);

			let drop_sql = `
				DROP TRIGGER IF EXISTS
					${i_trigger}
				ON
					${i_schema}.${i_table}
			`;

			let func_sql = `
				CREATE OR REPLACE FUNCTION pg_temp.${i_trigger}()
				RETURNS TRIGGER AS $$
					BEGIN
						EXECUTE pg_notify('__qw__', '${l_key}');
					RETURN NULL;
					END;
				$$ LANGUAGE plpgsql
			`;

			let create_sql = `
				CREATE TRIGGER
					${i_trigger}
				AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON
					${i_schema}.${i_table}
				EXECUTE PROCEDURE pg_temp.${i_trigger}()
			`;

			triggers[i] = new Promise((resolve, reject) => {
				this.client.query(drop_sql, (error, result) => {
					if(error) {
						reject(error);
					}
					else {
						this.client.query(func_sql, (error, result) => {
							if(error) {
								reject(error);
							}
							else {
								this.client.query(create_sql, (error, result) => {
									error ? reject(error) : resolve(result);
								});
							}
						});
					}
				});
			});

			promises.push(triggers[i]);
		}

		return Promise.all(promises);
	}

	// Process the queue
	process() {
		// Sort the queue to put the stalest queries first
		queue.sort((a, b) => b.stale - a.stale);

		// Get the stalest item
		let item = queue[0];

		// If there are no (stale) items do nothing
		if(!item || !item.stale) {
			return;
		}

		// This item is now fresh
		item.stale = 0;

		// Update the item, then process the next one
		item.update().then((changes) => {
			// Emit a data event
			item.handler.emit('changes', changes);

			changes.forEach(({ c }) => {
				if(c.data) {
					let out = {};

					// Set the row number symbol
					out[ROW_NUMBER] = c.rn;

					c.data.forEach((el, i) => {
						out[item.cols[i]] = el;
					});

					item.state[c.id] = Object.freeze(out);
				}
				else {
					delete item.state[c.id];
				}
			});

			let rows = [];

			for(let i in item.state) {
				rows.push(item.state[i]);
			}

			rows.sort((a, b) => {
				return a[ROW_NUMBER] - b[ROW_NUMBER];
			});

			item.handler.emit('rows', rows);
		}, (error) => {
			// Emit an error event
			item.handler.emit('error', error);
		}).then(this.process);
	}

	// Watch for changes to query results
	watch(sql) {
		let handler = new EventEmitter();

		// Initialize the state change table
		let promise = this.initializeQuery(sql).then(([ table, cols ]) => {
			// Add the meta columns to this query
			return this.uid.addMetaColumns(sql).then(({ sql, tables}) => {
				// Watch the tables for changes
				return this.createTriggers(tables).then(() => {
					// Start tracking changes from the beginning
					let rev = 0;

					// Create an update function for this query
					let update = () => {
						return this.update(table, cols, sql, rev).then((rows) => {
							// Update the last revision
							let tmp_rev = rows
								.map((row) => row.__rev__)
								.reduce((p, c) => Math.max(p, c), 0);

							rev = Math.max(rev, tmp_rev);

							return rows;
						});
					};

					// Initialize the query as stale
					let stale = true;

					// Initial state
					let state = {};

					// Add this query to the queue
					queue.push({
						update,
						stale,
						tables,
						cols,
						handler,
						state
					});

					// Process the queue
					this.process();
				});
			});
		});

		promise.then(() => {
			handler.emit('ready');
		}, (error) => {
			handler.emit('error', error);
		});

		return handler;
	}

	// Update query state
	update(table, cols, sql, last_rev) {
		last_rev = last_rev || 0;

		let i_table   = this.quote(table);
		let i_uid     = this.quote(this.uid.output.uid);
		let i_rev     = this.quote(this.uid.output.rev);
		let i_seq     = this.quote(this.uid.output.seq);
		let i_uid_out = this.quote(this.uid_col);
		let i_op_out  = this.quote(this.op_col);
		let i_rn_out  = this.quote(this.rn_col);
		let i_cols    = cols.map((col) => `q.${this.quote(col)}`).join(',');

		let update_sql = `
			WITH
				q AS (
					SELECT
						*,
						ROW_NUMBER() OVER() AS ${i_rn_out}
					FROM
						(${sql}) t
				),
				u AS (
					UPDATE ${i_table} SET
						rev = q.${i_rev}
					FROM
						q
					WHERE
						${i_table}.id = q.${i_uid} AND
						${i_table}.rev < q.${i_rev}
					RETURNING
						${i_table}.id,
						${i_table}.rev
				),
				d AS (
					DELETE FROM
						${i_table}
					WHERE
						NOT EXISTS(
							SELECT
								1
							FROM
								q
							WHERE
								q.${i_uid} = ${i_table}.id
						)
					RETURNING
						${i_table}.id,
						nextval('${i_seq}') AS rev
				),
				i AS (
					INSERT INTO ${i_table} (
						id,
						rev
					)
					SELECT
						${i_uid},
						${i_rev}
					FROM
						q
					WHERE
						q.${i_rev} > $1 AND
						q.${i_uid} NOT IN (select id FROM u) AND
						NOT EXISTS(
							SELECT
								1
							FROM
								${i_table} WHERE id = q.${i_uid}
						)
					RETURNING
						${i_table}.id,
						${i_table}.rev
				)
			SELECT
				jsonb_build_object(
					'id', md5(i.id),
					'rn', q.${i_rn_out},
					'data', jsonb_build_array(${i_cols})
				) AS c
			FROM
				i JOIN
				q ON
					i.id = q.${i_uid}

			UNION ALL

			SELECT
				jsonb_build_object(
					'id', md5(u.id),
					'rn', q.${i_rn_out},
					'data', jsonb_build_array(${i_cols})
				) AS c
			FROM
				u JOIN
				q ON
					u.id = q.${i_uid}

			UNION ALL

			SELECT
				jsonb_build_object(
					'id', md5(d.id)
				) AS c
			FROM
				d
		`;

		let update_query = {
			name : `__qw__${table}`,
			text : update_sql
		};

		let params = [
			last_rev
		];

		return new Promise((resolve, reject) => {
			this.client.query(update_query, params, (error, result) => {
				error ? reject(error) : resolve(result.rows);
			});
		});
	}

	// Helper function to quote identifiers
	quote() {
		return this.uid.quote.apply(this, arguments);
	}
}

module.exports = QueryWatcher;
