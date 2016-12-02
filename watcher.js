const PGUIDs       = require('./pguids');
const EventEmitter = require('events');
const helpers      = require('./helpers');

const EVENTS = {
	1 : 'insert',
	2 : 'update',
	3 : 'delete'
};

// A counter for naming query tables
let ctr = 0;

// A queue for re-running queries
const queue = [];

class Watcher {
	constructor(client, uid_col, rev_col) {
		this.uid_col = uid_col || '__id__';
		this.rev_col = rev_col || '__rev__';
		this.uid     = new PGUIDs(client, this.uid_col, this.rev_col);
		this.client  = client;

		// Keep track of which tables we've added triggers
		// to (currently not shared between instances)
		this.triggers = {};

		helpers.query(this.client, 'LISTEN __qw__').catch((err) => {
			console.error("watcher listen -29", err)
		});
		const watcher = this;
		this.client.on('notification', (message) => {
			const key = message.payload;

			queue.forEach((item) => {
				item.tables[key] && ++item.stale;
			});

			watcher.process();
		});
	}

	// Get the selected columns from a sql statement
	cols(sql) {
		const meta = [
			this.uid_col,
			this.rev_col
		];

		const cols_sql = `
			SELECT
				*
			FROM
				(${sql}) q
			WHERE
				0 = 1
		`;

		return helpers.query(this.client, cols_sql)
			.then((result) => {
				return result.fields
					.filter(({ name }) => meta.indexOf(name) === -1)
					.map(({ name }) => name);
			}).catch((err) => {
				console.error("get sql from selected", err)
			});
	}

	// Initialize a temporary table to keep track of state changes
	initializeQuery(sql) {
		const table   = `__qw__${ctr++}`;
		const i_table = helpers.quote(table);

		// Create a table to keep track of state changes
		const table_sql = `
			CREATE TEMP TABLE ${i_table} (
				id TEXT NOT NULL,
				rev BIGINT NOT NULL
			)
		`;

		const promises = [
			this.cols(sql),
			helpers.query(this.client, table_sql)
		];

		return Promise.all(promises).then(([ cols ]) => [ table, cols ]).catch((err) => {
			console.error("watcher, initialize query ", err);
		});
	}

	// Create some triggers
	createTriggers(tables) {
		const promises = [];

		for(const i in tables) {
			if(!this.triggers[i]) {
				const i_table   = helpers.tableRef(tables[i]);
				const i_trigger = helpers.quote(`__qw__${i}`);
				const l_key     = helpers.quote(i, true);

				const drop_sql = `
					DROP TRIGGER IF EXISTS
						${i_trigger}
					ON
						${i_table}
				`;

				const func_sql = `
					CREATE OR REPLACE FUNCTION pg_temp.${i_trigger}()
					RETURNS TRIGGER AS $$
						BEGIN
							EXECUTE pg_notify('__qw__', '${l_key}');
						RETURN NULL;
						END;
					$$ LANGUAGE plpgsql
				`;

				const create_sql = `
					CREATE TRIGGER
						${i_trigger}
					AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON
						${i_table}
					EXECUTE PROCEDURE pg_temp.${i_trigger}()
				`;

				this.triggers[i] = helpers.queries(this.client, [
					drop_sql,
					func_sql,
					create_sql
				]);
			}

			promises.push(this.triggers[i]);
		}

		return Promise.all(promises).catch((err) => {
			console.error(err);
		});
	}

	// Process the queue
	process() {
		// Sort the queue to put the stalest queries first
		queue.sort((a, b) => b.stale - a.stale);

		// Get the stalest item
		const item = queue[0];

		// If there are no (stale) items do nothing
		if(!item || !item.stale) {
			return;
		}

		// This item is now fresh
		item.stale = 0;

		// Update the item, then process the next one
		item.update().then((changes) => {
			if(!changes.length) {
				return;
			}

			// Emit individual events and map the
			// change rows to a more sensible format
			changes = changes.map(({ c }) => {
				// Emit an event for this change
				item.handler.emit(EVENTS[c.op], c.id, c.data, item.cols);

				return c;
			});

			// Emit a 'changes' event
			item.handler.emit('changes', changes, item.cols);
		}, (error) => {
			// Emit an 'error' event
			item.handler.emit('error', error);
		}).then(this.process).catch((err) => {
			console.error("raw error in process queue", err)
		});
	}

	// Watch for changes to query results
	watch(sql) {
		const handler = new EventEmitter();

		const pre_init = [
			this.initializeQuery(sql),
			this.uid.addMetaColumns(sql)
		];

		const post_init = Promise.all(pre_init).then((results) => {
			const [
				[ state_table, cols ],
				{ sql, tables }
			] = results;

			return this.createTriggers(tables).then(() => {
				return {
					state_table,
					cols,
					sql,
					tables
				};
			});
		});

		const promise = post_init.then((results) => {
			// Destructure all the results
			const {
				state_table,
				cols,
				sql,
				tables
			} = results;

			// Start tracking changes from the beginning
			let rev = 0;

			// Create an update function specific to this query
			const _update = this.update.bind(this, state_table, cols, sql);

			const update = () => {
				return _update(rev).then((rows) => {
					// Update the last revision
					const tmp_rev = rows
						.map((row) => row.__rev__)
						.reduce((p, c) => Math.max(p, c), 0);

					rev = Math.max(rev, tmp_rev);

					return rows;
				});
			};

			// Initialize the query as stale
			const stale = true;

			// Initial state
			const state = {};

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

		// Quote a bunch of identifiers
		const i_table   = helpers.quote(table);
		const i_uid     = helpers.quote(this.uid.output.uid);
		const i_rev     = helpers.quote(this.uid.output.rev);
		const i_seq     = helpers.quote(this.uid.output.seq);
		const i_uid_out = helpers.quote(this.uid_col);
		const i_rn_out  = helpers.quote('~~~rn~~~');
		const i_cols    = cols.map((col) => `q.${helpers.quote(col)}`).join(',');

		// Where all the magic happens
		const update_sql = `
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
						q.${i_uid},
						q.${i_rev}
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
					'id', i.id,
					'op', 1, -- INSERT
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
					'id', u.id,
					'op', 2, -- UPDATE
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
					'id', d.id,
					'op', 3 -- DELETE
				) AS c
			FROM
				d
		`;

		const update_query = {
			name : `__qw__${table}`,
			text : update_sql
		};

		const params = [
			last_rev
		];

		const promise = helpers.query(this.client, update_query, params).catch((err) => {
			console.error("update query state", err)
		});

		return promise.then((result) => {
			return result.rows;
		});
	}
}

module.exports = Watcher;
