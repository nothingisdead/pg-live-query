const PGUIDs = require('./pguids');

// A counter for naming query tables
let ctr = 0;

class QueryWatcher {
	constructor(client) {
		this.uid_col = '__id__';
		this.rev_col = '__rev__';
		this.op_col  = '__op__';
		this.uid     = new PGUIDs(client, this.uid_col, this.rev_col);
		this.client  = client;
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
	init(sql) {
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

	// Watch for changes to query results
	watch(sql) {
		let rev = 0;

		// Make sure we have initialized the state change table
		return this.init(sql).then(([ table, cols ]) => {
			// Add the meta columns to this query
			return this.uid.addMetaColumns(sql).then((sql) => {
				// Create an update function for this query
				let update = this.update.bind(this, table, cols, sql);

				// Return an object that can be used to get the state changes
				return {
					update : () => {
						return update(rev).then((rows) => {
							// Update the last revision
							let tmp_rev = rows
								.map((row) => row.__rev__)
								.reduce((p, c) => Math.max(p, c), 0);

							rev = Math.max(rev, tmp_rev);

							return rows;
						});
					}
				};
			});
		});
	}

	// Update query state
	update(table, cols, sql, last_rev) {
		last_rev = last_rev || 0;

		let i_table   = this.quote(table);
		let i_uid     = this.quote(this.uid.output.uid);
		let i_rev     = this.quote(this.uid.output.rev);
		let i_seq     = this.quote(this.uid.output.seq);
		let i_uid_out = this.quote(this.uid_col);
		let i_rev_out = this.quote(this.rev_col);
		let i_op_out  = this.quote(this.op_col);
		let i_cols    = cols.map((col) => `q.${this.quote(col)}`).join(',');

		let update_sql = `
			WITH
				q AS (${sql}),
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
				jsonb_build_array(${i_cols}) AS data,
				md5(i.id) AS ${i_uid_out},
				i.rev AS ${i_rev_out},
				1 AS ${i_op_out}
			FROM
				i JOIN
				q ON
					i.id = q.${i_uid}

			UNION ALL

			SELECT
				jsonb_build_array(${i_cols}) AS data,
				md5(u.id) AS ${i_uid_out},
				u.rev AS ${i_rev_out},
				2 AS ${i_op_out}
			FROM
				u JOIN
				q ON
					u.id = q.${i_uid}

			UNION ALL

			SELECT
				NULL AS data,
				md5(d.id) AS ${i_uid_out},
				d.rev AS ${i_rev_out},
				3 AS ${i_op_out}
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
