const PGUIDs = require('./pguids');

// A counter for naming query tables
let ctr = 0;

class QueryWatcher {
	constructor(client) {
		this.uid    = new PGUIDs(client);
		this.client = client;
	}

	// Initialize a temporary table to keep track of state changes
	init(sql) {
		let self = this;

		return new Promise((resolve, reject) => {
			let table = `__qw__${ctr++}`;

			let table_sql = `
				CREATE TEMP TABLE ${self.quote(table)} (
					id BIGINT[][] NOT NULL PRIMARY KEY,
					rev BIGINT NOT NULL,
					op SMALLINT NULL
				)
			`;

			self.client.query(table_sql, (error, result) => {
				error ? reject(error) : resolve(table);
			});
		});
	}

	// Watch for changes to query results
	watch(sql) {
		// Make sure we have initialized the state change table
		return this.init(sql).then((table) => {
			// Add the meta columns to this query
			return this.uid.addMetaColumns(sql).then((sql) => {
				// Return an object that can be used to get the state changes
				return {
					update : this.update.bind(this, table, sql)
				};
			});
		});
	}

	// Update query state
	update(table, sql) {
		let self    = this;
		let i_table = self.quote(table);
		let i_uid   = self.quote(self.uid.output.uid);
		let i_rev   = self.quote(self.uid.output.rev);
		let i_seq   = self.quote(self.uid.output.seq);

		let update_sql = `
			WITH
				q AS (${sql}),
				u AS (
					UPDATE
						${i_table}
					SET
						rev = q.${i_rev},
						op = 2 -- UPDATE
					FROM
						q
					WHERE
						q.${i_uid} = ${i_table}.id AND
						q.${i_rev} != ${i_table}.rev
					RETURNING
						${i_table}.id,
						${i_table}.rev,
						${i_table}.op
				),
				i AS (
					INSERT INTO ${i_table} (
						id,
						rev,
						op
					)
					SELECT
						${i_uid},
						${i_rev},
						1 -- INSERT
					FROM
						q
					ON CONFLICT DO NOTHING
					RETURNING
						${i_table}.id,
						${i_table}.rev,
						${i_table}.op
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
						id,
						nextval('${i_seq}') AS rev,
						3 AS op -- DELETE
				)
			SELECT
				row_to_json(q.*)::text AS data,
				md5(COALESCE(u.id, i.id)::text) AS __id__,
				COALESCE(u.rev, i.rev) AS __rev__,
				COALESCE(u.op, i.op) AS __op__
			FROM
				q LEFT JOIN
				u ON
					q.${i_uid} = u.id LEFT JOIN
				i ON
					q.${i_uid} = i.id
			WHERE
				COALESCE(u.id, i.id) IS NOT NULL

			UNION

			SELECT
				NULL AS data,
				md5(d.id::text) AS __id__,
				d.rev AS __rev__,
				d.op AS __op__
			FROM
				d
		`;

		return new Promise((resolve, reject) => {
			self.client.query(update_sql, (error, result) => {
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
