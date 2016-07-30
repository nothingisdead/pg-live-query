const parser  = require('pg-query-parser');
const helpers = require('./helpers');

// Static indexes that track which tables have the meta objects
const indexes = {};

class PGUIDs {
	constructor(client, uid_col, rev_col) {
		this.client   = client;
		this.uid_col  = uid_col || '__id__';
		this.rev_col  = rev_col || '__rev__';
		this.rev_seq  = `${this.rev_col}_sequence`;
		this.rev_func = `${this.rev_col}_update`;
		this.rev_trig = `${this.rev_col}_trigger`;

		this.output = {
			uid : `${this.uid_col}'`,
			rev : `${this.rev_col}'`,
			seq : this.rev_seq
		};

		// Get all the existing uid/rev columns
		const col_sql = `
			SELECT
				n.nspname AS schema,
				c.relname AS table
			FROM
				pg_attribute a JOIN
				pg_class c ON
					c.oid = a.attrelid JOIN
				pg_namespace n ON
					n.oid = c.relnamespace
			WHERE
				a.attnum > 0 AND
				c.relkind = 'r' AND
				a.attname = $1 AND
				NOT a.attisdropped
			;
		`;

		// Get all the existing triggers
		const trigger_sql = `
			SELECT
				c.relname AS table,
				n.nspname AS schema
			FROM
				pg_trigger t JOIN
				pg_class c ON
					c.oid = t.tgrelid JOIN
				pg_namespace n ON
					n.oid = c.relnamespace
			WHERE
				t.tgname = $1
		`;

		const seq_sql = `
			CREATE SEQUENCE IF NOT EXISTS
				${helpers.quote(this.rev_seq)}
			START WITH 1
			INCREMENT BY 1
		`;

		const func_sql = `
			CREATE OR REPLACE FUNCTION ${helpers.quote(this.rev_func)}()
			RETURNS trigger AS $$
				BEGIN
					NEW.${helpers.quote(this.rev_col)} :=
						nextval('${helpers.quote(this.rev_seq)}');
					RETURN NEW;
				END;
			$$ LANGUAGE plpgsql
		`;

		// Get the current schema
		const schema = new Promise((resolve, reject) => {
			const schema_sql = `
				SELECT current_schema
			`;

			this.client.query(schema_sql, (error, result) => {
				error ? reject(error) : resolve(result.rows[0].current_schema);
			});
		});

		this.init = schema.then((current_schema) => {
			// Build the uid/rev column index
			const promises = [ this.uid_col, this.rev_col ].map((col) => {
				return new Promise((resolve, reject) => {
					client.query(col_sql, [ col ], (error, result) => {
						if(error) {
							reject(error);
						}
						else {
							// Build an index of fully-qualified table names
							const index = {};

							result.rows.forEach(({ schema, table }) => {
								if(schema === current_schema) {
									schema = null;
								}

								const key = JSON.stringify([ schema, table ]);

								index[key] = Promise.resolve(false);
							});

							resolve(index);
						}
					});
				});
			});

			// Build the rev trigger index
			promises.push(new Promise((resolve, reject) => {
				client.query(trigger_sql, [ this.rev_trig ], (error, result) => {
					if(error) {
						reject(error);
					}
					else {
						// Build an index of fully-qualified table names
						const index = {};

						result.rows.forEach(({ schema, table }) => {
							if(schema === current_schema) {
								schema = null;
							}

							const key = JSON.stringify([ schema, table ]);

							index[key] = Promise.resolve(false);
						});

						resolve(index);
					}
				});
			}));

			// Create the rev sequence
			promises.push(new Promise((resolve, reject) => {
				client.query(seq_sql, (error, result) => {
					error ? reject(error) : resolve();
				});
			}));

			// Create the rev function
			promises.push(new Promise((resolve, reject) => {
				client.query(func_sql, (error, result) => {
					error ? reject(error) : resolve();
				});
			}));

			// Wait for all the promises
			return Promise.all(promises).then((results) => {
				[ this.uid_col, this.rev_col, this.rev_trig ].forEach((type, i) => {
					if(!indexes[type]) {
						indexes[type] = {};
					}

					Object.assign(indexes[type], results[i]);
				});

				return indexes;
			});
		});
	}

	// Add meta columns to a query
	addMetaColumns(sql) {
		const parsed = parser.parse(sql);
		const tree   = parsed.query;

		if(!tree.length) {
			return Promise.reject(parsed.error);
		}

		// Ensure that the necessary database objects have been created
		return this.ensureObjects(tree).then(() => {
			// Add the uid and rev columns to the parse tree
			this._addMetaColumns(tree);

			// Deparse the parse tree back into a query
			return {
				sql    : parser.deparse(tree),
				tables : this.getTables(tree)
			};
		});
	}

	// Add meta columns to a parse tree
	_addMetaColumns(tree) {
		for(const i in tree) {
			const node = tree[i];

			// If this is not an object, we are not interested in it
			if(typeof node !== 'object') {
				continue;
			}

			// Add some columns to select statements
			if(node && node.SelectStmt) {
				const select = node.SelectStmt;

				if(select.fromClause) {
					// Check if the columns need to be aggregated
					const grouped = !!select.groupClause;

					// Get all the top-level tables in this select statement
					const tables = this.getTables(select.fromClause, true, true);

					// Create a node to select the aggregate revision
					const rev_node = helpers.nodes.compositeRevNode(
						tables,
						grouped,
						this.rev_col,
						this.output.rev
					);

					// Create a node to select the aggregate UID
					const uid_node = helpers.nodes.compositeUidNode(
						tables,
						grouped,
						this.uid_col,
						this.output.uid
					);

					select.targetList.unshift(rev_node);
					select.targetList.unshift(uid_node);
				}
			}

			// Check the child nodes
			this._addMetaColumns(node);
		}
	}

	// Create a column if it doesn't exist
	ensureCol(table, col, key) {
		const type = col === this.uid_col ? 'BIGSERIAL' : 'BIGINT';

		const default_str = col === this.rev_col ? `
			DEFAULT nextval('${helpers.quote(this.rev_seq)}')
		` : '';

		// Make sure the initial objects have been created
		return this.init.then((indexes) => {
			const index = indexes[col] || {};

			const alter_sql = `
				ALTER TABLE
					${helpers.tableRef(table)}
				ADD COLUMN
					${helpers.quote(col)} ${type} ${default_str}
			`;

			if(!index[key]) {
				index[key] = new Promise((resolve, reject) => {
					this.client.query(alter_sql, (error, result) => {
						error ? reject(error) : resolve(true);
					});
				});
			}

			return index[key].then((created) => {
				return {
					table   : table,
					column  : col,
					created : created
				};
			});
		});
	}

	// Make sure there is a trigger for this table
	ensureTrigger(table, key) {
		const trigger_sql = `
			CREATE TRIGGER
				${helpers.quote(this.rev_trig)}
			BEFORE INSERT OR UPDATE ON
				${helpers.tableRef(table)}
			FOR EACH ROW EXECUTE PROCEDURE ${helpers.quote(this.rev_func)}();
		`;

		// Make sure the initial objects have been created
		return this.init.then((indexes) => {
			const index = indexes[this.rev_trig];

			if(!index[key]) {
				index[key] = new Promise((resolve, reject) => {
					this.client.query(trigger_sql, (error, result) => {
						error ? reject(error) : resolve(true);
					});
				});
			}

			return index[key].then((created) => {
				return {
					table   : table,
					created : created
				};
			});
		});
	}

	// Ensure that all the referenced tables have uid/rev columns/triggers
	ensureObjects(tree) {
		const tables = this.getTables(tree);

		// Create the uid/rev columns
		const cols = [ this.uid_col, this.rev_col ].map((col) => {
			const cols = [];

			for(const key in tables) {
				cols.push(this.ensureCol(tables[key], col, key));
			}

			return cols;
		}).reduce((p, c) => p.concat(c));

		// Create the rev triggers
		const triggers = [];

		for(const key in tables) {
			triggers.push(this.ensureTrigger(tables[key], key));
		}

		return Promise.all(cols).then((columns) => {
			return Promise.all(triggers).then((triggers) => {
				return { columns, triggers };
			});
		});
	}

	// Get all the referenced tables from a parse tree
	getTables(tree, top_level, subselects) {
		const tables = {};

		for(const i in tree) {
			// If this node is not an object, we don't care about it
			if(typeof tree[i] !== 'object') {
				continue;
			}

			// If we're only getting top-level tables, ignore subqueries
			if(top_level && i === 'SelectStmt') {
				continue;
			}

			// If we're including tables from subselects and this is a subselect node
			if(subselects && i === 'RangeSubselect') {
				const table    = null;
				const schema   = null;
				const alias    = tree[i].alias.Alias.aliasname;
				const key      = JSON.stringify([ -1, alias ]);

				tables[key] = { table, schema, alias };
			}

			// If this is a table node
			if(i === 'RangeVar') {
				const table  = tree[i].relname;
				const schema = tree[i].schemaname || null;
				const alias  = null;
				const key    = JSON.stringify([ schema, table ]);

				if(tree[i].alias) {
					alias = tree[i].alias.Alias.aliasname;
				}

				tables[key] = { table, schema, alias };
			}
			else {
				Object.assign(tables, this.getTables(tree[i], top_level, subselects));
			}
		}

		return tables;
	}
}

module.exports = PGUIDs;
