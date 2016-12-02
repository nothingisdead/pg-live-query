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
			uid : `${this.uid_col}`,
			rev : `${this.rev_col}`,
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

		// Check if the sequence exists
		const seq_sql = `
			CREATE SEQUENCE IF NOT EXISTS
				${helpers.quote(this.rev_seq)}
			START WITH 1
			INCREMENT BY 1
		`;

		// Get all the existing revision functions
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
		const schema_sql = `
			SELECT current_schema
		`;

		const state = helpers.queries(this.client, [
			[ col_sql, [ this.uid_col ] ],
			[ col_sql, [ this.rev_col ] ],
			[ trigger_sql, [ this.rev_trig ] ],
			seq_sql,
			func_sql,
			schema_sql
		]);

		this.init = state.then((results) => {
			const [
				uid_result,
				rev_result,
				rev_trig_result,
				seq_result,
				func_result,
				schema_result
			] = results;

			const current_schema = schema_result.rows[0].current_schema;

			[
				[ this.uid_col, uid_result ],
				[ this.rev_col, rev_result ],
				[ this.rev_trig, rev_trig_result ],
			].forEach(([ i, result ]) => {
				// Build an index of fully-qualified table names
				const index = {};

				result.rows.forEach(({ schema, table }) => {
					if(schema === current_schema) {
						schema = null;
					}

					const key = JSON.stringify([ schema, table ]);

					index[key] = Promise.resolve(false);
				});

				indexes[i] = index;
			});

			return indexes;
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
				index[key] = helpers.query(this.client, alter_sql).catch((err) => {
					console.error("ensure col - pguid -217", err)
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
				index[key] = helpers.query(this.client, trigger_sql).catch((err) => {
					console.error("ensure trigger - pguid -247", err)
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

		const promises = [
			Promise.all(cols),
			Promise.all(triggers)
		];

		return Promise.all(promises).then(([ columns, triggers ]) => {
			return { columns, triggers };
		}).catch((err) => {
			console.error(err);
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
				let alias  = null;
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
