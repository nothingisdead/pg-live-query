var _            = require('lodash');
var EventEmitter = require('events').EventEmitter;

var querySequence   = require('./querySequence');

class RowTrigger extends EventEmitter {
	constructor(parent, table) {
		this.table = table;
		this.ready = false;

		var { channel, connect, triggerTables } = parent;

		if(!(table in triggerTables)) {
			// Create the trigger for this table on this channel
			var triggerName = `${channel}_${table}`;

			triggerTables[table] = new Promise((resolve, reject) => {
				connect((error, client, done) => {
					if(error) return this.emit('error', error);

					var sql = [
						`CREATE OR REPLACE FUNCTION ${triggerName}() RETURNS trigger AS $$
							DECLARE
								row_data RECORD;
								changed BOOLEAN;
								col TEXT;
							BEGIN
								FOR row_data IN (
									SELECT DISTINCT
										cu.query_id, ARRAY_AGG(cu.column_name) AS columns
									FROM
										_liveselect_column_usage cu
									WHERE
										cu.table_schema = TG_TABLE_SCHEMA AND
										cu.table_name = TG_TABLE_NAME AND
										EXISTS (
											SELECT ''
											FROM information_schema.views
											WHERE table_name = '_liveselect_hashes_' || cu.query_id
										)
									GROUP BY
										cu.query_id
								) LOOP
									FOREACH col IN ARRAY row_data.columns
									LOOP
										IF TG_OP = 'UPDATE' THEN
											EXECUTE
												'SELECT ($1).' || col || ' = ($2).' || col
											INTO
												changed USING NEW, OLD;
										ELSE
											changed := 1;
										END IF;
										IF changed THEN
											EXECUTE '
												DELETE FROM
													_liveselect_hashes
												WHERE
													query_id = ' || row_data.query_id || ' AND
													NOT EXISTS (
														SELECT '''' FROM _liveselect_hashes_' || row_data.query_id || ' h
														WHERE
															h.row = _liveselect_hashes.row AND
															h.hash = _liveselect_hashes.hash
													)';
											EXECUTE '
												INSERT INTO _liveselect_hashes
													(query_id, row, hash)
												SELECT
													' || row_data.query_id || ', *
												FROM
													_liveselect_hashes_' || row_data.query_id || ' h
												WHERE
													NOT EXISTS (
														SELECT '''' FROM _liveselect_hashes h2
														WHERE
															h2.row = h.row AND
															h2.hash = h.hash
													)';
											PERFORM pg_notify('${channel}', row_data.query_id::TEXT);
											EXIT;
										END IF;
									END LOOP;
								END LOOP;
								RETURN NULL;
							END;
						$$ LANGUAGE plpgsql`,
						`DROP TRIGGER IF EXISTS "${triggerName}"
							ON "${table}"`,
						`CREATE TRIGGER "${triggerName}"
							AFTER INSERT OR UPDATE OR DELETE ON "${table}"
							FOR EACH ROW EXECUTE PROCEDURE ${triggerName}()`
					];

					querySequence(client, sql, (error, results) => {
						if(error) {
							this.emit('error', error);
							reject(error);
						}

						done();
						resolve();
					});
				});
			});
		}

		triggerTables[table]
			.then(() => {
				this.ready = true;
				this.emit('ready');
			}, (error) => {
				this.emit('error', error);
			});
	}
}

module.exports = RowTrigger;
