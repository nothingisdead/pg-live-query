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
								hash TEXT;
								view TEXT;
								query TEXT;
								hashes TEXT;
								message TEXT;
							BEGIN
								FOR row_data IN (
									SELECT DISTINCT
										tu.query_id
									FROM
										_ls_table_usage tu
									WHERE
										tu.table_schema = TG_TABLE_SCHEMA AND
										tu.table_name = TG_TABLE_NAME AND
										EXISTS (
											SELECT ''
											FROM information_schema.views
											WHERE table_name = '_ls_hashes_' || tu.query_id
										)
								) LOOP
									view = '_ls_hashes_' || row_data.query_id;

									query = '
										SELECT
											NOW() || ''::'' || $1 || ''::'' || STRING_AGG(hash, '','')
										FROM
											' || view::regclass;

									EXECUTE query INTO hashes USING row_data.query_id;

									FOR message IN SELECT _ls_split_message(hashes)
									LOOP
										PERFORM pg_notify('${channel}', message);
									END LOOP;
									PERFORM pg_notify('${channel}', row_data.query_id::TEXT);
								END LOOP;
								RETURN NULL;
							END;
						$$ LANGUAGE plpgsql`,
						`DROP TRIGGER IF EXISTS "${triggerName}"
							ON "${table}"`,
						`CREATE TRIGGER "${triggerName}"
							AFTER INSERT OR UPDATE OR DELETE ON "${table}"
							FOR EACH STATEMENT EXECUTE PROCEDURE ${triggerName}()`
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
