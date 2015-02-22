var _            = require('lodash');
var EventEmitter = require('events').EventEmitter;

var querySequence = require('./querySequence');
var RowCache      = require('./RowCache');
var RowTrigger    = require('./RowTrigger');
var LiveSelect    = require('./LiveSelect');

class PgTriggers extends EventEmitter {
	constructor(connect, channel) {
		this.connect       = connect;
		this.channel       = channel;
		this.rowCache      = new RowCache;
		this.triggerTables = [];

		this.setMaxListeners(0); // Allow unlimited listeners

		listen.call(this);
		createTables.call(this);
	}

	getClient(cb) {
		if(this.client && this.done) {
			cb(null, this.client, this.done);
		}
		else {
			this.connect((error, client, done) => {
				if(error) return this.emit('error', error);

				this.client = client;
				this.done   = done;

				cb(null, this.client, this.done);
			});
		}
	}

	createTrigger(table) {
		return new RowTrigger(this, table);
	}

	select(query, params) {
		return new LiveSelect(this, query, params);
	}

	cleanup(callback) {
		var { triggerTables, channel } = this;

		var queries = [];

		this.getClient((error, client, done) => {
			if(error) return this.emit('error', error);

			_.forOwn(triggerTables, (tablePromise, table) => {
				var triggerName = `${channel}_${table}`;

				queries.push(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
				queries.push(`DROP FUNCTION IF EXISTS ${triggerName}()`);
			});

			querySequence(client, queries, (error, result) => {
				if(error) return this.emit('error', error);

				done();

				if(_.isFunction(callback)) {
					callback(null, result);
				}
			});
		});
	}
}

function listen(callback) {
	this.getClient((error, client, done) => {
		if(error) return this.emit('error', error);

		client.query(`LISTEN "${this.channel}"`, function(error, result) {
				if(error) throw error;
			});

			client.on('notification', (info) => {
				var i = info.payload.indexOf('test:');
				if(i === 0) {
					var payload = JSON.parse(info.payload.substring(5));
					console.log('update a for ', payload.hash);
				}

				this.emit(`change:${info.payload}`);
			});
	});
}

function createTables(callback) {
	var sql = [
		`CREATE TABLE IF NOT EXISTS _liveselect_queries (
			id BIGINT PRIMARY KEY,
			query TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS _liveselect_column_usage (
			id SERIAL PRIMARY KEY,
			query_id BIGINT,
			table_schema VARCHAR(255),
			table_name VARCHAR(255),
			column_name VARCHAR(255)
		)`,
		`CREATE TABLE IF NOT EXISTS _liveselect_hashes (
			id SERIAL PRIMARY KEY,
			query_id BIGINT,
			row BIGINT,
			hash VARCHAR(255)
		)`,
		`TRUNCATE TABLE _liveselect_queries`,
		`TRUNCATE TABLE _liveselect_column_usage`,
		`CREATE OR REPLACE FUNCTION _liveselect_update() RETURNS trigger AS $$
			BEGIN
				IF TG_OP = 'DELETE' THEN
					PERFORM pg_notify('${this.channel}', 'test:' || ROW_TO_JSON(old.*));
				ELSE
					PERFORM pg_notify('${this.channel}', 'test:' || ROW_TO_JSON(new.*));
				END IF;
				RETURN NULL;
			END;
		$$ LANGUAGE plpgsql`,
		`DROP TRIGGER IF EXISTS "_liveselect_update"
			ON "_liveselect_hashes"`,
		`CREATE TRIGGER "_liveselect_update"
			AFTER INSERT OR UPDATE OR DELETE ON "_liveselect_hashes"
			FOR EACH ROW EXECUTE PROCEDURE _liveselect_update()`
	];

	this.getClient((error, client, done) => {
		if(error) return this.emit('error', error);

		querySequence(client, sql, (error, result) => {
			if(error) return this.emit('error', error);

			if(_.isFunction(callback)) {
				callback(null, result);
			}
		});
	});
}

module.exports = PgTriggers;
