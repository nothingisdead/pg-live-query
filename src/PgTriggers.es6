var _            = require('lodash');
var moment       = require('moment');
var EventEmitter = require('events').EventEmitter;

var querySequence = require('./querySequence');
var RowCache      = require('./RowCache');
var RowTrigger    = require('./RowTrigger');
var LiveSelect    = require('./LiveSelect');

var messageCache = {};

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
				var [ messageId, part, max, text ] = info.payload.split('||');

				// TODO: Remove outer if statement in cleanup
				if(text) {
					if(_.isUndefined(messageCache[messageId])) {
						messageCache[messageId] = [];
					}

					messageCache[messageId][+part] = text;

					if(messageCache[messageId].length === +max + 1) {
						var message = messageCache[messageId].join('');

						var [ timestamp, queryHash, rowHashes ] = message.split('::');

						var hashes = rowHashes.split(',');
						var date   = moment(timestamp).toDate();

						this.emit(`update:${queryHash}`, date, hashes);

						delete messageCache[messageId];
					}
				}

				this.emit(`change:${info.payload}`);
			});
	});
}

function createTables(callback) {
	var sql = [
		`CREATE TABLE IF NOT EXISTS _ls_table_usage (
			id SERIAL PRIMARY KEY,
			query_id BIGINT,
			table_schema VARCHAR(255),
			table_name VARCHAR(255)
		)`,
		`DROP SEQUENCE IF EXISTS "_ls_message_seq"`,
		`CREATE SEQUENCE "_ls_message_seq"`,
		`ALTER SEQUENCE "_ls_message_seq" RESTART WITH 1`,
		`TRUNCATE TABLE _ls_table_usage`,
		`CREATE OR REPLACE FUNCTION _ls_split_message(message TEXT) RETURNS SETOF TEXT AS $$
			DECLARE max_index INT;
			DECLARE message_id INT;
			DECLARE part TEXT;
			BEGIN
				message_id = NEXTVAL('_ls_message_seq');
				max_index  = FLOOR(OCTET_LENGTH(message) / 7900);

				FOR i IN 0..max_index LOOP
					RETURN NEXT
						message_id || '||' || i || '||' || max_index || '||' ||
						SUBSTRING(message FROM i * 7900 FOR 7900);
				END LOOP;
			END;
		$$ LANGUAGE plpgsql`
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
