"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var EventEmitter = require("events").EventEmitter;

var querySequence = require("./querySequence");
var RowCache = require("./RowCache");
var RowTrigger = require("./RowTrigger");
var LiveSelect = require("./LiveSelect");

var PgTriggers = (function (EventEmitter) {
	function PgTriggers(connect, channel) {
		_classCallCheck(this, PgTriggers);

		this.connect = connect;
		this.channel = channel;
		this.rowCache = new RowCache();
		this.triggerTables = [];

		this.setMaxListeners(0); // Allow unlimited listeners

		listen.call(this);
		createTables.call(this);
	}

	_inherits(PgTriggers, EventEmitter);

	_prototypeProperties(PgTriggers, null, {
		getClient: {
			value: function getClient(cb) {
				var _this = this;
				if (this.client && this.done) {
					cb(null, this.client, this.done);
				} else {
					this.connect(function (error, client, done) {
						if (error) return _this.emit("error", error);

						_this.client = client;
						_this.done = done;

						cb(null, _this.client, _this.done);
					});
				}
			},
			writable: true,
			configurable: true
		},
		createTrigger: {
			value: function createTrigger(table) {
				return new RowTrigger(this, table);
			},
			writable: true,
			configurable: true
		},
		select: {
			value: function select(query, params) {
				return new LiveSelect(this, query, params);
			},
			writable: true,
			configurable: true
		},
		cleanup: {
			value: function cleanup(callback) {
				var _this = this;
				var _ref = this;
				var triggerTables = _ref.triggerTables;
				var channel = _ref.channel;


				var queries = [];

				this.getClient(function (error, client, done) {
					if (error) return _this.emit("error", error);

					_.forOwn(triggerTables, function (tablePromise, table) {
						var triggerName = "" + channel + "_" + table;

						queries.push("DROP TRIGGER IF EXISTS " + triggerName + " ON " + table);
						queries.push("DROP FUNCTION IF EXISTS " + triggerName + "()");
					});

					querySequence(client, queries, function (error, result) {
						if (error) return _this.emit("error", error);

						done();

						if (_.isFunction(callback)) {
							callback(null, result);
						}
					});
				});
			},
			writable: true,
			configurable: true
		}
	});

	return PgTriggers;
})(EventEmitter);

function listen(callback) {
	var _this = this;
	this.getClient(function (error, client, done) {
		if (error) return _this.emit("error", error);

		client.query("LISTEN \"" + _this.channel + "\"", function (error, result) {
			if (error) throw error;
		});

		client.on("notification", function (info) {
			var i = info.payload.indexOf("test:");
			if (i === 0) {
				var payload = JSON.parse(info.payload.substring(5));
				console.log("update a for ", payload.hash);
			}

			_this.emit("change:" + info.payload);
		});
	});
}

function createTables(callback) {
	var _this = this;
	var sql = ["CREATE TABLE IF NOT EXISTS _liveselect_queries (\n\t\t\tid BIGINT PRIMARY KEY,\n\t\t\tquery TEXT\n\t\t)", "CREATE TABLE IF NOT EXISTS _liveselect_column_usage (\n\t\t\tid SERIAL PRIMARY KEY,\n\t\t\tquery_id BIGINT,\n\t\t\ttable_schema VARCHAR(255),\n\t\t\ttable_name VARCHAR(255),\n\t\t\tcolumn_name VARCHAR(255)\n\t\t)", "CREATE TABLE IF NOT EXISTS _liveselect_hashes (\n\t\t\tid SERIAL PRIMARY KEY,\n\t\t\tquery_id BIGINT,\n\t\t\trow BIGINT,\n\t\t\thash VARCHAR(255)\n\t\t)", "TRUNCATE TABLE _liveselect_queries", "TRUNCATE TABLE _liveselect_column_usage", "CREATE OR REPLACE FUNCTION _liveselect_update() RETURNS trigger AS $$\n\t\t\tBEGIN\n\t\t\t\tIF TG_OP = 'DELETE' THEN\n\t\t\t\t\tPERFORM pg_notify('" + this.channel + "', 'test:' || ROW_TO_JSON(old.*));\n\t\t\t\tELSE\n\t\t\t\t\tPERFORM pg_notify('" + this.channel + "', 'test:' || ROW_TO_JSON(new.*));\n\t\t\t\tEND IF;\n\t\t\t\tRETURN NULL;\n\t\t\tEND;\n\t\t$$ LANGUAGE plpgsql", "DROP TRIGGER IF EXISTS \"_liveselect_update\"\n\t\t\tON \"_liveselect_hashes\"", "CREATE TRIGGER \"_liveselect_update\"\n\t\t\tAFTER INSERT OR UPDATE OR DELETE ON \"_liveselect_hashes\"\n\t\t\tFOR EACH ROW EXECUTE PROCEDURE _liveselect_update()"];

	this.getClient(function (error, client, done) {
		if (error) return _this.emit("error", error);

		querySequence(client, sql, function (error, result) {
			if (error) return _this.emit("error", error);

			if (_.isFunction(callback)) {
				callback(null, result);
			}
		});
	});
}

module.exports = PgTriggers;