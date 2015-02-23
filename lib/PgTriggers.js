"use strict";

var _slicedToArray = function (arr, i) { if (Array.isArray(arr)) { return arr; } else { var _arr = []; for (var _iterator = arr[Symbol.iterator](), _step; !(_step = _iterator.next()).done;) { _arr.push(_step.value); if (i && _arr.length === i) break; } return _arr; } };

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var moment = require("moment");
var EventEmitter = require("events").EventEmitter;

var querySequence = require("./querySequence");
var RowCache = require("./RowCache");
var RowTrigger = require("./RowTrigger");
var LiveSelect = require("./LiveSelect");

var messageCache = {};
var updateQueue = {};

var THROTTLE_INTERVAL = 1000;

var PgTriggers = (function (EventEmitter) {
	function PgTriggers(connect, channel) {
		_classCallCheck(this, PgTriggers);

		this.connect = connect;
		this.channel = channel;
		this.rowCache = new RowCache();
		this.triggerTables = [];
		this.instances = {};

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
				var instance = new LiveSelect(this, query, params);

				this.instances[instance.staticHash] = instance;

				return instance;
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

function update() {
	var _this = this;
	var queryHashes = _.keys(updateQueue);

	updateQueue = {};

	var sql = queryHashes.filter(function (hash) {
		return !_this.instances[hash].stopped;
	}).map(function (hash) {
		return "SELECT _ls_update_query(" + hash + ")";
	});

	if (sql.length) {
		this.getClient(function (error, client, done) {
			querySequence(client, sql);
		});
	}
}

function listen(callback) {
	var _this = this;
	var throttledUpdate = _.debounce(update, THROTTLE_INTERVAL).bind(this);

	this.getClient(function (error, client, done) {
		if (error) return _this.emit("error", error);

		client.query("LISTEN \"" + _this.channel + "\"", function (error, result) {
			if (error) throw error;
		});

		client.on("notification", function (info) {
			if (info.payload.indexOf("update:") === 0) {
				var _info$payload$split = info.payload.split(":");

				var _info$payload$split2 = _slicedToArray(_info$payload$split, 2);

				var action = _info$payload$split2[0];
				var queryHash = _info$payload$split2[1];


				updateQueue[queryHash] = true;
				throttledUpdate();
			} else {
				var _info$payload$split3 = info.payload.split("||");

				var _info$payload$split32 = _slicedToArray(_info$payload$split3, 4);

				var messageId = _info$payload$split32[0];
				var part = _info$payload$split32[1];
				var max = _info$payload$split32[2];
				var text = _info$payload$split32[3];


				if (_.isUndefined(messageCache[messageId])) {
					messageCache[messageId] = [];
				}

				messageCache[messageId][+part] = text;

				if (messageCache[messageId].length === +max + 1) {
					var message = messageCache[messageId].join("");

					var _message$split = message.split("::");

					var _message$split2 = _slicedToArray(_message$split, 3);

					var timestamp = _message$split2[0];
					var queryHash = _message$split2[1];
					var rowHashes = _message$split2[2];


					var hashes = rowHashes.split(",");
					var date = moment(timestamp).toDate();

					_this.emit("update:" + queryHash, date, hashes);

					delete messageCache[messageId];
				}

				_this.emit("change:" + info.payload);
			}
		});
	});
}

function createTables(callback) {
	var _this = this;
	var sql = ["CREATE TABLE IF NOT EXISTS _ls_table_usage (\n\t\t\tid SERIAL PRIMARY KEY,\n\t\t\tquery_id BIGINT,\n\t\t\ttable_schema VARCHAR(255),\n\t\t\ttable_name VARCHAR(255)\n\t\t)", "DROP SEQUENCE IF EXISTS \"_ls_message_seq\"", "CREATE SEQUENCE \"_ls_message_seq\"", "ALTER SEQUENCE \"_ls_message_seq\" RESTART WITH 1", "TRUNCATE TABLE _ls_table_usage", "CREATE OR REPLACE FUNCTION _ls_split_message(message TEXT) RETURNS SETOF TEXT AS $$\n\t\t\tDECLARE max_index INT;\n\t\t\tDECLARE message_id INT;\n\t\t\tDECLARE part TEXT;\n\t\t\tBEGIN\n\t\t\t\tmessage_id = NEXTVAL('_ls_message_seq');\n\t\t\t\tmax_index  = FLOOR(OCTET_LENGTH(message) / 7900);\n\n\t\t\t\tFOR i IN 0..max_index LOOP\n\t\t\t\t\tRETURN NEXT\n\t\t\t\t\t\tmessage_id || '||' || i || '||' || max_index || '||' ||\n\t\t\t\t\t\tSUBSTRING(message FROM i * 7900 FOR 7900);\n\t\t\t\tEND LOOP;\n\t\t\tEND;\n\t\t$$ LANGUAGE plpgsql", "CREATE OR REPLACE FUNCTION _ls_update_query(query_id BIGINT) RETURNS void AS $$\n\t\t\tDECLARE\n\t\t\t\thash TEXT;\n\t\t\t\tview TEXT;\n\t\t\t\tquery TEXT;\n\t\t\t\thashes TEXT;\n\t\t\t\tmessage TEXT;\n\t\t\tBEGIN\n\t\t\t\tview = '_ls_hashes_' || query_id;\n\n\t\t\t\tquery = '\n\t\t\t\t\tSELECT\n\t\t\t\t\t\tNOW() || ''::'' || $1 || ''::'' || STRING_AGG(hash, '','')\n\t\t\t\t\tFROM\n\t\t\t\t\t\t' || view::regclass;\n\n\t\t\t\tEXECUTE query INTO hashes USING query_id;\n\n\t\t\t\tFOR message IN SELECT _ls_split_message(hashes)\n\t\t\t\tLOOP\n\t\t\t\t\tPERFORM pg_notify('" + this.channel + "', message);\n\t\t\t\tEND LOOP;\n\t\t\tEND;\n\t\t$$ LANGUAGE plpgsql"];

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