"use strict";

var _prototypeProperties = function (child, staticProps, instanceProps) { if (staticProps) Object.defineProperties(child, staticProps); if (instanceProps) Object.defineProperties(child.prototype, instanceProps); };

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var deep = require("deep-diff");
var EventEmitter = require("events").EventEmitter;

var murmurHash = require("murmurhash-js").murmur3;
var querySequence = require("./querySequence");

// Minimum duration in milliseconds between refreshing results
// TODO: determine based on load
// https://git.focus-sis.com/beng/pg-notify-trigger/issues/6
var THROTTLE_INTERVAL = 1000;

var LiveSelect = (function (EventEmitter) {
	function LiveSelect(parent, query, params) {
		var _this = this;
		_classCallCheck(this, LiveSelect);

		var connect = parent.connect;
		var channel = parent.channel;
		var rowCache = parent.rowCache;


		this.connect = connect;
		this.rowCache = rowCache;
		this.data = [];
		this.hashes = [];
		this.ready = false;
		this.stopped = false;
		this.staticQuery = interpolate(query, params);
		this.staticHash = murmurHash(this.staticQuery);
		this.parent = parent;
		this.lastUpdate = null;
		this.params = params;

		// throttledRefresh method buffers
		this.throttledRefresh = _.debounce(this.refresh, THROTTLE_INTERVAL).bind(this);

		parent.on("update:" + this.staticHash, this.throttledRefresh);

		this.connect(function (error, client, done) {
			if (error) return _this.emit("error", error);

			init.call(_this, client, function (error, tables) {
				if (error) return _this.emit("error", error);

				_this.triggers = tables.map(function (table) {
					return parent.createTrigger(table);
				});

				var sql = "SELECT hash FROM _ls_hashes_" + _this.staticHash;

				client.query(sql, function (error, result) {
					if (error) return _this.emit("error", error);

					done();
					_this.refresh(new Date(), result.rows.map(function (row) {
						return row.hash;
					}));
					_this.ready = true;
					_this.emit("ready");
				});
			});
		});
	}

	_inherits(LiveSelect, EventEmitter);

	_prototypeProperties(LiveSelect, null, {
		refresh: {
			value: function refresh(date, hashes) {
				var _this = this;
				// Only process this update if it
				// is more recent than the last update
				if (this.lastUpdate && this.lastUpdate >= date) {
					return;
				}

				this.lastUpdate = date;

				var fetch = {};
				var diff = deep.diff(this.hashes, hashes) || [];

				this.hashes = hashes;

				var changes = diff.map(function (change) {
					var tmpChange = {};

					if (change.kind === "E") {
						_.extend(tmpChange, {
							type: "changed",
							index: change.path.pop(),
							oldHash: change.lhs,
							newHash: change.rhs
						});

						if (_this.rowCache.get(tmpChange.oldHash) === null) {
							fetch[tmpChange.oldHash] = true;
						}

						if (_this.rowCache.get(tmpChange.newHash) === null) {
							fetch[tmpChange.newHash] = true;
						}
					} else if (change.kind === "A") {
						_.extend(tmpChange, {
							index: change.index
						});

						if (change.item.kind === "N") {
							tmpChange.type = "added";
							tmpChange.hash = change.item.rhs;
						} else {
							tmpChange.type = "removed";
							tmpChange.hash = change.item.lhs;
						}

						if (_this.rowCache.get(tmpChange.hash) === null) {
							fetch[tmpChange.hash] = true;
						}
					} else {
						throw new Error("Unrecognized change: " + JSON.stringify(change));
					}

					return tmpChange;
				});

				// If there were no changes, do nothing
				if (!changes.length) {
					return;
				}

				if (_.isEmpty(fetch)) {
					this.update(changes);
				} else {
					var sql = "\n\t\t\t\tWITH tmp AS (\n\t\t\t\t\tSELECT\n\t\t\t\t\t\tMD5(CAST(t.* AS TEXT)) AS _hash,\n\t\t\t\t\t\tt.*\n\t\t\t\t\tFROM\n\t\t\t\t\t\t_ls_instance_" + this.staticHash + " t\n\t\t\t\t)\n\t\t\t\tSELECT\n\t\t\t\t\tDISTINCT *\n\t\t\t\tFROM\n\t\t\t\t\ttmp\n\t\t\t\tWHERE\n\t\t\t\t\ttmp._hash IN ('" + _.keys(fetch).join("', '") + "')\n\t\t\t";

					this.connect(function (error, client, done) {
						if (error) return _this.emit("error", error);

						// Fetch rows that have changed
						client.query(sql, function (error, result) {
							if (error) return _this.emit("error", error);

							result.rows.forEach(function (row) {
								return _this.rowCache.set(row._hash, _.omit(row, "_hash"));
							});

							done();
							_this.update(changes);
						});
					});
				}
			},
			writable: true,
			configurable: true
		},
		update: {
			value: function update(changes) {
				var _this = this;
				var add = [];
				var remove = [];

				// Emit an update event with the changes
				var changes = changes.map(function (change) {
					var args = [change.type];

					if (change.type === "added") {
						var row = _this.rowCache.get(change.hash);
						args.push(change.index, row);
						add.push(change.hash);
					} else if (change.type === "changed") {
						var oldRow = _this.rowCache.get(change.oldHash);
						var newRow = _this.rowCache.get(change.newHash);
						args.push(change.index, oldRow, newRow);
						add.push(change.newHash);
						remove.push(change.oldHash);
					} else if (change.type === "removed") {
						var row = _this.rowCache.get(change.hash);
						args.push(change.index, row);
						remove.push(change.hash);
					}

					if (args[2] === null) {
						var hash = args.length === 3 ? change.hash : change.oldHash;
						return _this.emit("error", new Error("CACHE_MISS (" + args[0] + "): " + hash));
					}
					if (args.length > 3 && args[3] === null) {
						var hash = change.newHash;
						return _this.emit("error", new Error("CACHE_MISS (" + args[0] + "): " + hash));
					}

					return args;
				});

				add.forEach(function (key) {
					return _this.rowCache.add(key);
				});
				remove.forEach(function (key) {
					return _this.rowCache.remove(key);
				});

				this.emit("update", changes);
			},
			writable: true,
			configurable: true
		},
		stop: {
			value: function stop() {
				var _this = this;
				this.connect(function (error, client, done) {
					deinit.call(_this, client, done);
				});

				this.stopped = true;
				this.hashes.forEach(function (key) {
					return _this.rowCache.remove(key);
				});
				this.removeAllListeners();
				this.parent.removeListener("update:" + this.staticHash, this.throttledRefresh);
			},
			writable: true,
			configurable: true
		}
	});

	return LiveSelect;
})(EventEmitter);

function interpolate(query, params) {
	if (!params || !params.length) return query;

	return query.replace(/\$(\d)/, function (match, index) {
		var param = params[index - 1];

		if (_.isString(param)) {
			// TODO: Need to escape quotes here!
			return "'" + param + "'";
		} else if (param instanceof Date) {
			return "'" + param.toISOString() + "'";
		} else {
			return param;
		}
	});
}

function init(client, callback) {
	var _ref = this;
	var query = _ref.query;
	var staticQuery = _ref.staticQuery;
	var staticHash = _ref.staticHash;


	var viewName = "_ls_instance_" + staticHash;
	var hashViewName = "_ls_hashes_" + staticHash;

	var sql = ["CREATE OR REPLACE VIEW " + viewName + " AS (" + staticQuery + ")", "CREATE OR REPLACE VIEW " + hashViewName + " AS (\n\t\t\tSELECT\n\t\t\t\tROW_NUMBER() OVER () AS row,\n\t\t\t\tMD5(CAST(tmp.* AS TEXT)) AS hash\n\t\t\tFROM\n\t\t\t\t(" + staticQuery + ") tmp\n\t\t)", ["SELECT DISTINCT vc.table_name\n\t\t\tFROM information_schema.view_column_usage vc\n\t\t\tWHERE view_name = $1", [viewName]], ["INSERT INTO _ls_table_usage\n\t\t\t\t(query_id, table_schema, table_name)\n\t\t\tSELECT $1, vc.table_schema, vc.table_name\n\t\t\tFROM information_schema.view_column_usage vc\n\t\t\tWHERE vc.view_name = $2\n\t\t\tGROUP BY vc.table_schema, vc.table_name", [staticHash, viewName]]];

	querySequence(client, sql, function (error, result) {
		if (error) return callback(error);

		var tables = result[2].rows.map(function (row) {
			return row.table_name;
		});

		callback(null, tables);
	});
}

function deinit(client, callback) {
	var _ref = this;
	var query = _ref.query;
	var staticQuery = _ref.staticQuery;
	var staticHash = _ref.staticHash;


	var viewName = "_ls_instance_" + staticHash;
	var hashViewName = "_ls_hashes_" + staticHash;

	var sql = ["DROP VIEW " + viewName, "DROP VIEW " + hashViewName, ["DELETE FROM _ls_table_usage\n\t\t\tWHERE query_id = $1", [staticHash]]];

	querySequence(client, sql, callback);
};

module.exports = LiveSelect;