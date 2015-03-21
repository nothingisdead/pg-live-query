"use strict";

var _classCallCheck = require("babel-runtime/helpers/class-call-check")["default"];

var _inherits = require("babel-runtime/helpers/inherits")["default"];

var _createClass = require("babel-runtime/helpers/create-class")["default"];

var _core = require("babel-runtime/core-js")["default"];

var _regeneratorRuntime = require("babel-runtime/regenerator")["default"];

var EventEmitter = require("events").EventEmitter;
var _ = require("lodash");
var murmurHash = require("murmurhash-js").murmur3;
var sqlParser = require("sql-parser");

var common = require("./common");

// Number of milliseconds between refreshes
var THROTTLE_INTERVAL = 500;

var LiveSQL = (function (_EventEmitter) {
	function LiveSQL(connStr, channel) {
		_classCallCheck(this, LiveSQL);

		this.connStr = connStr;
		this.channel = channel;
		this.notifyHandle = null;
		this.updateInterval = null;
		this.waitingToUpdate = [];
		this.selectBuffer = [];
		this.tablesUsed = [];
		this.queryDetailsCache = [];
		// DEBUG HELPER
		this.refreshCount = 0;
		this.notifyCount = 0;

		this.ready = this.init();
	}

	_inherits(LiveSQL, _EventEmitter);

	_createClass(LiveSQL, {
		getQueryBuffer: {
			value: function getQueryBuffer(queryHash) {
				var queryBuffer = this.selectBuffer.filter(function (buffer) {
					return buffer.hash === queryHash;
				});

				if (queryBuffer.length !== 0) {
					return queryBuffer[0];
				}return null;
			}
		},
		getDetailsCache: {
			value: function getDetailsCache(query) {
				var detailsCache = this.queryDetailsCache.filter(function (cache) {
					return cache.query === query;
				});

				if (detailsCache.length !== 0) {
					return detailsCache[0];
				}return null;
			}
		},
		getTableQueries: {
			value: function getTableQueries(table) {
				var tableQueries = this.tablesUsed.filter(function (item) {
					return item.table === table;
				});

				if (tableQueries.length !== 0) {
					return tableQueries[0];
				}return null;
			}
		},
		init: {
			value: function init() {
				var _this = this;

				return _regeneratorRuntime.async(function init$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							context$2$0.next = 2;
							return common.getClient(_this.connStr);

						case 2:
							_this.notifyHandle = context$2$0.sent;
							context$2$0.next = 5;
							return common.performQuery(_this.notifyHandle.client, "LISTEN \"" + _this.channel + "\"");

						case 5:

							_this.notifyHandle.client.on("notification", function (info) {
								if (info.channel === _this.channel) {
									_this.notifyCount++;

									try {
										// See common.createTableTrigger() for payload definition
										var payload = JSON.parse(info.payload);
									} catch (error) {
										return _this.emit("error", new Error("INVALID_NOTIFICATION " + info.payload));
									}

									var tableQueries = _this.getTableQueries(payload.table);
									if (tableQueries !== null) {
										var _iteratorNormalCompletion = true;
										var _didIteratorError = false;
										var _iteratorError = undefined;

										try {
											for (var _iterator = _core.$for.getIterator(tableQueries.queries), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
												var queryHash = _step.value;

												var queryBuffer = _this.getQueryBuffer(queryHash);
												if (queryBuffer.triggers
												// Check for true response from manual trigger
												 && payload.table in queryBuffer.triggers && (payload.op === "UPDATE" ? queryBuffer.triggers[payload.table](payload.new_data[0]) || queryBuffer.triggers[payload.table](payload.old_data[0]) : queryBuffer.triggers[payload.table](payload.data[0])) || queryBuffer.triggers
												// No manual trigger for this table
												 && !(payload.table in queryBuffer.triggers) || !queryBuffer.triggers) {

													if (queryBuffer.parsed !== null) {
														queryBuffer.notifications.push(payload);
													}

													_this.waitingToUpdate.push(queryHash);
												}
											}
										} catch (err) {
											_didIteratorError = true;
											_iteratorError = err;
										} finally {
											try {
												if (!_iteratorNormalCompletion && _iterator["return"]) {
													_iterator["return"]();
												}
											} finally {
												if (_didIteratorError) {
													throw _iteratorError;
												}
											}
										}
									}
								}
							});

							_this.updateInterval = setInterval((function () {
								var queriesToUpdate = _.uniq(_this.waitingToUpdate.splice(0, _this.waitingToUpdate.length));
								_this.refreshCount += queriesToUpdate.length;

								var _iteratorNormalCompletion = true;
								var _didIteratorError = false;
								var _iteratorError = undefined;

								try {
									for (var _iterator = _core.$for.getIterator(queriesToUpdate), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
										var queryHash = _step.value;

										_this._updateQuery(queryHash);
									}
								} catch (err) {
									_didIteratorError = true;
									_iteratorError = err;
								} finally {
									try {
										if (!_iteratorNormalCompletion && _iterator["return"]) {
											_iterator["return"]();
										}
									} finally {
										if (_didIteratorError) {
											throw _iteratorError;
										}
									}
								}
							}).bind(_this), THROTTLE_INTERVAL);

						case 7:
						case "end":
							return context$2$0.stop();
					}
				}, null, this);
			}
		},
		select: {
			value: function select(query, params, onUpdate, triggers) {
				var _this = this;

				var queryHash, queryBuffer, newBuffer, pgHandle, detailsCache, queryDetails, cleanQuery, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, table, tableQueries, stop;

				return _regeneratorRuntime.async(function select$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							// Allow omission of params argument
							if (typeof params === "function" && typeof onUpdate === "undefined") {
								triggers = onUpdate;
								onUpdate = params;
								params = [];
							}

							if (!(typeof query !== "string")) {
								context$2$0.next = 3;
								break;
							}

							throw new Error("QUERY_STRING_MISSING");

						case 3:
							if (params instanceof Array) {
								context$2$0.next = 5;
								break;
							}

							throw new Error("PARAMS_ARRAY_MISMATCH");

						case 5:
							if (!(typeof onUpdate !== "function")) {
								context$2$0.next = 7;
								break;
							}

							throw new Error("UPDATE_FUNCTION_MISSING");

						case 7:
							queryHash = murmurHash(JSON.stringify([query, params]));
							queryBuffer = _this.getQueryBuffer(queryHash);

							if (!(queryBuffer !== null)) {
								context$2$0.next = 14;
								break;
							}

							queryBuffer.handlers.push(onUpdate);

							if (queryBuffer.data.length !== 0) {
								// Initial results from cache
								onUpdate({ removed: null, moved: null, copied: null, added: queryBuffer.data }, queryBuffer.data);
							}
							context$2$0.next = 64;
							break;

						case 14:
							newBuffer = {
								query: query,
								params: params,
								triggers: triggers,
								hash: queryHash,
								data: [],
								handlers: [onUpdate],
								// Queries that have parsed property are simple and may be updated
								//  without re-running the query
								parsed: null,
								notifications: []
							};

							_this.selectBuffer.push(newBuffer);

							context$2$0.next = 18;
							return common.getClient(_this.connStr);

						case 18:
							pgHandle = context$2$0.sent;
							detailsCache = _this.getDetailsCache(query);
							queryDetails = undefined;

							if (!(detailsCache !== null)) {
								context$2$0.next = 25;
								break;
							}

							queryDetails = detailsCache.data;
							context$2$0.next = 29;
							break;

						case 25:
							context$2$0.next = 27;
							return common.getQueryDetails(pgHandle.client, query);

						case 27:
							queryDetails = context$2$0.sent;

							_this.queryDetailsCache.push({
								query: query,
								data: queryDetails
							});

						case 29:

							if (queryDetails.isUpdatable) {
								cleanQuery = query.replace(/\t/g, " ");

								try {
									newBuffer.parsed = sqlParser.parse(cleanQuery);
								} catch (error) {}

								// OFFSET and GROUP BY not supported with simple queries
								if (newBuffer.parsed && (newBuffer.parsed.limit && newBuffer.parsed.limit.offset || newBuffer.parsed.group)) {
									newBuffer.parsed = null;
								}
							}

							_iteratorNormalCompletion = true;
							_didIteratorError = false;
							_iteratorError = undefined;
							context$2$0.prev = 33;
							_iterator = _core.$for.getIterator(queryDetails.tablesUsed);

						case 35:
							if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
								context$2$0.next = 48;
								break;
							}

							table = _step.value;
							tableQueries = _this.getTableQueries(table);

							if (!(tableQueries === null)) {
								context$2$0.next = 44;
								break;
							}

							_this.tablesUsed.push({
								table: table,
								queries: [queryHash]
							});
							context$2$0.next = 42;
							return common.createTableTrigger(pgHandle.client, table, _this.channel);

						case 42:
							context$2$0.next = 45;
							break;

						case 44:
							if (tableQueries.queries.indexOf(queryHash) === -1) {
								tableQueries.queries.push(queryHash);
							}

						case 45:
							_iteratorNormalCompletion = true;
							context$2$0.next = 35;
							break;

						case 48:
							context$2$0.next = 54;
							break;

						case 50:
							context$2$0.prev = 50;
							context$2$0.t1 = context$2$0["catch"](33);
							_didIteratorError = true;
							_iteratorError = context$2$0.t1;

						case 54:
							context$2$0.prev = 54;
							context$2$0.prev = 55;

							if (!_iteratorNormalCompletion && _iterator["return"]) {
								_iterator["return"]();
							}

						case 57:
							context$2$0.prev = 57;

							if (!_didIteratorError) {
								context$2$0.next = 60;
								break;
							}

							throw _iteratorError;

						case 60:
							return context$2$0.finish(57);

						case 61:
							return context$2$0.finish(54);

						case 62:

							pgHandle.done();

							// Retrieve initial results
							_this.waitingToUpdate.push(queryHash);

						case 64:
							stop = (function callee$2$0() {
								var _this2 = this;

								var queryBuffer, _iteratorNormalCompletion2, _didIteratorError2, _iteratorError2, _iterator2, _step2, item;

								return _regeneratorRuntime.async(function callee$2$0$(context$3$0) {
									while (1) switch (context$3$0.prev = context$3$0.next) {
										case 0:
											queryBuffer = _this2.getQueryBuffer(queryHash);

											if (!queryBuffer) {
												context$3$0.next = 25;
												break;
											}

											_.pull(queryBuffer.handlers, onUpdate);

											if (!(queryBuffer.handlers.length === 0)) {
												context$3$0.next = 25;
												break;
											}

											// No more query/params like this, remove from buffers
											_.pull(_this2.selectBuffer, queryBuffer);
											_.pull(_this2.waitingToUpdate, queryHash);

											_iteratorNormalCompletion2 = true;
											_didIteratorError2 = false;
											_iteratorError2 = undefined;
											context$3$0.prev = 9;
											for (_iterator2 = _core.$for.getIterator(_this2.tablesUsed); !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
												item = _step2.value;

												_.pull(item.queries, queryHash);
											}
											context$3$0.next = 17;
											break;

										case 13:
											context$3$0.prev = 13;
											context$3$0.t0 = context$3$0["catch"](9);
											_didIteratorError2 = true;
											_iteratorError2 = context$3$0.t0;

										case 17:
											context$3$0.prev = 17;
											context$3$0.prev = 18;

											if (!_iteratorNormalCompletion2 && _iterator2["return"]) {
												_iterator2["return"]();
											}

										case 20:
											context$3$0.prev = 20;

											if (!_didIteratorError2) {
												context$3$0.next = 23;
												break;
											}

											throw _iteratorError2;

										case 23:
											return context$3$0.finish(20);

										case 24:
											return context$3$0.finish(17);

										case 25:
										case "end":
											return context$3$0.stop();
									}
								}, null, this, [[9, 13, 17, 25], [18,, 20, 24]]);
							}).bind(_this);

							return context$2$0.abrupt("return", { stop: stop });

						case 66:
						case "end":
							return context$2$0.stop();
					}
				}, null, this, [[33, 50, 54, 62], [55,, 57, 61]]);
			}
		},
		_updateQuery: {
			value: function _updateQuery(queryHash) {
				var _this = this;

				var pgHandle, queryBuffer, update, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, updateHandler;

				return _regeneratorRuntime.async(function _updateQuery$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							context$2$0.next = 2;
							return common.getClient(_this.connStr);

						case 2:
							pgHandle = context$2$0.sent;
							queryBuffer = _this.getQueryBuffer(queryHash);
							update = undefined;

							if (!(queryBuffer.parsed !== null
							// Notifications array will be empty for initial results
							 && queryBuffer.notifications.length !== 0)) {
								context$2$0.next = 11;
								break;
							}

							context$2$0.next = 8;
							return common.getDiffFromSupplied(pgHandle.client, queryBuffer.data, queryBuffer.notifications.splice(0, queryBuffer.notifications.length), queryBuffer.query, queryBuffer.parsed, queryBuffer.params);

						case 8:
							update = context$2$0.sent;
							context$2$0.next = 14;
							break;

						case 11:
							context$2$0.next = 13;
							return common.getResultSetDiff(pgHandle.client, queryBuffer.data, queryBuffer.query, queryBuffer.params);

						case 13:
							update = context$2$0.sent;

						case 14:

							pgHandle.done();

							if (!(update !== null)) {
								context$2$0.next = 36;
								break;
							}

							queryBuffer.data = update.data;

							_iteratorNormalCompletion = true;
							_didIteratorError = false;
							_iteratorError = undefined;
							context$2$0.prev = 20;
							for (_iterator = _core.$for.getIterator(queryBuffer.handlers); !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
								updateHandler = _step.value;

								updateHandler(filterHashProperties(update.diff), filterHashProperties(update.data));
							}
							context$2$0.next = 28;
							break;

						case 24:
							context$2$0.prev = 24;
							context$2$0.t2 = context$2$0["catch"](20);
							_didIteratorError = true;
							_iteratorError = context$2$0.t2;

						case 28:
							context$2$0.prev = 28;
							context$2$0.prev = 29;

							if (!_iteratorNormalCompletion && _iterator["return"]) {
								_iterator["return"]();
							}

						case 31:
							context$2$0.prev = 31;

							if (!_didIteratorError) {
								context$2$0.next = 34;
								break;
							}

							throw _iteratorError;

						case 34:
							return context$2$0.finish(31);

						case 35:
							return context$2$0.finish(28);

						case 36:
						case "end":
							return context$2$0.stop();
					}
				}, null, this, [[20, 24, 28, 36], [29,, 31, 35]]);
			}
		},
		cleanup: {
			value: function cleanup() {
				var _this = this;

				var pgHandle, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, item;

				return _regeneratorRuntime.async(function cleanup$(context$2$0) {
					while (1) switch (context$2$0.prev = context$2$0.next) {
						case 0:
							_this.notifyHandle.done();

							clearInterval(_this.updateInterval);

							context$2$0.next = 4;
							return common.getClient(_this.connStr);

						case 4:
							pgHandle = context$2$0.sent;
							_iteratorNormalCompletion = true;
							_didIteratorError = false;
							_iteratorError = undefined;
							context$2$0.prev = 8;
							_iterator = _core.$for.getIterator(_this.tablesUsed);

						case 10:
							if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
								context$2$0.next = 17;
								break;
							}

							item = _step.value;
							context$2$0.next = 14;
							return common.dropTableTrigger(pgHandle.client, item.table, _this.channel);

						case 14:
							_iteratorNormalCompletion = true;
							context$2$0.next = 10;
							break;

						case 17:
							context$2$0.next = 23;
							break;

						case 19:
							context$2$0.prev = 19;
							context$2$0.t3 = context$2$0["catch"](8);
							_didIteratorError = true;
							_iteratorError = context$2$0.t3;

						case 23:
							context$2$0.prev = 23;
							context$2$0.prev = 24;

							if (!_iteratorNormalCompletion && _iterator["return"]) {
								_iterator["return"]();
							}

						case 26:
							context$2$0.prev = 26;

							if (!_didIteratorError) {
								context$2$0.next = 29;
								break;
							}

							throw _iteratorError;

						case 29:
							return context$2$0.finish(26);

						case 30:
							return context$2$0.finish(23);

						case 31:

							pgHandle.done();

						case 32:
						case "end":
							return context$2$0.stop();
					}
				}, null, this, [[8, 19, 23, 31], [24,, 26, 30]]);
			}
		}
	});

	return LiveSQL;
})(EventEmitter);

module.exports = LiveSQL;

function filterHashProperties(diff) {
	if (diff instanceof Array) {
		return diff.map(function (event) {
			return _.omit(event, "_hash");
		});
	}
	// Otherwise, diff is object with arrays for keys
	_.forOwn(diff, function (rows, key) {
		diff[key] = filterHashProperties(rows);
	});
	return diff;
}

// Initialize result set cache

// Query parser does not support tab characters

// Not a serious error, fallback to using full refreshing