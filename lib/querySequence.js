"use strict";

// Execute a sequence of queries on a database connection
// @param {object} client - The database client
// @param {boolean} debug - Print queries as they execute (optional)
// @param {[string]} queries - Queries to execute, in order
// @param {function} callback - Call when complete (error, results)
module.exports = function (client, debug, queries, callback) {
	if (debug instanceof Array) {
		callback = queries;
		queries = debug;
		debug = false;
	}

	if (queries.length === 0) {
		if (typeof callback === "function") {
			return callback();
		} else {
			return;
		}
	}

	var results = [];

	client.query("BEGIN", function (error, result) {
		if (error) {
			if (typeof callback === "function") {
				return callback(error);
			} else {
				throw new Error(error);
			}
		}

		var sequence = queries.map(function (query, index, initQueries) {
			var tmpCallback = function (error, rows, fields) {
				if (error) {
					client.query("ROLLBACK", function (rollbackError, result) {
						if (typeof callback === "function") {
							callback(rollbackError || error);
						} else if (rollbackError || error) {
							throw new Error(rollbackError || error);
						}
					});
				}

				results.push(rows);

				if (index < sequence.length - 1) {
					sequence[index + 1]();
				} else {
					client.query("COMMIT", function (error, result) {
						if (error) {
							if (typeof callback === "function") {
								return callback(error);
							} else {
								throw new Error(error);
							}
						}

						if (typeof callback === "function") {
							return callback(null, results);
						} else {
							return results;
						}
					});
				}
			};

			return function () {
				debug && console.log("Query Sequence", index, query);

				if (query instanceof Array) {
					client.query(query[0], query[1], tmpCallback);
				} else {
					client.query(query, tmpCallback);
				}
			};
		});

		sequence[0]();
	});
};