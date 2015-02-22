"use strict";

var _inherits = function (subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } };

var _ = require("lodash");
var EventEmitter = require("events").EventEmitter;

var querySequence = require("./querySequence");

var RowTrigger = (function (EventEmitter) {
	function RowTrigger(parent, table) {
		var _this = this;
		_classCallCheck(this, RowTrigger);

		this.table = table;
		this.ready = false;

		var channel = parent.channel;
		var connect = parent.connect;
		var triggerTables = parent.triggerTables;


		if (!(table in triggerTables)) {
			// Create the trigger for this table on this channel
			var triggerName = "" + channel + "_" + table;

			triggerTables[table] = new Promise(function (resolve, reject) {
				connect(function (error, client, done) {
					if (error) return _this.emit("error", error);

					var sql = ["CREATE OR REPLACE FUNCTION " + triggerName + "() RETURNS trigger AS $$\n\t\t\t\t\t\t\tDECLARE\n\t\t\t\t\t\t\t\trow_data RECORD;\n\t\t\t\t\t\t\t\tchanged BOOLEAN;\n\t\t\t\t\t\t\t\tcol TEXT;\n\t\t\t\t\t\t\tBEGIN\n\t\t\t\t\t\t\t\tFOR row_data IN (\n\t\t\t\t\t\t\t\t\tSELECT DISTINCT\n\t\t\t\t\t\t\t\t\t\tcu.query_id, ARRAY_AGG(cu.column_name) AS columns\n\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t_liveselect_column_usage cu\n\t\t\t\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\t\t\t\tcu.table_schema = TG_TABLE_SCHEMA AND\n\t\t\t\t\t\t\t\t\t\tcu.table_name = TG_TABLE_NAME AND\n\t\t\t\t\t\t\t\t\t\tEXISTS (\n\t\t\t\t\t\t\t\t\t\t\tSELECT ''\n\t\t\t\t\t\t\t\t\t\t\tFROM information_schema.views\n\t\t\t\t\t\t\t\t\t\t\tWHERE table_name = '_liveselect_hashes_' || cu.query_id\n\t\t\t\t\t\t\t\t\t\t)\n\t\t\t\t\t\t\t\t\tGROUP BY\n\t\t\t\t\t\t\t\t\t\tcu.query_id\n\t\t\t\t\t\t\t\t) LOOP\n\t\t\t\t\t\t\t\t\tFOREACH col IN ARRAY row_data.columns\n\t\t\t\t\t\t\t\t\tLOOP\n\t\t\t\t\t\t\t\t\t\tIF TG_OP = 'UPDATE' THEN\n\t\t\t\t\t\t\t\t\t\t\tEXECUTE\n\t\t\t\t\t\t\t\t\t\t\t\t'SELECT ($1).' || col || ' = ($2).' || col\n\t\t\t\t\t\t\t\t\t\t\tINTO\n\t\t\t\t\t\t\t\t\t\t\t\tchanged USING NEW, OLD;\n\t\t\t\t\t\t\t\t\t\tELSE\n\t\t\t\t\t\t\t\t\t\t\tchanged := 1;\n\t\t\t\t\t\t\t\t\t\tEND IF;\n\t\t\t\t\t\t\t\t\t\tIF changed THEN\n\t\t\t\t\t\t\t\t\t\t\tEXECUTE '\n\t\t\t\t\t\t\t\t\t\t\t\tDELETE FROM\n\t\t\t\t\t\t\t\t\t\t\t\t\t_liveselect_hashes\n\t\t\t\t\t\t\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\t\t\t\t\t\t\tquery_id = ' || row_data.query_id || ' AND\n\t\t\t\t\t\t\t\t\t\t\t\t\tNOT EXISTS (\n\t\t\t\t\t\t\t\t\t\t\t\t\t\tSELECT '''' FROM _liveselect_hashes_' || row_data.query_id || ' h\n\t\t\t\t\t\t\t\t\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\th.row = _liveselect_hashes.row AND\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\th.hash = _liveselect_hashes.hash\n\t\t\t\t\t\t\t\t\t\t\t\t\t)';\n\t\t\t\t\t\t\t\t\t\t\tEXECUTE '\n\t\t\t\t\t\t\t\t\t\t\t\tINSERT INTO _liveselect_hashes\n\t\t\t\t\t\t\t\t\t\t\t\t\t(query_id, row, hash)\n\t\t\t\t\t\t\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\t\t\t\t\t\t\t' || row_data.query_id || ', *\n\t\t\t\t\t\t\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\t\t\t\t\t\t\t_liveselect_hashes_' || row_data.query_id || ' h\n\t\t\t\t\t\t\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\t\t\t\t\t\t\tNOT EXISTS (\n\t\t\t\t\t\t\t\t\t\t\t\t\t\tSELECT '''' FROM _liveselect_hashes h2\n\t\t\t\t\t\t\t\t\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\th2.row = h.row AND\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\th2.hash = h.hash\n\t\t\t\t\t\t\t\t\t\t\t\t\t)';\n\t\t\t\t\t\t\t\t\t\t\tPERFORM pg_notify('" + channel + "', row_data.query_id::TEXT);\n\t\t\t\t\t\t\t\t\t\t\tEXIT;\n\t\t\t\t\t\t\t\t\t\tEND IF;\n\t\t\t\t\t\t\t\t\tEND LOOP;\n\t\t\t\t\t\t\t\tEND LOOP;\n\t\t\t\t\t\t\t\tRETURN NULL;\n\t\t\t\t\t\t\tEND;\n\t\t\t\t\t\t$$ LANGUAGE plpgsql", "DROP TRIGGER IF EXISTS \"" + triggerName + "\"\n\t\t\t\t\t\t\tON \"" + table + "\"", "CREATE TRIGGER \"" + triggerName + "\"\n\t\t\t\t\t\t\tAFTER INSERT OR UPDATE OR DELETE ON \"" + table + "\"\n\t\t\t\t\t\t\tFOR EACH ROW EXECUTE PROCEDURE " + triggerName + "()"];

					querySequence(client, sql, function (error, results) {
						if (error) {
							_this.emit("error", error);
							reject(error);
						}

						done();
						resolve();
					});
				});
			});
		}

		triggerTables[table].then(function () {
			_this.ready = true;
			_this.emit("ready");
		}, function (error) {
			_this.emit("error", error);
		});
	}

	_inherits(RowTrigger, EventEmitter);

	return RowTrigger;
})(EventEmitter);

module.exports = RowTrigger;