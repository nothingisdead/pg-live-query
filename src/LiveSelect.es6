var EventEmitter = require('events').EventEmitter;
var _            = require('lodash');

var murmurHash    = require('../dist/murmurhash3_gc');
var querySequence = require('./querySequence');
var cachedQueries = {};

// Minimum duration in milliseconds between refreshing results
// TODO: determine based on load
//  https://git.focus-sis.com/beng/pg-notify-trigger/issues/6
const THROTTLE_INTERVAL = 200;
// Number of potential refresh conditions before ignoring THROTTLE_INTERVAL
//  and refreshing immediately in order to avoid call stack overrun
const MAX_CONDITIONS    = 3500;
// Specify prefix for aliased columns in result set to a string that
//  starts with underscore or letter and is unique from any column prefix
//  existing in the schema
const ALIAS_PREFIX      = '_0';
// Specify "_id" meta column to avoid conflicts
const ID_ALIAS      = '_id';

class LiveSelect extends EventEmitter {
  constructor(parent, query, params) {
    var { client, channel } = parent;

    this.params = params || [];
    this.client = client;
    this.data   = {};
    this.ready  = false;

    // throttledRefresh method buffers
    this.refreshQueue     = [];
    this.throttledRefresh = _.debounce(this.refresh, THROTTLE_INTERVAL);

    // Create view for this query
    addHelpers.call(this, query, (error, result) => {
      if(error) return this.emit('error', error);

      var triggers     = {};
      var aliases      = {};
      var primary_keys = result.keys;

      result.columns.forEach((col) => {
        if(!triggers[col.table]) {
          triggers[col.table] = [];
        }

        if(!aliases[col.table]) {
          aliases[col.table] = {};
        }

        triggers[col.table].push(col.name);
        aliases[col.table][col.name] = col.alias;
      });

      this.triggers = _.map(triggers,
        (columns, table) => parent.createTrigger(table, columns));

      this.aliases = aliases;
      this.query   = result.query;

      this.listen();

      // Grab initial results
      this.refresh(true);
    });
  }

  listen() {
    this.triggers.forEach((trigger) => {
      trigger.on('change', (payload) => {
        // Update events contain both old and new values in payload
        // using 'new_' and 'old_' prefixes on the column names
        var argVals = {};

        if(payload._op === 'UPDATE') {
          trigger.payloadColumns.forEach((col) => {
            if(payload[`new_${col}`] !== payload[`old_${col}`]) {
              argVals[col] = payload[`new_${col}`];
            }
          });
        }
        else {
          trigger.payloadColumns.forEach((col) => {
            argVals[col] = payload[col];
          });
        }

        // Generate a map denoting which rows to replace
        var tmpRow = {};

        _.forOwn(argVals, (value, column) => {
          var alias = this.aliases[trigger.table][column];
          tmpRow[alias] = value;
        });

        if(!_.isEmpty(tmpRow)) {
          this.refreshQueue.push(tmpRow);

          if(MAX_CONDITIONS && this.refreshQueue.length >= MAX_CONDITIONS) {
            this.refresh();
          }
          else {
            this.throttledRefresh();
          }
        }
      });

      trigger.on('ready', (results) => {
        // Check if all handlers are ready
        if(this.triggers.filter(trigger => !trigger.ready).length === 0) {
          this.ready = true;
          this.emit('ready', results);
        }
      });
    });
  }

  refresh(initial) {
    var params = this.params.slice(), where;

    if(initial) {
      where = '';
    }
    else if(this.refreshQueue.length) {
      // Build WHERE clause if not refreshing entire result set
      var valueCount = params.length;

      where = 'WHERE ' +
        this.refreshQueue.map((condition) => '(' +
          _.map(condition, (value, column) => {
            params.push(value);
            return `${column} = $${++valueCount}`
          }).join(' AND ') + ')'
        ).join(' OR ');

      this.refreshQueue = [];
    }
    else {
      return; // Do nothing if there are no conditions
    }

    var sql = `
      WITH tmp AS (${this.query})
      SELECT *
      FROM tmp
      ${where}
    `;

    this.client.query(sql, params, (error, result) =>  {
      if(error) return this.emit('error', error);

      this.update(result.rows);
    });
  }

  update(rows) {
    var diff = [];

    // Handle added/changed rows
    rows.forEach((row) => {
      var id = row[ID_ALIAS];

      if(this.data[id]) {
        // If this row existed in the result set,
        // check to see if anything has changed
        var hasDiff = false;

        for(var col in this.data[id]) {
          if(this.data[id][col] !== row[col]) {
            hasDiff = true;
            break;
          }
        }

        hasDiff && diff.push([
          'changed',
          filterAliasedProperties(this.data[id]),
          filterAliasedProperties(row)
        ]);
      }
      else {
        // Otherwise, it was added
        diff.push(['added', filterAliasedProperties(row)]);
      }

      this.data[id] = row;
    });

    // Check to see if there are any
    // IDs that have been removed
    var existingIds = _.keys(this.data);

    if(existingIds.length) {
      var sql = `
        WITH tmp AS (${this.query})
        SELECT id
        FROM UNNEST(ARRAY['${_.keys(this.data).join("', '")}']) id
        LEFT JOIN tmp ON tmp.${ID_ALIAS} = id
        WHERE tmp.${ID_ALIAS} IS NULL
      `;

      var query = {
        name   : `prepared_${murmurHash(sql)}`,
        text   : sql,
        values : this.params
      };

      // Get any IDs that have been removed
      this.client.query(query, (error, result) => {
        if(error) return this.emit('error', error);

        result.rows.forEach((row) => {
          var oldRow = this.data[row.id];

          diff.push(['removed', filterAliasedProperties(oldRow)]);
          delete this.data[row.id];
        });

        if(diff.length !== 0){
          // Output all difference events in a single event
          this.emit('update', diff, dataToRows(this.data));
        }
      });
    }
    else if(diff.length !== 0){
      // Output all difference events in a single event
      this.emit('update', diff, dataToRows(this.data));
    }
  }

  flush() {
    if(this.refreshQueue.length) {
      refresh(this.refreshQueue);
      this.refreshQueue = [];
    }
  }
}

function filterAliasedProperties(row) {
  return _.pick(row, (value, key) =>
    key.substr(0, ALIAS_PREFIX.length + 1) !== ALIAS_PREFIX + '_')
}

function dataToRows(data) {
  return _.map(data, row => filterAliasedProperties(row))
}

/**
 * Adds helper columns to a query
 * @context LiveSelect instance
 * @param   String   query    The query
 * @param   Function callback A function that is called with information about the view
 */
function addHelpers(query, callback) {
  var hash = murmurHash(query);

  // If this query was cached before, reuse it
  if(cachedQueries[hash]) {
    return callback(null, cachedQueries[hash]);
  }

  var tmpName = `tmp_view_${hash}`;

  var columnUsageSQL = `
    SELECT DISTINCT
      vc.table_name,
      vc.column_name
    FROM
      information_schema.view_column_usage vc
    WHERE
      view_name = $1
  `;

  var tableUsageSQL = `
    SELECT DISTINCT
      vt.table_name,
      cc.column_name
    FROM
      information_schema.view_table_usage vt JOIN
      information_schema.table_constraints tc ON
        tc.table_catalog = vt.table_catalog AND
        tc.table_schema = vt.table_schema AND
        tc.table_name = vt.table_name AND
        tc.constraint_type = 'PRIMARY KEY' JOIN
      information_schema.constraint_column_usage cc ON
        cc.table_catalog = tc.table_catalog AND
        cc.table_schema = tc.table_schema AND
        cc.table_name = tc.table_name AND
        cc.constraint_name = tc.constraint_name
    WHERE
      view_name = $1
  `;

  // Replace all parameter values with NULL
  var tmpQuery      = query.replace(/\$\d/g, 'NULL');
  var createViewSQL = `CREATE OR REPLACE TEMP VIEW ${tmpName} AS (${tmpQuery})`;

  var columnUsageQuery = {
    name   : 'column_usage_query',
    text   : columnUsageSQL,
    values : [tmpName]
  };

  var tableUsageQuery = {
    name   : 'table_usage_query',
    text   : tableUsageSQL,
    values : [tmpName]
  };

  var sql = [
    `CREATE OR REPLACE TEMP VIEW ${tmpName} AS (${tmpQuery})`,
    tableUsageQuery,
    columnUsageQuery
  ];

  // Create a temporary view to figure out what columns will be used
  querySequence(this.client, sql, (error, result) => {
    if(error) return callback.call(this, error);

    var tableUsage  = result[1].rows;
    var columnUsage = result[2].rows;

    var keys    = {};
    var columns = [];

    tableUsage.forEach((row, index) => {
      keys[row.table_name] = row.column_name;
    });

    // This might not be completely reliable
    // Will match until last FROM, allowing subqueries in column selectors
    var pattern = /SELECT([\s\S]+)FROM/i;

    columnUsage.forEach((row, index) => {
      columns.push({
        table : row.table_name,
        name  : row.column_name,
        alias : `${ALIAS_PREFIX}_${row.table_name}_${row.column_name}`
      });
    });

    var keySql = _.map(keys,
      (value, key) => `CONCAT('${key}', ':', "${key}"."${value}")`);

    var columnSql = _.map(columns,
      (col, index) => `"${col.table}"."${col.name}" AS ${col.alias}`);

    query = query.replace(pattern, `
      SELECT
        CONCAT(${keySql.join(", '|', ")}) AS ${ID_ALIAS},
        ${columnSql},
        $1
      FROM
    `);

    cachedQueries[hash] = { keys, columns, query };

    return callback(null, cachedQueries[hash]);
  });
}

module.exports = LiveSelect;
