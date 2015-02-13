/**
 * Test Intialization
 */
if(!('CONN' in process.env))
  throw new Error(
    'CONN environment variable required! (database connection string)');
if(!('CHANNEL' in process.env))
  throw new Error(
    'CHANNEL environment variable required! (notification identifier string)');

// Global flags
global.printDebug = process.env.DEBUG !== undefined && process.env.DEBUG !== '0';
global.printStats = process.env.STATS !== undefined && process.env.STATS !== '0';

// ES6 may be used in all files required by this one
require('6to5/register');

var _          = require('lodash');
var anyDB      = require('any-db');
var PgTriggers = require('../');

// Define global instances
global.conn     = anyDB.createConnection(process.env.CONN);
global.triggers = new PgTriggers(conn, process.env.CHANNEL);

module.exports = _.assign(
  require('./helpers/lifecycle'),
  // Load each test module
  require('./LiveSelect')
);
