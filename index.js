'use strict';

/**
 * Replicate MySQL Database to Google BigQuery routine definition.
 */
const configs = require('./config.js');
const routine = require('./mysql_to_bq.js');

return routine(configs);
