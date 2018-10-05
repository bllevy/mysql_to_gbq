'use strict';

(function() {
  const getConfig = require('./util.js').getConfig;
  const _ = require('lodash');

  /**
   * Filters out tables that should not be replicated to BigQuery.
   *
   * @param {Array} tables
   * @return {Array} filteredTableList
   */
  const filterTables = function filterTables(tables){
    // _.filter() works by including only records that pass the test.
    const filteredTableList = _.filter(tables, (table) => {
      // If the table does not start with a '#', '_', 'cache_', or is the table 'cache',
      //  include it in the list to be exported.
      return !(_.startsWith(table, "#"))
        && !(_.startsWith(table, "_"))
        && !(_.startsWith(table, "cache_"))
        && table !== 'cache'
        && table !== 'user_scheduled_delete'
        && table !== 'user_scheduled_recover';
    });
      
    // Return the filtered list of tables.
    return filteredTableList;
  };

  /**
   * Transforms and returns a record as newline delimited JSON.
   *
   * @param {Object} recordObj
   * @return {String} json
   */
  const transformRecord = function transformRecord(recordObj, currentTableName){
    // Loop over the raw record and apply transformations in place.
    _.each(recordObj, (value, field) => {
      
      // The mysql module automatically converts BLOBs to Buffers.
      // If the value is of type buffer, replace it with its .toString() value.
      if (_.isBuffer(value)){
        recordObj[field] = value.toString('utf-8');
      }

      // If the currentTableName is users and the field contains a password hash, null it out (for security reasons).
      if (currentTableName === 'users' && field === 'pass'){
        recordObj[field] = null;
      }

    });

    // Pass the stringified transformed record to the next pipe.
    return JSON.stringify(recordObj) +'\n';
  };

  module.exports = {
    // MySQL Connection / schema details.
    mysqlHost: getConfig('WWW_MYSQL_HOST'),
    mysqlUser: getConfig('WWW_MYSQL_USER'),
    mysqlPort: getConfig('WWW_MYSQL_PORT'),
    mysqlPw: getConfig('WWW_MYSQL_PW'),
    mysqlDb: getConfig('WWW_MYSQL_DB'),
    // Google Cloud Credentials
    // Utilizing application-default credentials, we only need the google cloud project name.
    projectId: getConfig('GCLOUD_PROJECT_NAME'),
    // The GBQ dataset you want to write the tables into.
    datasetId: getConfig('BQ_DATASET', 'www'),
    // The Google Cloud Storage bucket where the .json files will be written to prior to import into GBQ.
    bucket: getConfig('GCS_BUCKET', 'www-prod'),
    /*
     * If filterTables is removed/commented out, the routine will export all tables.
     * NOTE: Table names must be valid BQ syntax for creation:
     *  (https://cloud.google.com/bigquery/docs/tables#create-table)
     *  If you have tables with non-BQ-friendly names the routine will fail.
     *  Be sure to filter out those tables in the filterTables function, or rename them in the source DB.
     */
    filterTables: filterTables,
    // Function to be used by the mysql_to_bq routine to transform individual records from the current table.
    // If this function is removed/commented out, the routine will perform no additional transforms to each record.
    transformRecord: transformRecord,
    // The number of tables to be exported concurrently.
    exportConcurrency: getConfig('EXPORT_CONCURRENCY', 5),
    // The upper limit of mysql connections to be made to the DB.
    mysqlConnectionLimit: getConfig('MYSQL_CONNECTION_LIMIT', 100),
    // Nodejs default is 16 for objectMode streams, we want to bump that up.
    highWaterMark: getConfig('HIGH_WATER_MARK', 100),
    // If you would like to rename tables in the ETL, 
    // provide the table name mapping between the origin table name (as it appears in MySQL), and its desired destination for GBQ.
    // Note, GBQ table name rules apply! See https://cloud.google.com/bigquery/docs/tables#create-table for details.
    // tableMapping: {
    //   origin_table_name: 'destination_name',
    //   origin_name: 'destination_name',
    //   origin_name: 'destination_name',
    //   ...
    //   origin_name: 'destination_name'
    // }
  };

}).call(this);
