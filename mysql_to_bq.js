'use strict';

(function() {
  const Promise = require('bluebird'),
      through = require('through2'),
      moment = require('moment-timezone'),
      retry = require('retry'),
      _ = require('lodash'),
      mysql = require('mysql'),
      http = require('http'),
      bigquery = require('@google-cloud/bigquery'),
      storage = require('@google-cloud/storage');

  module.exports = function mysqlToBq(config) {
    let exportConcurrency,
        connectionLimit,
        highWaterMark,
        projectId,
        datasetId,
        bucket,
        gcs,
        bq,
        dataset,
        currentTime,
        exportCount,
        rowsProcessed,
        _transformRecord,
        startTime,
        endTime,
        tableMapping,
        pool;

    // Dynamically exports tables from a MySQL database to a BigQuery dataset.
    return _initialize(config)
      .then(_getTablesFromMysql)
      .then(_exportTables)
      .then(() => {
        // Capture the finish time.
        endTime = moment();
        // Close all remaining pool connections.
        pool.end();
        // Log the stats.
        console.log(`All tables have finished exporting @ ${endTime.format("YYYY-MM-DD HH:mm:ss")}`);
        console.log(`Total Duration: ${endTime.diff(startTime, 'minutes')} minutes`);
        console.log(`Tables Exported: ${exportCount}`);
        console.log(`Rows Processed: ${rowsProcessed}`);
      })
      .catch((err) => {
        throw new Error(err);
      });

    /**
     * Initializes module variables and required API clients.
     * 
     * @param {Object} config
     * @return {Promise}
     * @throws {Error}
     * @private
     */
    function _initialize(config) {
      return new Promise((resolve, reject) => {
        // Capture routine start time.
        startTime = moment();

        // Init throughput options.
        exportConcurrency = config.exportConcurrency;
        connectionLimit = config.mysqlConnectionLimit;
        highWaterMark = config.highWaterMark;

        // Init GCloud project, dataset, and bucket.
        projectId = config.projectId;
        datasetId = config.datasetId;
        bucket = config.bucket;

        // Init the GCloud Storage & BigQuery clients (application-default credentials).
        gcs = storage({projectId: projectId});
        bq = bigquery({projectId: projectId}); 

        // Create a dataset object, used to create tables and import files into BQ.
        dataset = bq.dataset(datasetId);

        // Init table mapping (if it exists);
        tableMapping = config.tableMapping || null;

        // Set default timezone to PST.
        moment.tz.setDefault("America/Los_Angeles");

        // Define momentjs convenience function for getting the current time.
        currentTime = () => moment().format("YYYY-MM-DD HH:mm:ss");

        // Keep track of the # of tables exported.
        exportCount = 0;

        // Count the # of rows processed.
        rowsProcessed = 0;

        // Increase default maxSockets from 5 (for writing multiple files to GCS concurrently).
        http.globalAgent.maxSockets = 500;
        // Set default timezone to PST.
        moment.tz.setDefault("America/Los_Angeles");

        // Check if the config contains a transformRecord function.
        if (_.has(config, 'transformRecord') && _.isFunction(config.transformRecord)) {
          // Set the _transformRecord function to the one passed in by the config.
          _transformRecord = config.transformRecord;
        } else {
          // Otherwise, set the _transformRecord function to a basic stringification of the data.
          _transformRecord = (data, table) => {
            return JSON.stringify(data) +'\n';
          };
        }

        // Create a connection pool to handle the multiple mysql connections.
        pool  = mysql.createPool({
          connectionLimit: connectionLimit,
          host: config.mysqlHost,
          port: config.mysqlPort,
          user: config.mysqlUser,
          password: config.mysqlPw,
          database: config.mysqlDb,
          dateStrings: true
        });

        console.log(`Starting replication ETL to BigQuery @ ${currentTime()}`);

        resolve();
      }); 
    };

    /**
     * Exports each table to BigQuery.
     * 
     * @param {Promise<Array>} tables
     * @return {Promise<Array>} processedTables
     * @throws {Error}
     * @private
     */
    function _exportTables(tables) {
      return Promise.map(tables, (tableName) => {
        return new Promise((resolve, reject) => {
          const operation = retry.operation({retries: 10});

          // If an individual table export fails, retry from the top.
          operation.attempt(() => {
            return _createTable(tableName)
              .then(_etlTableDataToGCS)
              .then(_importJsonDataToTable)
              .then(_pollJobStatus)
              .then(resolve)
              .catch((err) => {
                // If there are retry attempts, retry.
                if (err && operation.retry(err)) {
                  console.error(`\n[ERROR]: [${tableName}]\n${err}`);
                  console.log(`Retrying [${tableName}] @ ${currentTime()}`);
                  return;
                }

                // If we are out of retry attempts, reject.
                if (err && !operation.retry(err)) {
                  console.error(`\nNo more retry attempts available for ${tableName}.`);
                  // If all retry attempts have failed, resolve null.
                  reject(`[ERROR]: [${tableName}]\n${err}`);
                }
              });
          });
        });
      }, {concurrency: exportConcurrency});
    };

    /**
     * Queries a MySQL database for all its table names and stores them in an array.
     * 
     * Note: Will include all tables unless a filterTables function is provided.
     * 
     * @return {Promise<Array>} tableList
     * @throws {error}
     * @private
     */
    function _getTablesFromMysql() {
      return new Promise((resolve, reject) => {
        pool.getConnection((err, conn) => {          
          if (err) {
            try {
              conn.release();
            } catch (err) {
              console.log(JSON.stringify(err));
              reject(err);
            }
          }

          // Capture all tables in the database.
          conn.query(`SHOW TABLES;`, (err, results) => {
            // release the connection back to the pool.
            conn.release();
            
            if (err) {
              reject(err);
            }

            if (results.length == 0) {
              reject(`No tables found in {config.mysqlDb}.`);
            }

            // Add all of the table names to the tableList array.
            const tableList = _.map(results, (result) => {
              //  Example result row: "RowDataPacket { Tables_in_<DB_NAME>: 'table_name' }"
              return result[`Tables_in_${config.mysqlDb}`];
            });

            // If the config contains a 'filterTables' function, use it.
            if (_.has(config, 'filterTables') && _.isFunction(config.filterTables)) {
              // Filter out unwanted tables from the list of tables.
              const filteredList = config.filterTables(tableList);
              // Resolve the filtered list of tables.
              resolve(filteredList);
            } else {
              // Otherwise, resolves all found tables.
              resolve(tableList);
            }
          });
        });
      }); 
    };

    /**
     * Queries a MySQL database for a particular table's schema,
     * converts it to BQ formatting, and adds the table to BigQuery.
     *
     * @param {String} tableName
     * @return {Promise<String>} tableName
     * @throws {Error}
     */
    function _createTable(tableName) {
      return new Promise((resolve, reject) => {
        // If the table's name has been remapped, use the mapped version for creation in BQ.
        const bqTableName = _.has(tableMapping, tableName) ? tableMapping[tableName] : tableName;

        pool.getConnection((err, conn) => {
          if (err) {
            try {
              conn.release();
            } catch (err) {
              console.log(JSON.stringify(err));
              reject(err);  
            }
          }

          // Query the table's description details for field types.
          conn.query(`DESC ${tableName};`, (connErr, rows, fields) => {
            // Release the connection back to the pool.
            conn.release();

            if (connErr) {
              reject(`[ERROR]: [${tableName}]\n${connErr}`);
            }

            // Generate MySQL -> BigQuery schema.
            const json = rows.map((row) => {
              return {name: row.Field, type: _convertToBqColumnType(row.Type)};
            });

            // Delete the table if it exists...
            dataset.table(bqTableName).delete((deletionErr) => {
              if (deletionErr && deletionErr.code !== 404) {
                console.error(`[ERROR]: [${bqTableName}]\n${deletionErr}`);
                reject(`[ERROR]: [${bqTableName}]\n${deletionErr}`);
              }

              // Options to be supplied to the createTable function.
              const options = {
                schema: {
                  // Use the generated schema.
                  fields: json
                }
              };

              // Creates a table in BigQuery.
              dataset.createTable(bqTableName, options, (creationErr) => {
                if (creationErr) {
                  reject(`[ERROR]: [${bqTableName}]\n${creationErr}`);
                }

                // Resolve the original table name.
                resolve(tableName);
              });
            });
          });
        });
      });
    };

    /**
     * Converts a MySQL column type to a BigQuery column type.
     *
     * @param {String} columnType
     * @return {String} convertedType
     */
    function _convertToBqColumnType(columnType) {
      const type = columnType.toLowerCase();
      if (type === 'tinyint(1)') {
        return 'BOOLEAN'
      }
      else if (type.indexOf('int') != -1) {
        return 'INTEGER'
      }
      else if (type === 'datetime' || type === 'timestamp') {
        return 'TIMESTAMP'
      }
      else if (type === 'date') {
        return 'DATE'
      }      
      else if ((type.indexOf('float') != -1) || (type.indexOf('double') != -1) || (type.indexOf('decimal') != -1)) {
        return 'FLOAT';
      }
      else {
        return 'STRING';
      }
    };

    /**
     * Queries a specified MySQL table for its data, transforms it,
     *   and writes the data to GCS as JSON.
     * 
     * @param {Promise<String>} tableName
     *
     * @return {Promise<Object>} exportDetails
     */
    function _etlTableDataToGCS(tableName) {
      return new Promise((resolve, reject) => {
        const transformedStream = through.obj({highWaterMark: highWaterMark}),
              // If the table's name has been remapped, use it for writing to GCS.
              bqTableName = _.has(tableMapping, tableName) ? tableMapping[tableName] : tableName,
              // Create the desired file object in GCS.
              gcsFile = gcs.bucket(bucket).file(`${bqTableName}.json`);

        pool.getConnection((err, conn) => {
          if (err) {
            try {
              conn.release();
            } catch (err) {
              console.log(JSON.stringify(err));
              reject(err);  
            }
          }

         /*
          * Solves for a weird MySQL character encoding issues:
          * Before every query for data, sets the connection to utf8.
          *
          * (Note: Setting the connection pool's configuration charset
          * to the various utf8 collations did not work, this did.)
          */
          conn.query(`SET NAMES utf8;`);

          // Queries the table for all of its data and streams the results.
          conn.query(`SELECT * FROM ${tableName};`)
            .stream({highWaterMark: highWaterMark})
            .on('end', () => {
              // Release the connection back to the pool.
              conn.release();
            })
            .pipe(through({objectMode: true, highWaterMark: highWaterMark}, (record, enc, passThru) => {
              // Increment the number of rows processed.
              rowsProcessed++;

              // Process/Transform the record.
              const processedRecord = _transformRecord(record, tableName);
              
              // Pass the transformed record onto the transformedStream.
              passThru(null, processedRecord)
            }))
            .pipe(gcsFile.createWriteStream({metadata: {contentEncoding: 'utf-8'}, resumable: false, validation: 'crc32c'}))
            .on('error', (err) => {
              reject(`[ERROR]: [${bqTableName}]\n${err}`);
            })
            .on('finish', () => {
              resolve({table: bqTableName, file: gcsFile});
            });
        });
      });
    };

    /**
     * Imports a JSON file to the specified BigQuery table.
     *
     * @param {Promise<Object>} importDetails
     *
     * @return {Promise<Object>} job
     */
    function _importJsonDataToTable(importDetails) {
      return new Promise((resolve, reject) => {
        // Generate arguments for bq.importFile.
        const tableName = importDetails.table,
              gcsFile = importDetails.file,
              // Import metadata.
              metadata = {
                 sourceFormat: 'NEWLINE_DELIMITED_JSON',
                 writeDisposition: 'WRITE_TRUNCATE',
                 maxBadRecords: 0
              };
        
        // Create import job.
        dataset.table(`${tableName}`).import(gcsFile, metadata)
          .then((data) => {
            // Resolve job detail object for polling.
            resolve({job: data[0], table: tableName});
          })
          .catch((err) =>{
            reject(`[ERROR]: [${tableName}]\n${err}`);
          });
      });
    };

    /**
     * Polls the job for its status.
     *
     * @param {Promise<Object>} jobDetails
     * @returns {Promise}
     */
    function _pollJobStatus(jobDetails) {
      console.log("\nPolling job for status...");
      let operation = retry.operation({retries: 100, maxTimeout: 30 * 1000}),
          jobObj = jobDetails.job,
          tableName = jobDetails.table,
          state;

      return new Promise((resolve, reject) => {
        operation.attempt(() => {
          // Query the bq job's endpoint with the job id.
          jobObj.get((err, job, apiResponse) => {
            if (err) {
              if (_.has(job, 'id')) {
                reject(`[ERROR]: [${tableName}] (jobId: ${job.id})\n${JSON.stringify(err)}`);
              } else {
                reject(`[ERROR]: [${tableName}]\n${JSON.stringify(err)}`);
              }
            }

            if (_.has(job.metadata.status, 'state')) {
              state = job.metadata.status.state;
            }

            // Check metadata.status for whether a job completed with errors.
            if (state === 'DONE' && job.metadata.status.errorResult) {
              console.log(`The import of [${tableName}] (${job.id}) has finished with errors @ ${currentTime()}`);
              console.error(job.metadata.status);
              reject(JSON.stringify(job.metadata.status));
            } 
            // Check metadata.status for a successful state.
            else if (state === 'DONE') {
              console.log(`The import of [${tableName}] (${job.id}) has completed successfully @ ${currentTime()}`);
              resolve();
            } else {
            // Retry.
              console.log(`Import job status for [${tableName}] is still: ${state} @ ${currentTime()}`);
              operation.retry(`Import job status for [${tableName}] is still: ${state}`);
            }
          });
        });
      });
    };

  };
}).call(this);
