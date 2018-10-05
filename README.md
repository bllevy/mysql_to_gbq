# mysql_to_gbq
Easily export tables from a MySQL database to Google BigQuery using Nodejs!

# How to Use
- Ensure that you have permissions to read / write from Google Cloud Storage and Google BigQuery
- Ensure that you have a Google Cloud Storage Bucket created and ready to be used
- Note, this ETL utilizes Google's [application-default credentials](https://cloud.google.com/docs/authentication/production), so be sure that you've followed steps to set that up. You may also choose to modify the script to work with additional env variables or a key file, that's fine too.
- Provide necessary MySQL details (including credentials that have the permissions to read from the MySQL db's information schema and tables)
- Update the "filterTables" function to change the filtering logic, or null out the filterTables function in the config completely if you'd rather not exclude any tables from the export.
- If you want any row-level transformations to occur, update the `transformRecord` function in the config file, as it's passed to the `mysql_to_bq` routine to modify records row-by-row for each table. You can filter to a specific table and fieldname and set rules for modifications.
- You can choose how many tables are exported concurrently, reduce from 5 if you're RAM or CPU constrained -- and increase sparingly, depending on the tables, # of rows, # of columns, exporting multiple tables at once can take a heavy toll on resources.
- You can provide a table name mapping if you'd prefer to rename the tables in transit.
- The `mysql_to_gbq` routine will automatically translate the MySQL schemas to GBQ schemas, so you don't have to sweat the details!
- Once the config is set up, call index.js with `node index.js` from the root directory to kick off the exports!
