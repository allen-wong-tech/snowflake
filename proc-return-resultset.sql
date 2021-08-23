/*
References:
    Snowpipe data creation script:
        https://github.com/allen-wong-tech/snowflake/blob/master/unload-and-snowpipe-demo.sql
    
    Stored Proc to return a resultset
        https://docs.snowflake.com/en/sql-reference/stored-procedures-usage.html#returning-a-result-set


Purpose:
    Create two procedures for our API to call.

    We want to expose our table data to a proc and returned as JSON:
        procGetLastTen: get last ten records
        procGetRecord: pass in ID variable N, and get that record returned

Benefits:
    Procs provide advanced business logic, encapsulation, and reusability
    API calls allow a limited, governed surface to expose Snowflake data


*/


--set context
    use role sysadmin; use warehouse play_wh; use schema playdb.public;
    alter warehouse play_wh set warehouse_size = 'xsmall';


    
--our data which came in via SnowPipe
    select top 10 * 
    from nums_target_pipe order by 1 desc;



--get last ten records
CALL procGetLastTen(
        'nums_target_pipe',                                     -- Table name.
        array_append(
          ARRAY_APPEND(TO_ARRAY('N'), 'R'),'INSERT_TS')         -- Array of column names.
        );
        
        
        
        
        
        
        
--pass in ID and get record back
CALL procGetRecord(
        'nums_target_pipe',                                     
        array_append(                                           
          ARRAY_APPEND(TO_ARRAY('N'), 'R'),'INSERT_TS'
        ),
        6                                               --ID that we want the record for
        );
        







--procGetLastTen: get the last ten records
CREATE OR REPLACE PROCEDURE procGetLastTen(TABLE_NAME VARCHAR, COL_NAMES ARRAY)
      RETURNS VARIANT NOT NULL
      LANGUAGE JAVASCRIPT
      AS
      $$
      // This variable will hold a JSON data structure that holds ONE row.
      var row_as_json = {};
      // This array will contain all the rows.
      var array_of_rows = [];
      // This variable will hold a JSON data structure that we can return as
      // a VARIANT.
      // This will contain ALL the rows in a single "value".
      var table_as_json = {};

      // Run SQL statement(s) and get a resultSet.
      var command = "SELECT TOP 10 * FROM " + TABLE_NAME + " ORDER BY N DESC";
      var cmd1_dict = {sqlText: command};
      var stmt = snowflake.createStatement(cmd1_dict);
      var rs = stmt.execute();

      // Read each row and add it to the array we will return.
      var row_num = 1;
      while (rs.next())  {
        // Put each row in a variable of type JSON.
        row_as_json = {};
        // For each column in the row...
        for (var col_num = 0; col_num < COL_NAMES.length; col_num = col_num + 1) {
          var col_name = COL_NAMES[col_num];
          row_as_json[col_name] = rs.getColumnValue(col_num + 1);
          }
        // Add the row to the array of rows.
        array_of_rows.push(row_as_json);
        ++row_num;
        }
      // Put the array in a JSON variable (so it looks like a VARIANT to
      // Snowflake).  The key is "key1", and the value is the array that has
      // the rows we want.
      table_as_json = { "key1" : array_of_rows };

      // Return the rows to Snowflake, which expects a JSON-compatible VARIANT.
      return table_as_json;
      $$
      ;
      





--procGetRecord: pass in PARAM parameter and get that record returned as JSON
CREATE OR REPLACE PROCEDURE procGetRecord(TABLE_NAME VARCHAR, COL_NAMES ARRAY, PARAM VARCHAR)
      RETURNS VARIANT NOT NULL
      LANGUAGE JAVASCRIPT
      AS
      $$
      // This variable will hold a JSON data structure that holds ONE row.
      var row_as_json = {};
      // This array will contain all the rows.
      var array_of_rows = [];
      // This variable will hold a JSON data structure that we can return as
      // a VARIANT.
      // This will contain ALL the rows in a single "value".
      var table_as_json = {};

      // Run SQL statement(s) and get a resultSet.
      var command = "SELECT TOP 5 * FROM " + TABLE_NAME + " WHERE N = " + PARAM;
      var cmd1_dict = {sqlText: command};
      var stmt = snowflake.createStatement(cmd1_dict);
      var rs = stmt.execute();

      // Read each row and add it to the array we will return.
      var row_num = 1;
      while (rs.next())  {
        // Put each row in a variable of type JSON.
        row_as_json = {};
        // For each column in the row...
        for (var col_num = 0; col_num < COL_NAMES.length; col_num = col_num + 1) {
          var col_name = COL_NAMES[col_num];
          row_as_json[col_name] = rs.getColumnValue(col_num + 1);
          }
        // Add the row to the array of rows.
        array_of_rows.push(row_as_json);
        ++row_num;
        }
      // Put the array in a JSON variable (so it looks like a VARIANT to
      // Snowflake).  The key is "key1", and the value is the array that has
      // the rows we want.
      table_as_json = { "key1" : array_of_rows };

      // Return the rows to Snowflake, which expects a JSON-compatible VARIANT.
      return table_as_json;
      $$
      ;        






/*
RECAP

Purpose:
    Create two procedures for our API to call.

    We want to expose our table data to a proc and returned as JSON:
        procGetLastTen: get last ten records
        procGetRecord: pass in ID variable N, and get that record returned

Benefits:
    Procs provide advanced business logic, encapsulation, and reusability
    API calls allow a limited, governed surface to expose Snowflake data


*/
