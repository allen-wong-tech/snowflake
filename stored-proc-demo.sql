/*
GOAL
    demonstrate stored procs. 

https://docs.snowflake.com/en/sql-reference/stored-procedures-usage.html#label-stored-procedure-examples

*/

use role sysadmin; use warehouse play_wh; use schema playdb.public;
alter warehouse play_wh set warehouse_size = 'xsmall';

-----------------------------------------------------
--return pi
create or replace procedure sp_pi()
    returns float not null
    language javascript
    as
    $$
    return 3.1415926;
    $$
    ;
    
call sp_pi();

-----------------------------------------------------
--insert your parameter into table

CREATE or replace TABLE stproc_test_table1 (num_col1 numeric(14,7));

create or replace procedure stproc1(FLOAT_PARAM1 FLOAT)
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = 
     "INSERT INTO stproc_test_table1 (num_col1) VALUES (" + FLOAT_PARAM1 + ")";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;
    
call stproc1(5.14::float);

select * from stproc_test_table1;

-----------------------------------------------------
--dynamic SQL (equivalent to select count(*) from passed-in table):

create or replace procedure get_row_count(table_name VARCHAR)
  returns float not null
  language javascript
  as
  $$
  var row_count = 0;
  // Dynamically compose the SQL statement to execute.
  var sql_command = "select count(*) from " + TABLE_NAME;
  // Run the statement.
  var stmt = snowflake.createStatement(
         {
         sqlText: sql_command
         }
      );
  var res = stmt.execute();
  // Get back the row count. Specifically, get the first (and in this case only) row from the result set ...
  res.next();
  
  // and then get the returned value, which in this case is the number of rows in the table.
  row_count = res.getColumnValue(1);
  
  return row_count;
  $$
  ;
 

    --how many rows are in the table:
    call get_row_count('stproc_test_table1');  select count(*) from stproc_test_table1;

-----------------------------------------------------
--recursive stored procedure:
  create or replace table stproc_test_table2 (col1 FLOAT);

create or replace procedure recursive_stproc(counter FLOAT)
    returns varchar not null
    language javascript
    as
    -- "$$" is the delimiter that shows the beginning and end of the stored proc.
    $$
    var counter1 = COUNTER;
    var returned_value = "";
    var accumulator = "";
    var stmt = snowflake.createStatement(
        {
        sqlText: "INSERT INTO stproc_test_table2 (col1) VALUES (?);",
        binds:[counter1]
        }
        );
    var res = stmt.execute();
    if (COUNTER > 0)
        {
        stmt = snowflake.createStatement(
            {
            sqlText: "call recursive_stproc (?);",
            binds:[counter1 - 1]
            }
            );
        res = stmt.execute();
        res.next();
        returned_value = res.getColumnValue(1);
        }
    accumulator = accumulator + counter1 + ":" + returned_value;
    return accumulator;
    $$
    ;
    
call recursive_stproc(4.0::float);  SELECT * FROM stproc_test_table2 ORDER BY col1;

-----------------------------------------------------
--retrieving a small amount of metadata from a result set:

create or replace table stproc_test_table3 (
    n10 numeric(10,0),     /* precision = 10, scale = 0 */
    n12 numeric(12,4),     /* precision = 12, scale = 4 */
    v1 varchar(19)         /* scale = 0 */
    );
    
create or replace procedure get_column_scale(column_index float)
    returns float not null
    language javascript
    as
    $$
    var stmt = snowflake.createStatement(
        {sqlText: "select n10, n12, v1 from stproc_test_table3;"}
        );
    stmt.execute();  // ignore the result set; we just want the scale.
    return stmt.getColumnScale(COLUMN_INDEX); // Get by column index (1-based)
    $$
    ;
    
select top 300 * from stproc_test_table3;

call get_column_scale(1);  call get_column_scale(2);  call get_column_scale(3);

-----------------------------------------------------
--try/catch block to catch an error inside a stored procedure:

    create or replace procedure broken()
      returns varchar not null
      language javascript
      as
      $$
      var result = "";
      try {
          snowflake.execute( {sqlText: "Invalid Command!;"} );
          result = "Succeeded";
          }
      catch (err)  {
          result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
          result += "\n  Message: " + err.message;
          result += "\nStack Trace:\n" + err.stackTraceTxt; 
          }
      return result;
      $$
      ;
      
    -- This is expected to fail.
    call broken();
    
-----------------------------------------------------
--throwing a custom exception:

CREATE OR REPLACE PROCEDURE validate_age (age float)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
    try {
        if (AGE < 0) {
            throw "Age cannot be negative!";
        } else {
            return "Age validated.";
        }
    } catch (err) {
        return "Error: " + err;
    }
$$;

CALL validate_age(50);

CALL validate_age(-2);

-----------------------------------------------------
--wraps multiple related statements in a transaction
--parameter force_failure 

-- Create the procedure
create or replace procedure cleanup(force_failure varchar)
  returns varchar not null
  language javascript
  as
  $$
  var result = "";
  snowflake.execute( {sqlText: "BEGIN WORK;"} );
  try {
      snowflake.execute( {sqlText: "DELETE FROM child;"} );
      snowflake.execute( {sqlText: "DELETE FROM parent;"} );
      if (FORCE_FAILURE === "fail")  {
          // To see what happens if there is a failure/rollback,
          snowflake.execute( {sqlText: "DELETE FROM no_such_table;"} );
          }
      snowflake.execute( {sqlText: "COMMIT WORK;"} );
      result = "Succeeded";
      }
  catch (err)  {
      snowflake.execute( {sqlText: "ROLLBACK WORK;"} );
      return "Failed: " + err;   // Return a success/error indicator.
      }
  return result;
  $$
  ;

call cleanup('fail');

create temp table child (a int);
create temp table parent (a int);
call cleanup('do not fail');

-----------------------------------------------------
--logging an error

CREATE OR REPLACE TABLE error_log (error_code number, error_state string, error_message string, stack_trace string);

CREATE OR REPLACE PROCEDURE broken() 
RETURNS varchar 
NOT NULL 
LANGUAGE javascript 
AS $$
var result;
try {
    snowflake.execute({ sqlText: "Invalid Command!;" });
    result = "Succeeded";
} catch (err) {
    result = "Failed";
    snowflake.execute({
      sqlText: `insert into error_log VALUES (?,?,?,?)`
      ,binds: [err.code, err.state, err.message, err.stackTraceTxt]
      });
}
return result;
$$;

call broken();
select * from error_log;

-----------------------------------------------------
--allows the caller to specify the log table name, and create the table if it doesn’t already exist. 
--allows the caller to easily turn logging on and off.

--Note also that one of these stored procedures creates a small JavaScript function that it can re-use. 
--In long stored procedures with repetitive code, creating JavaScript functions inside the stored procedure can be convenient.

CREATE or replace PROCEDURE do_log(MSG STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS $$
 
    // See if we should log - checks for session variable do_log = true.
    try {
       var stmt = snowflake.createStatement( { sqlText: `select $do_log` } ).execute();
    } catch (ERROR){
       return; //swallow the error, variable not set so don't log
    }
    stmt.next();
    if (stmt.getColumnValue(1)==true){ //if the value is anything other than true, don't log
       try {
           snowflake.createStatement( { sqlText: `create temp table identifier ($log_table) if not exists (ts number, msg string)`} ).execute();
           snowflake.createStatement( { sqlText: `insert into identifier ($log_table) values (:1, :2)`, binds:[Date.now(), MSG] } ).execute();
       } catch (ERROR){
           throw ERROR;
       }
    }
 $$
;

CREATE or replace PROCEDURE my_test()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS $$

    // Define the SP call as a function - it's cleaner this way.
    // Add this function to your stored procs
    function log(msg){
        snowflake.createStatement( { sqlText: `call do_log(:1)`, binds:[msg] } ).execute();
        }

    // Now just call the log function anytime...
    try {
        var x = 10/10;
        log('log this message'); //call the log function
        //do some stuff here
        log('x = ' + x.toString()); //log the value of x 
        log('this is another log message'); //throw in another log message
    } catch(ERROR) {
        log(ERROR); //we can even catch/log the error messages
        return ERROR;
    }

    $$
;

-----------------------------------------------------
--Turn on logging:

set do_log = true; --true to enable logging, false (or undefined) to disable
set log_table = 'my_log_table';  -- The name of the temp table where log messages go.


CALL my_test();

-----------------------------------------------------
--Check that the table was created and the messages were logged:

select msg 
    from my_log_table 
    order by 1;
    
drop table my_log_table;

-----------------------------------------------------
--overload stored procedure names. For example:

create or replace procedure stproc1(FLOAT_PARAM1 FLOAT)
    returns string
    language javascript
    strict
    as
    $$
    return FLOAT_PARAM1;
    $$
    ;
create or replace procedure stproc1(FLOAT_PARAM1 FLOAT, FLOAT_PARAM2 FLOAT)
    returns string
    language javascript
    strict
    as
    $$
    return FLOAT_PARAM1 * FLOAT_PARAM2;
    $$
    ;

call stproc1(5.14::FLOAT);

call stproc1(5.14::FLOAT, 2.00::FLOAT);

-----------------------------------------------------
--use the RESULT_SCAN function to retrieve the result from a CALL statement:

CREATE or replace TABLE western_provinces(ID INT, province VARCHAR);

INSERT INTO western_provinces(ID, province) VALUES
    (1, 'Alberta'),
    (2, 'British Columbia'),
    (3, 'Manitoba')
    ;
    
select * from western_provinces;

-----------------------------------------------------
--looks like a result set of three rows, but is actually a single string:

CREATE OR REPLACE PROCEDURE read_western_provinces()
  RETURNS VARCHAR NOT NULL
  LANGUAGE JAVASCRIPT
  AS
  $$
  var return_value = "";
  try {
      var command = "SELECT * FROM western_provinces ORDER BY province;"
      var stmt = snowflake.createStatement( {sqlText: command } );
      var rs = stmt.execute();
      if (rs.next())  {
          return_value += rs.getColumnValue(1);
          return_value += ", " + rs.getColumnValue(2);
          }
      while (rs.next())  {
          return_value += "\n";
          return_value += rs.getColumnValue(1);
          return_value += ", " + rs.getColumnValue(2);
          }
      }
  catch (err)  {
      result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
      result += "\n  Message: " + err.message;
      result += "\nStack Trace:\n" + err.stackTraceTxt;
      }
  return return_value;
  $$
  ;
  
CALL read_western_provinces();    

set q = last_query_id();

select * from table(result_scan($q));

-----------------------------------------------------
--extract the individual “rows” that appear to be contained within that string, and store those rows in another table.
--Create a table for long-term storage. 
--contains the province name and the province ID after you’ve extracted them from the string returned by the CALL command:

CREATE or replace TABLE all_provinces(ID INT, province VARCHAR);

--Call the stored procedure, then retrieve the result by using RESULT_SCAN, and then 
--extract the three rows from the string and put those rows into the table:

INSERT INTO all_provinces
  WITH 
    one_string (string_col) AS
      (SELECT * FROM TABLE(result_scan($q))),
    three_strings (one_row) AS
      (SELECT VALUE FROM one_string, LATERAL SPLIT_TO_TABLE(one_string.string_col, '\n'))
  SELECT
         STRTOK(one_row, ',', 1) AS ID,
         STRTOK(one_row, ',', 2) AS province
    FROM three_strings
    WHERE NOT (ID IS NULL AND province IS NULL);
    
-----------------------------------------------------
--Verify that this worked by showing the rows in the table:

SELECT ID, province 
    FROM all_provinces;


----------------------------------------------------------------------------------------------------------
-- return a status/error message for each SQL statement. 
--return Array
--https://docs.snowflake.com/en/sql-reference/stored-procedures-usage.html#returning-a-result-set

CREATE OR REPLACE PROCEDURE sp_return_array()
      RETURNS VARIANT NOT NULL
      LANGUAGE JAVASCRIPT
      AS
      $$
      // This array will contain one error message (or an empty string) 
      // for each SQL command that we executed.
      var array_of_rows = [];

      // Artificially fake the error messages.
      array_of_rows.push("ERROR: The foo was barred.")
      array_of_rows.push("WARNING: A Carrington Event is predicted.")

      return array_of_rows;
      $$
      ;
CALL sp_return_array();

-- Now get the individual error messages, in order.
SELECT INDEX, VALUE 
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) AS res, LATERAL FLATTEN(INPUT => res.$1)
    ORDER BY index;
    
----------------------------------------------------------------------------------------------------------
--returning a result set

CREATE or replace TABLE return_to_me(col_i INT, col_v VARCHAR);

INSERT INTO return_to_me (col_i, col_v) VALUES
    (1, 'Ariel'),
    (2, 'October'),
    (3, NULL),
    (NULL, 'Project');
    
select * from return_to_me;

-- Create the stored procedure that retrieves a result set and returns it.
CREATE OR REPLACE PROCEDURE sp_return_table(TABLE_NAME VARCHAR, COL_NAMES ARRAY)
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
      var command = "SELECT * FROM " + TABLE_NAME;
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
      
CALL sp_return_table(
        -- Table name.
        'return_to_me',
        -- Array of column names.
        ARRAY_APPEND(TO_ARRAY('COL_I'), 'COL_V')
        );
        
SELECT $1:key1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT VALUE:COL_I AS col_i, value:COL_V AS col_v
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) AS res, LATERAL FLATTEN(input => res.$1)
  ORDER BY COL_I;

CREATE or replace VIEW stproc_view (col_i, col_v) AS 
  SELECT NULLIF(VALUE:COL_I::varchar, 'null'::varchar), 
         NULLIF(value:COL_V::varchar, 'null'::varchar)
    FROM (SELECT $1:key1 AS tbl FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))) AS res, 
         LATERAL FLATTEN(input => res.tbl);
         
CALL sp_return_table(
        -- Table name.
        'return_to_me',
        -- Array of column names.
        ARRAY_APPEND(TO_ARRAY('COL_I'), 'COL_V')
        );         
         
         
select * from stproc_view;








----------------------------------------------------------------------------------------------------------
--online retailer protecting-privacy; delete customer's data for privacy reasons, except if
    --Any purchased item has a warranty that has not yet expired.
    --The customer still owes money (or the customer is owed a refund).
--https://docs.snowflake.com/en/sql-reference/stored-procedures-usage.html#protecting-privacy

create or replace procedure delete_nonessential_customer_data(customer_ID varchar)
    returns varchar not null
    language javascript
    as
    $$

    // If the customer posted reviews of products, delete those reviews.
    var sql_cmd = "DELETE FROM reviews WHERE customer_ID = " + CUSTOMER_ID;
    snowflake.execute( {sqlText: sql_cmd} );

    // Delete any other records not needed for warranty or payment info.
    // ...

    var result = "Deleted non-financial, non-warranty data for customer " + CUSTOMER_ID;

    // Find out if the customer has any net unpaid balance (or surplus/prepayment).
    sql_cmd = "SELECT SUM(price) - SUM(paid) FROM purchase_history WHERE customer_ID = " + CUSTOMER_ID;
    var stmt = snowflake.createStatement( {sqlText: sql_cmd} );
    var rs = stmt.execute();
    // There should be only one row, so should not need to iterate.
    rs.next();
    var net_amount_owed = rs.getColumnValue(1);

    // Look up the number of purchases still under warranty...
    var number_purchases_under_warranty = 0;
    // Assuming a 1-year warranty...
    sql_cmd = "SELECT COUNT(*) FROM purchase_history ";
    sql_cmd += "WHERE customer_ID = " + CUSTOMER_ID;
    // Can't use CURRENT_DATE() because that changes. So assume that today is 
    // always June 15, 2019.
    sql_cmd += "AND PURCHASE_DATE > dateadd(year, -1, '2019-06-15'::DATE)";
    var stmt = snowflake.createStatement( {sqlText: sql_cmd} );
    var rs = stmt.execute();
    // There should be only one row, so should not need to iterate.
    rs.next();
    number_purchases_under_warranty = rs.getColumnValue(1);

    // Check whether need to keep some purchase history data; if not, then delete the data.
    if (net_amount_owed == 0.0 && number_purchases_under_warranty == 0)  {
        // Delete the purchase history of this customer ...
        sql_cmd = "DELETE FROM purchase_history WHERE customer_ID = " + CUSTOMER_ID;
        snowflake.execute( {sqlText: sql_cmd} );
        // ... and delete anything else that that should be deleted.
        // ...
        result = "Deleted all data, including financial and warranty data, for customer " + CUSTOMER_ID;
        }
    return result;
    $$
    ;


create table reviews (customer_ID VARCHAR, review VARCHAR);
create table purchase_history (customer_ID VARCHAR, price FLOAT, paid FLOAT,
                               product_ID VARCHAR, purchase_date DATE);
insert into purchase_history (customer_ID, price, paid, product_ID, purchase_date) values 
    (1, 19.99, 19.99, 'chocolate', '2018-06-17'::date),
    (2, 19.99,  0.00, 'chocolate', '2017-02-14'::date),
    (3, 19.99,  19.99, 'chocolate', '2017-03-19'::date);

insert into reviews (customer_ID, review) values (1, 'Loved the milk chocolate!');
insert into reviews (customer_ID, review) values (2, 'Loved the dark chocolate!');

select * from purchase_history;  
select * from reviews;


// let date = June 15, 2019.

call delete_nonessential_customer_data(1);
SELECT * FROM reviews;
SELECT * FROM purchase_history;

call delete_nonessential_customer_data(2);
SELECT * FROM reviews;
SELECT * FROM purchase_history;

//Customer #3 does not owe any money (and is not owed any money). Their warranty expired, so the stored procedure deletes both the review comments and the purchase records:
call delete_nonessential_customer_data(3);
SELECT * FROM reviews;
SELECT * FROM purchase_history;






