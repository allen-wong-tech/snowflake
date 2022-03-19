/*--------------------------------------------------------------------------------

https://github.com/allen-wong-tech/snowflake/new/master/data-engineering/json-data-pipeline-1-of-2.sql
https://github.com/kromozome2003/Snowflake-Json-DataPipeline
https://raw.githubusercontent.com/kromozome2003/Snowflake-Json-DataPipeline/master/Images/snowflake-approach.png
https://docs.snowflake.net/manuals/user-guide/data-pipelines-intro.html
https://docs.snowflake.com/en/user-guide/streams.html
https://docs.snowflake.com/en/user-guide/tasks-intro.html

What we will show:
    copy into
        raw_json_table 
    stream has data
    extract_json_data task wakes once a minute; if streams has data call proc
        transformed_json_table - 
        aggregate_final_data task runs stored_proc_aggregate_final
            final_table

Benefits:
    Easy, governed visibility into data pipeline and data lineage
    

----------------------------------------------------------------------------------------------------------
--Setup

                                      --------------------------------------------------------------------------------*/
                                      -- Context setting
                                      use role sysadmin;
//                                      use role accountadmin;
                                      CREATE DATABASE if not exists CDPST;
                                      USE SCHEMA CDPST.PUBLIC;
                                      CREATE warehouse IF NOT EXISTS XSMALL_CONST_WH WITH warehouse_size = 'xsmall' auto_suspend = 60 initially_suspended = true;
                                      CREATE warehouse IF NOT EXISTS play_wh WITH warehouse_size = 'xsmall' auto_suspend = 60 initially_suspended = true;

                                      USE WAREHOUSE play_wh;



                                      -- Create a FILE FORMAT to parse JSON files
                                      CREATE FILE FORMAT if not exists CDPST.PUBLIC.JSON
                                          TYPE = 'JSON'
                                          COMPRESSION = 'AUTO'
                                          ENABLE_OCTAL = FALSE
                                          ALLOW_DUPLICATE = FALSE
                                          STRIP_OUTER_ARRAY = FALSE
                                          STRIP_NULL_VALUES = FALSE
                                          IGNORE_UTF8_ERRORS = FALSE;

                                      -- Create a STAGE where to put our json files
                                      CREATE STAGE if not exists CDPST.PUBLIC.cdpst_json_files;

                                      -- Put some files (in json-files folder) using SnowSQL or the GUI to @~/
                                      -- put 'file:///Path/to/your/github/project/json-files/weather*.json.gz' @CDPST.PUBLIC.cdpst_json_files;
                                      -- put file:///Users/awong/Downloads/weather*.json.gz @CDPST.PUBLIC.cdpst_json_files;

LIST @CDPST.PUBLIC.cdpst_json_files;

SELECT $1 FROM @CDPST.PUBLIC.cdpst_json_files/weather1.json.gz (FILE_FORMAT => CDPST.PUBLIC.JSON) LIMIT 5;
SELECT $1 FROM @CDPST.PUBLIC.cdpst_json_files/weather2.json.gz (FILE_FORMAT => CDPST.PUBLIC.JSON) LIMIT 5;


--raw data in variant datatype
CREATE OR REPLACE TABLE raw_json_table (v variant);

--transformed: json converted to structured and kelvin
CREATE OR REPLACE TABLE transformed_json_table (
  date timestamp_ntz,
  country string,
  city string,
  id string,
  temp_kel float,
  temp_min_kel float,
  temp_max_kel float,
  conditions string,
  wind_dir float,
  wind_speed float
);

--final: converted to celcius
CREATE OR REPLACE TABLE final_table (
  date timestamp_ntz,
  country string,
  city string,
  id string,
  temp_cel float,
  temp_min_cel float,
  temp_max_cel float,
  conditions string,
  wind_dir float,
  wind_speed float
);

select top 3000 * from raw_json_table;

-- monitor Data Changes against RAW table
CREATE OR REPLACE STREAM raw_data_stream ON TABLE raw_json_table;

-- monitor Data Changes against TRANSFORMED table
CREATE OR REPLACE STREAM transformed_data_stream ON TABLE transformed_json_table;

SHOW STREAMS; 

--table record + stream metadata
    SELECT * FROM raw_data_stream;

    SELECT * FROM transformed_data_stream;

-- read raw_data_stream and convert JSON to structured in transformed_json_table
create or replace procedure stored_proc_extract_json()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO transformed_json_table (date,country,city,id,temp_kel,temp_min_kel,temp_max_kel,conditions,wind_dir,wind_speed)";
    sql_command += "    SELECT";
    sql_command += "        convert_timezone('UTC', 'Europe/Paris', v:time::timestamp_ntz) date,";
    sql_command += "        v:city.country::string country,";
    sql_command += "        v:city.name::string city,";
    sql_command += "        v:city.id::string id,";
    sql_command += "        v:main.temp::float temp_kel,";
    sql_command += "        v:main.temp_min::float temp_min_kel,";
    sql_command += "        v:main.temp_max::float temp_max_kel,";
    sql_command += "        v:weather[0].main::string conditions,";
    sql_command += "        v:wind.deg::float wind_dir,";
    sql_command += "        v:wind.speed::float wind_speed";
    sql_command += "    FROM raw_data_stream";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "JSON extracted.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

-- read transformed_data_stream and convert to celcius in final_table
CREATE OR REPLACE PROCEDURE stored_proc_aggregate_final()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO final_table (date,country,city,id,temp_cel,temp_min_cel,temp_max_cel,conditions,wind_dir,wind_speed)";
    sql_command += "    SELECT";
    sql_command += "        date,";
    sql_command += "        country,";
    sql_command += "        city,";
    sql_command += "        id,";
    sql_command += "        temp_kel-273.15 temp_cel,";
    sql_command += "        temp_min_kel-273.15 temp_min_cel,";
    sql_command += "        temp_max_kel-273.15 temp_max_cel,";
    sql_command += "        conditions,";
    sql_command += "        wind_dir,";
    sql_command += "        wind_speed";
    sql_command += "    FROM transformed_data_stream";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "TRANSFORMED JSON - AGGREGATED.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

select system$stream_has_data('raw_data_stream');

-- Create a task to look for newly inserted RAW data every 1 minute
CREATE OR REPLACE TASK extract_json_data 
warehouse = XSMALL_CONST_WH 
SCHEDULE = '1 minute' 
WHEN system$stream_has_data('raw_data_stream') 
AS CALL stored_proc_extract_json();

-- Create a sub-task to run after RAW data has been extracted (RUN AFTER)
CREATE OR REPLACE TASK aggregate_final_data 
warehouse = XSMALL_CONST_WH 
AFTER extract_json_data 
AS CALL stored_proc_aggregate_final();

-- Tasks are suspended by default; so we resume

ALTER TASK aggregate_final_data RESUME;
ALTER TASK extract_json_data RESUME;

SHOW TASKS;

-- switch to "Stream & Tasks 2" tab
