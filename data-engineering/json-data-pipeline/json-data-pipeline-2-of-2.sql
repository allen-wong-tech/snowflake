/*--------------------------------------------------------------------------------
https://github.com/allen-wong-tech/snowflake/new/master/data-engineering/json-data-pipeline-2-of-2.sql
https://github.com/kromozome2003/Snowflake-Json-DataPipeline


What we will show:
    copy into
        raw_json_table 
    stream has data
    extract_json_data task wakes once a minute; if streams has data call proc
        transformed_json_table - 
        aggregate_final_data task runs stored_proc_aggregate_final
            final_table


--------------------------------------------------------------------------------*/
use role sysadmin;
USE SCHEMA CDPST.PUBLIC;
use warehouse play_wh;


-- Insert some data to the raw_json_table (you can repeat the COPY with others weather json files in the json-files folder)
COPY INTO raw_json_table FROM (SELECT $1 FROM @CDPST.PUBLIC.cdpst_json_files/weather1.json.gz (FILE_FORMAT => CDPST.PUBLIC.JSON));--118K
COPY INTO raw_json_table FROM (SELECT $1 FROM @CDPST.PUBLIC.cdpst_json_files/weather2.json.gz (FILE_FORMAT => CDPST.PUBLIC.JSON));
COPY INTO raw_json_table FROM (SELECT $1 FROM @CDPST.PUBLIC.cdpst_json_files/weather3.json.gz (FILE_FORMAT => CDPST.PUBLIC.JSON));
COPY INTO raw_json_table FROM (SELECT $1 FROM @CDPST.PUBLIC.cdpst_json_files/weather4.json.gz (FILE_FORMAT => CDPST.PUBLIC.JSON));



select * from raw_json_table;


--The Snowflake task runs this every minute
select system$stream_has_data('raw_data_stream');

-- Read the content of the newly created stream if DML hasn't been run on it yet
SELECT * FROM raw_data_stream;
SELECT * FROM transformed_data_stream;

--populated via task; notice Kelvin
SELECT * FROM transformed_json_table;

--populated via child task; notice Celcius
SELECT * FROM final_table;




-----------------------------------------------------
--What's going on?

-- How long do we have to wait for next run ?
SELECT timestampdiff(second, CURRENT_TIMESTAMP, scheduled_time) AS next_run, scheduled_time, CURRENT_TIMESTAMP, name, state
FROM TABLE(information_schema.task_history()) WHERE state = 'SCHEDULED' ORDER BY completed_time DESC;

--task history
select * from table(information_schema.task_history())
order by scheduled_time desc;




----------------------------------------------------------------------------------------------------------
--

                                      /*--------------------------------------------------------------------------------
                                        CLEAN UP
                                      --------------------------------------------------------------------------------*/
                                      DROP STREAM IF EXISTS raw_data_stream;
                                      DROP STREAM IF EXISTS transformed_data_stream;
                                      DROP TABLE IF EXISTS raw_json_table;
                                      DROP TABLE IF EXISTS transformed_json_table;
                                      DROP TABLE IF EXISTS final_table;
                                      DROP PROCEDURE IF EXISTS stored_proc_extract_json();
                                      DROP PROCEDURE IF EXISTS stored_proc_aggregate_final();

                                      ALTER TASK if exists extract_json_data suspend;
                                      ALTER TASK if exists aggregate_final_data suspend;

                                      DROP TASK IF EXISTS extract_json_data;
                                      DROP TASK IF EXISTS aggregate_final_data;
                                      DROP FILE FORMAT IF EXISTS CDPST.PUBLIC.JSON;
                                      //DROP STAGE IF EXISTS CDPST.PUBLIC.cdpst_json_files;


--when done
ALTER TASK extract_json_data suspend;
ALTER TASK aggregate_final_data suspend;

/*
-- Context setting
USE SCHEMA CDPST.PUBLIC; USE WAREHOUSE load_wh;




--task history
select * from table(information_schema.task_history())
order by scheduled_time desc;




show tasks;
  
desc task CDPST.PUBLIC.EXTRACT_JSON_DATA;
desc task CDPST.PUBLIC.AGGREGATE_FINAL_DATA;

--dependencies
select *
from table(information_schema.task_dependents(task_name => 'CDPST.PUBLIC.EXTRACT_JSON_DATA', recursive => false));


*/
