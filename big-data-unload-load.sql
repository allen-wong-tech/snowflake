/*
Summary:
    How to run Snowpipe and Copy Into for big data (1.3TB Compressed / 4TB Uncompressed) unloads and loads

Prerequisites:
    This will create a Stage @S3_db.public.S3_stage and monitor for Snowpipe.  We need S3 stage for auto-ingest
        https://quickstarts.snowflake.com/guide/getting_started_with_snowpipe/index.html?index=..%2F..index#0

Open-Sourced:
    https://github.com/allen-wong-tech/snowflake/blob/master/big-data-unload-load.sql

Youtube demo and explanation of this script:
    Version 1 (Back in May 2021, we ran a 4XL for 5min 30sec): https://www.youtube.com/watch?v=Zsr2OONlMYY
    
Actual Results
    TABLE           ROWCOUNT    SIZE COMPRESSED     VWH         UNLOAD TIME     # FILES AT 250MB    INGEST TIME    WHEN
    customer        65M         2.9GB               medium      19s             32                  19s
    store_returns   2.9B        116GB               xlarge      3m34s           640                 3m25s                       
                                                    x2large     2m              762                 2m
                                                    x2large     1m25s           762                 2m
    store_sales     28B         1.3TB (4TB Uncom)   x4large     5m19s           6901                5m30s          April 2021       
    store_sales     28B         1.3TB (4TB Uncom)   x4large     4m6s            7161                4m19s          May 2023 
    store_sales     28B         1.3TB (4TB Uncom)   x5large     2m14s           8053                2m20s          May 2023 
    store_sales     28B         1.3TB (4TB Uncom)   x6large     1m35s           8000                1m16s          May 2023 


Benefits:
    Save your most precious resource: human time
    Test various compute sizes
    
Agenda:
    PART 1: SETUP
    PART 2: COPY INTO STAGE (BLOB STORAGE)
    PART 3: SNOWPIPE into tpcds_target_snowpipe
    PART 3A: SNOWPIPE into tpcds_target_snowpipe_json
    PART 4: COPY INTO tpcds_target_copy_into
    PART 4A: COPY INTO tpcds_target_copy_into_json


*/



----------------------------------------------------------------------------------------------------------
--PART 1: SETUP
    use role sysadmin;

    --warehouse, database, schema
    create warehouse if not exists compute_wh with warehouse_size = 'xsmall' auto_suspend = 60 initially_suspended = true;
    
    --specifically name a warehouse that we use for store_sales to easier see credit consumption
    create warehouse if not exists x4large_wh with warehouse_size = 'x4large' auto_suspend = 60 initially_suspended = true;

    use warehouse compute_wh;
    
    create database if not exists play_db;
    create schema if not exists play_db.tpcds;
    
    use schema play_db.tpcds;
    
    --use a view so we can pull from different tables and only change this view
    drop view if exists tpcds.source_vw;
    
    --source table rowcount options: customer 65M; store_returns 2.9B; store_sales 28B
    create or replace view tpcds.source_vw as
    select * from snowflake_sample_data.tpcds_sf10tcl.customer;  
    
    select top 300 * from tpcds.source_vw;

        ----------------------------------------------------------------------------------------------------------
        --PART 1A: CREATE EMPTY DESTINATION TABLES
        --snowpipe target CSV & JSON
        create or replace transient table tpcds.tpcds_target_snowpipe as select * from tpcds.source_vw limit 0;
        create or replace transient table tpcds.tpcds_target_snowpipe_json as select object_construct(*) v from tpcds.source_vw limit 0;

        --copy into target CSV & JSON
        create or replace transient table tpcds.tpcds_target_copy_into as select * from tpcds.source_vw limit 0;
        create or replace transient table tpcds.tpcds_target_copy_into_json as select object_construct(*) v from tpcds.source_vw limit 0;
        
        --counts
        select count(*), 'source' location from tpcds.source_vw union all
        select count(*), 'target_snowpipe_CSV' location from tpcds.tpcds_target_snowpipe union all
        select count(*), 'target_snowpipe_JSON' location from tpcds.tpcds_target_snowpipe_JSON union all
        select count(*), 'target_copy_into' location from tpcds.tpcds_target_copy_into union all
        select count(*), 'target_copy_into_json' location from tpcds.tpcds_target_copy_into_json;

    --reset demo by dropping all files in specified stage
        ls @S3_db.public.S3_stage/tpcds;
        remove @S3_db.public.S3_stage/tpcds;

        
        --Snowpipe for CSV; PIPE is a wrapper around COPY INTO
        create or replace pipe play_db.tpcds.pipe_171_snow_customer_csv auto_ingest = true as 
        copy into tpcds.tpcds_target_snowpipe
            from @S3_db.public.S3_stage/tpcds/snow_customer_csv
                file_format = (type = csv
                field_optionally_enclosed_by='"'        //double-quote strings 
                replace_invalid_characters = TRUE);       //Snowflake supports UTF-8 characters
        


        --Snowpipe for JSON
        create or replace pipe play_db.tpcds.pipe_171_snow_customer_json auto_ingest = true as 
        copy into tpcds.tpcds_target_snowpipe_json
            from @S3_db.public.S3_stage/tpcds/snow_customer_json
                file_format = (type = json
                replace_invalid_characters = TRUE);       //Snowflake supports UTF-8 characters


    







----------------------------------------------------------------------------------------------------------
--PART 2: COPY INTO STAGE (BLOB STORAGE)

    --SIZE UP: medium (customer) 40s   xlarge (store_returns)    x4large (store_sales)
    use warehouse compute_wh;
    alter warehouse compute_wh set warehouse_size = 'medium' wait_for_completion = true;
    
        
            --copy into <stage> will UNLOAD from Snowflake
                copy into @S3_db.public.S3_stage/tpcds/snow_customer_csv from 
                    (select * from tpcds.source_vw)
                    max_file_size = 262144000   //250MB
                    overwrite = true
                    file_format = (type = csv field_optionally_enclosed_by='"');

                
                copy into @S3_db.public.S3_stage/tpcds/snow_customer_json from 
                    (select object_construct(*) from tpcds.source_vw)
                    max_file_size = 262144000   //250MB
                    overwrite = true
                    file_format = (type = JSON);
                    
    --SIZE DOWN:
    alter warehouse compute_wh set warehouse_size = 'xsmall' wait_for_completion = true;
        
    --notice 32 files for CSV & 32 for JSON
    ls @S3_db.public.S3_stage/tpcds/;

    --we can always peer into a file
    select top 30 $1, $2, $3, $4, $5 from @S3_db.public.S3_stage/tpcds/snow_customer_csv;
    select top 30 $1, $2, $3, $4, $5 from @S3_db.public.S3_stage/tpcds/snow_customer_json;
        
        






        
----------------------------------------------------------------------------------------------------------
--PART 3: SNOWPIPE AUTO-INGEST

    --counts
    select count(*), 'source' location from tpcds.source_vw union all
    select count(*), 'target_snowpipe_CSV' location from tpcds.tpcds_target_snowpipe union all
    select count(*), 'target_snowpipe_JSON' location from tpcds.tpcds_target_snowpipe_JSON union all
    select count(*), 'target_copy_into' location from tpcds.tpcds_target_copy_into union all
    select count(*), 'target_copy_into_json' location from tpcds.tpcds_target_copy_into_json;

    select system$pipe_status('pipe_171_snow_customer');      
    select system$pipe_status('pipe_171_snow_customer_JSON');      

    select top 300 * from tpcds_target_snowpipe;
    select top 300 * from tpcds_target_snowpipe_JSON;


        
        
----------------------------------------------------------------------------------------------------------
--PART 4: COPY INTO tpcds_target_copy_into
    --SIZE UP: medium (customer) 20 sec   xlarge (store_returns)    x4large (store_sales)
    alter warehouse compute_wh set warehouse_size = 'medium' wait_for_completion = true;
    
        copy into tpcds.tpcds_target_copy_into 
        from @S3_db.public.S3_stage/tpcds/snow_customer_csv
        file_format = (type = csv
            field_optionally_enclosed_by='"'        //double-quote strings 
            replace_invalid_characters = TRUE);       

    --PART 4A: COPY INTO target_copy_into_json
        --Medium (customer) 54 sec
        copy into tpcds.tpcds_target_copy_into_json 
        from @S3_db.public.S3_stage/tpcds/snow_customer_json
        file_format = (type = json
        replace_invalid_characters = TRUE);       

    --SIZE DOWN:
    alter warehouse compute_wh set warehouse_size = 'xsmall' wait_for_completion = true;

    --counts
    select count(*), 'source' location from tpcds.source_vw union all
    select count(*), 'target_snowpipe_CSV' location from tpcds.tpcds_target_snowpipe union all
    select count(*), 'target_snowpipe_JSON' location from tpcds.tpcds_target_snowpipe_JSON union all
    select count(*), 'target_copy_into' location from tpcds.tpcds_target_copy_into union all
    select count(*), 'target_copy_into_json' location from tpcds.tpcds_target_copy_into_json;

    select top 300 * from tpcds.tpcds_target_copy_into_json;
    

    
    
/*
RECAP

--PART 1: SETUP
--PART 2: COPY INTO STAGE (BLOB STORAGE)
--PART 3: SNOWPIPE into tpcds_target_snowpipe
--PART 3A: SNOWPIPE into tpcds_target_snowpipe_json
--PART 4: COPY INTO tpcds_target_copy_into
--PART 4A: COPY INTO tpcds_target_copy_into_json

*/






/*

-----------------------------------------------------
--RESET

truncate table tpcds.tpcds_target_snowpipe;
truncate table tpcds.tpcds_target_snowpipe_json;
truncate table tpcds.tpcds_target_copy_into;
truncate table tpcds.tpcds_target_copy_into_json;


drop table tpcds.tpcds_target_snowpipe;
drop table tpcds.tpcds_target_snowpipe_json;
drop table tpcds.tpcds_target_copy_into;
drop table tpcds.tpcds_target_copy_into_json;

drop pipe if exists pipe_171_snow_customer;
drop pipe if exists pipe_171_snow_customer_JSON;

remove @S3_db.public.S3_stage/tpcds;

drop warehouse if exists x4large_wh;



--COMPLETE RESET
-- drop warehouse if exists compute_wh;
-- drop database play_db;
*/



