/*
Summary:
    How to run Snowpipe and Copy Into for big data (1.3TB Compressed / 4TB Uncompressed) unloads and loads

Prerequisites:
    This will create a Stage @S3_db.public.S3_stage and monitor for Snowpipe.  We need S3 stage for auto-ingest
        https://quickstarts.snowflake.com/guide/getting_started_with_snowpipe/index.html?index=..%2F..index#0

Open-Sourced:
    https://github.com/allen-wong-tech/snowflake/blob/master/big-data-unload-load.sql

Youtube demo and explanation of this script:
    Version 2 (TBD)
    Version 1 (Back in May 2021, we ran a 4XL for 5min 30sec): https://www.youtube.com/watch?v=Zsr2OONlMYY
    
Actual Results
    TABLE           ROWCOUNT    SIZE COMPRESSED     VWH         UNLOAD TIME     # FILES AT 250MB    INGEST TIME    WHEN
    customer        65M         2.9GB               small       46s             48                  1m1s
    store_returns   2.9B        116GB               xlarge      3m34s           640                 3m25s                       
                                                    x2large     2m              762                 2m
                                                    x2large     1m25s           762                 2m
    store_sales     28B         1.3TB (4TB Uncom)   x4large     5m19s           6901                5m30s          April 2021       
    store_sales     28B         1.3TB (4TB Uncom)   x4large     4m6s            7161                4m19s          May 2023 
    store_sales     28B         1.3TB (4TB Uncom)   x5large     2m14s           8053                2m20s          May 2023 
    store_sales     28B         1.3TB (4TB Uncom)   x6large     1m35s           8000                1m16s          May 2023 


Benefits:
    Save your most precious resource: human time
    Test various data loading configurations
    
Agenda:
    PART 1: SETUP
    PART 2: COPY INTO STAGE (BLOB STORAGE)
    PART 3: SNOWPIPE into tpcds_target_snowpipe
    PART 4: COPY INTO tpcds_target_copy_into


*/



----------------------------------------------------------------------------------------------------------
--PART 1: SETUP
    use role sysadmin;

    --warehouse, database, schema
    create warehouse if not exists compute_wh with warehouse_size = 'xsmall' auto_suspend = 60 initially_suspended = true;
    
    --specifically name a warehouse that we use for store_sales to easier see credit consumption
    create warehouse if not exists x4large_wh with warehouse_size = 'x4large' auto_suspend = 1 initially_suspended = true;
    -- create warehouse if not exists x5large_wh with warehouse_size = 'x5large' auto_suspend = 1 initially_suspended = true;

    use warehouse compute_wh;
    
    create database if not exists play_db;
    create schema if not exists play_db.tpcds;
    
    use schema play_db.tpcds;
    
    --use a view so we can pull from different tables and only change this view
    drop view if exists tpcds.source_vw;
    
    --CHANGE THE SOURCE TABLE AS NECESSARY
    --Find and Replace snow_<table> with snow_<new_table>
    create view tpcds.source_vw as
    select * from snowflake_sample_data.tpcds_sf10tcl.customer;     //customer (65M)  //store_returns (2.9B)    //store_sales (28B)
    
    select top 300 * from tpcds.source_vw;

        ----------------------------------------------------------------------------------------------------------
        --PART 1A: CREATE EMPTY DESTINATION TABLES
        --snowpipe
        drop table if exists tpcds.tpcds_target_snowpipe;
        create transient table tpcds.tpcds_target_snowpipe as select * from tpcds.source_vw limit 0;
        
        --copy into
        drop table if exists tpcds.tpcds_target_copy_into;
        create transient table tpcds.tpcds_target_copy_into as select * from tpcds.source_vw limit 0;

        --copy into Parquet
        drop table if exists tpcds.tpcds_target_copy_into_parquet;
        create transient table tpcds.tpcds_target_copy_into_parquet as select * from tpcds.source_vw limit 0;

        
        --counts
        select count(*), 'source' location from tpcds.source_vw union all
        select count(*), 'target_snowpipe' location from tpcds.tpcds_target_snowpipe union all
        select count(*), 'target_copy_into' location from tpcds.tpcds_target_copy_into union all
        select count(*), 'target_copy_into_parquet' location from tpcds.tpcds_target_copy_into_parquet
        ;

        --Create Snowpipe
        drop pipe if exists play_db.tpcds.pipe_171_snow_customer;
        
        //PIPE is a wrapper around COPY INTO
        create pipe play_db.tpcds.pipe_171_snow_customer auto_ingest = true as 
        copy into tpcds.tpcds_target_snowpipe
            from @S3_db.public.S3_stage/tpcds/
                file_format = (type = csv
                field_optionally_enclosed_by='"'        //double-quote strings 
                replace_invalid_characters = TRUE       //Snowflake supports UTF-8 characters
        );

    --reset demo by dropping all files in specified stage
    ls @S3_db.public.S3_stage/tpcds;
    remove @S3_db.public.S3_stage/tpcds;

    







----------------------------------------------------------------------------------------------------------
--PART 2: COPY INTO STAGE (BLOB STORAGE)

    --SIZE UP: small (customer)    xlarge (store_returns)    x4large (store_sales)
    use warehouse compute_wh;
    alter warehouse compute_wh set warehouse_size = 'small' wait_for_completion = true;
    
        -- use warehouse x4large_wh; alter warehouse x4large_wh resume;
        -- use warehouse x5large_wh;
    
            --copy into <stage> will UNLOAD from Snowflake
                copy into @S3_db.public.S3_stage/tpcds/snow_customer from 
                    (select * from tpcds.source_vw)
                    max_file_size = 262144000   //250MB
                    overwrite = true
            //        file_format = (type = parquet);
                    file_format = (type = csv field_optionally_enclosed_by='"');

                copy into @S3_db.public.S3_stage/tpcds/snow_customer_parquet from 
                    (select * from tpcds.source_vw)
                    max_file_size = 262144000   //250MB
                    overwrite = true
                    file_format = (type = parquet);
                    
        -- alter warehouse x4large_wh suspend;
        use warehouse compute_wh;
    
    --SIZE DOWN:
    alter warehouse compute_wh suspend;
    alter warehouse compute_wh set warehouse_size = 'xsmall' wait_for_completion = true;
    

        
    --verify files unloaded @ = stage
    ls @S3_db.public.S3_stage/tpcds/;

    --we can always peer into a file
    select top 30 $1, $2, $3, $4, $5 from @S3_db.public.S3_stage/tpcds/;
        
        






        
----------------------------------------------------------------------------------------------------------
--PART 3: SNOWPIPE into tpcds_target_snowpipe

    --counts
    select count(*), 'source' location from tpcds.source_vw union all
    select count(*), 'target_snowpipe' location from tpcds.tpcds_target_snowpipe union all
    select count(*), 'target_copy_into' location from tpcds.tpcds_target_copy_into union all
    select count(*), 'target_copy_into_parquet' location from tpcds.tpcds_target_copy_into_parquet
    ;

    
    select system$pipe_status('pipe_171_snow_customer');      

    select top 300 * from tpcds_target_snowpipe;





        
        
        
----------------------------------------------------------------------------------------------------------
--PART 4: COPY INTO tpcds_target_copy_into
    --SIZE UP: small (customer)    xlarge (store_returns)    x4large (store_sales)
    -- alter warehouse compute_wh resume;
    alter warehouse compute_wh set warehouse_size = 'small' wait_for_completion = true;

        -- use warehouse x4large_wh;
        -- use warehouse x5large_wh;
    
        copy into tpcds.tpcds_target_copy_into 
        from @S3_db.public.S3_stage/tpcds/ 
        file_format = (type = csv
            field_optionally_enclosed_by='"'        //double-quote strings 
            replace_invalid_characters = TRUE       //Snowflake supports UTF-8 characters
        );

        use warehouse compute_wh;
        
    --SIZE DOWN:
    alter warehouse compute_wh suspend;
    alter warehouse compute_wh set warehouse_size = 'xsmall' wait_for_completion = true;

    --counts
    select count(*), 'source' location from tpcds.source_vw union all
    select count(*), 'target_snowpipe' location from tpcds.tpcds_target_snowpipe union all
    select count(*), 'target_copy_into' location from tpcds.tpcds_target_copy_into union all
    select count(*), 'target_copy_into_parquet' location from tpcds.tpcds_target_copy_into_parquet
    ;


-----------------------------------------------------
--RESET


truncate table tpcds.tpcds_target_snowpipe;
truncate table tpcds.tpcds_target_copy_into;
truncate table tpcds.tpcds_target_copy_into_parquet;

show pipes;
drop pipe if exists pipe_171_snow_customer;

ls @S3_db.public.S3_stage/tpcds;
remove @S3_db.public.S3_stage/tpcds;

drop warehouse if exists x4large_wh;
drop warehouse if exists x5large_wh;


/* COMPLETE RESET
-- drop warehouse if exists compute_wh;
-- drop database play_db;
*/



/*
RECAP

--PART 1: SETUP
--PART 2: COPY INTO STAGE (BLOB STORAGE)
--PART 3: SNOWPIPE into tpcds_target_snowpipe
--PART 4: COPY INTO tpcds_target_copy_into

*/
