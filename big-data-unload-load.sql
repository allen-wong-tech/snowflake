/*
How to run big data (1.3TB compressed) unloads and loads on Snowflake using the Transaction Processing Council Decision Support Dataset (TPC-DS)

Logging
    TABLE           ROWCOUNT    SIZE COMPRESSED     VWH         UNLOAD TIME     # FILES AT 250MB    INGEST TIME
    customer        65M         2.9GB               small       46s             48                  1m1s
    store_returns   2.9B        116GB               xlarge      3m34s           640                 3m25s                       
                                                    x2large     2m1s            762                 2m
    store_sales     28B         1.3TB               x4large     5m19s           6901                5m30s             



How to:
    COPY INTO <LOCATION>    aka unload to a Stage (S3)
    COPY INTO <TABLE>       aka ingest to a Table
    Size up to save time for unload/ingest then back down to save credits
    Open the Worksheet History to track performance
    
Snowflake Prerequisites:
    Create a STAGE - connection to your cloud storage
    Create a User, Database, and VWH

Benefits:
    Save your most precious resource: employee time
    Query the Transaction Processing Council - Decision Support (TP-CDS) to learn different query patterns
    Test various data loading configurations
    
Use Cases for this script:
    Stress-test unloading and loading
    Test unloading and loading to different file formats (Delimited, JSON, Parquet)
    See impact of Virtual Warehouse Size and different configurations on performance

Open-Sourced:
    https://github.com/allen-wong-tech/snowflake/blob/master/big-data-unload-load.sql
    
References:
    https://www.snowflake.com/blog/tpc-ds-now-available-snowflake-samples/
    https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html
    https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
    https://docs.snowflake.com/en/user-guide/warehouses-overview.html#warehouse-size
    


    

    
    


    

*/


--context
use role sysadmin; use warehouse play_wh; 

create schema if not exists tpcds;
use schema playdb.tpcds;

--xsmall small medium large xlarge x2large x3large x4large
    alter warehouse play_wh set warehouse_size = 'xsmall';









--use a view so we can pull from different tables and only change this view
  drop view if exists tpcds.source_vw;
  
  
  --CHANGE THE SOURCE TABLE AS NECESSARY
  create view tpcds.source_vw as
  select *
  from snowflake_sample_data.tpcds_sf10tcl.customer;     //customer (unit test)  //store_returns (mid-sized)

  select top 3000 * from tpcds.source_vw;










--reset demo by dropping all files in specified stage
  remove @playdb.public.stageofficial_171/tpcds/;


--create empty destination table
  drop table if exists tpcds.tpcds_target;
  
  --transient table is great for staging & ELT use-cases since we don't need time travel
  create transient table tpcds.tpcds_target as 
      select *
      from tpcds.source_vw limit 0;
      
  select top 300 * from tpcds.tpcds_target;
      










--size up to save time and get more parallel operations
    --xsmall small medium large xlarge x2large x3large x4large
    alter warehouse play_wh set warehouse_size = 'small';




-----------------------------------------------------
--copy into <stage> will UNLOAD from Snowflake
    copy into @playdb.public.stageofficial_171/tpcds/load from 
        (select * from tpcds.source_vw)
        max_file_size = 262144000   //250MB
        overwrite = true
        file_format = (type = csv field_optionally_enclosed_by='"');
    
    --verify files unloaded @ = stage
    ls @playdb.public.stageofficial_171/tpcds/;


    --we can always peer into a file
    select top 30 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        from @playdb.public.stageofficial_171/tpcds/;
        
        
        
        
        
        
        
        

-----------------------------------------------------
--copy those files back into Snowflake
    copy into tpcds.tpcds_target 
    from @playdb.public.stageofficial_171/tpcds/ 
    file_format = (type = csv
        field_optionally_enclosed_by='"'        //double-quote strings 
        replace_invalid_characters = TRUE       //Snowflake supports UTF-8 characters
    );

--size down when done to save credits
    alter warehouse play_wh set warehouse_size = 'xsmall';








-----------------------------------------------------
--verify target table
    select top 3000 * from tpcds.tpcds_target;



--count will match what we unloaded earlier
    select count(*), 'source' location
    from tpcds.source_vw
        union all
    select count(*), 'target' location
    from tpcds.tpcds_target;








/*
Recap

How to:
    COPY INTO <LOCATION>    aka unload to a Stage (S3)
    COPY INTO <TABLE>       aka ingest to a Table
    Size up to save time for unload/ingest then back down to save credits
    Open the Worksheet History to track performance

Benefits:
    Save your most precious resource: employee time
    Query the Transaction Processing Council - Decision Support (TP-CDS) to learn different query patterns
    Test various data loading configurations
    
Use Cases for this script:
    Stress-test unloading and loading
    Test unloading and loading to different file formats (Delimited, JSON, Parquet)
    See impact of Virtual Warehouse Size and different configurations on performance
    
Open-Sourced:
    https://github.com/allen-wong-tech/snowflake/blob/master/big-data-unload-load.sql

References:
    https://www.snowflake.com/blog/tpc-ds-now-available-snowflake-samples/
    https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html
    https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
    https://docs.snowflake.com/en/user-guide/warehouses-overview.html#warehouse-size
    
*/
