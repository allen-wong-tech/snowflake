/*

Data Loading Demo (4 of 4):
Unload to Data Lake; SnowPipe: Automated, Near Real-Time

1 Create dummy data
2 Create SnowPipe (wrapper around Copy Into)
3 Unload dummy data to Stage/Data Lake (S3) 
4 Snowpipe will automatically load data in ~1 minute

*/

----------------------------------------------------------------------------------------------------------
--1 Create dummy data
    create or replace transient table pipedata as
        select 
            row_number() over (order by seq1()) n,
            RANDSTR(10, RANDOM()) r
        from table(generator(rowcount => 1000000)) order by 1;--1 million
    
    --verify
        select top 300 * from pipedata order by 1 desc;

    --reset demo
      remove @stageofficial_171/nums_pipesource/;
      drop pipe if exists pipe_official_171;    









----------------------------------------------------------------------------------------------------------
--2 Create SnowPipe (wrapper around Copy Into)
      create pipe pipe_official_171 auto_ingest = true as 
          copy into nums_target_pipe from (select $1, $2, convert_timezone('America/New_York', current_timestamp()) 
                                           from @stageofficial_171/nums_pipesource/);

    --create destination table
      drop table if exists nums_target_pipe;
      create transient table nums_target_pipe as 
          select *, convert_timezone('America/New_York', current_timestamp()) insert_ts from pipedata limit 0;












----------------------------------------------------------------------------------------------------------
--3 Unload dummy data to Stage/Data Lake (S3) 

  --CHANGE FILE NAME to simulate new file (Otherwise Snowpipe won't pick up since has memory of files loaded)
  --Note: This will trigger snowpipe by adding file(s) with new name
    copy into @stageofficial_171/nums_pipesource/ModifyEachTime17 from 
        (select * from pipedata)
        max_file_size = 10000
        overwrite = false; 
        
    --verify files unloaded @ = stage
    ls @stageofficial_171/nums_pipesource/;











----------------------------------------------------------------------------------------------------------
--4 Snowpipe will automatically load data in ~1 minute

    --the pipe we created earlier pipe_official_171 is now loading; focus on pendingFileCount
        select system$pipe_status('pipe_official_171');                

    --Optional: while waiting for near real-time 1 minute
        ls @stageofficial_171/nums_pipesource/;    
        show pipes; 

    --target table
        select top 300 * from nums_target_pipe order by 1 desc;         

    --count will match what we unloaded earlier
        select count(*) from nums_target_pipe;
    


/*

Recap:Data Loading Demo (4 of 4):
Unload to Data Lake; SnowPipe: Automated, Near Real-Time

1 Create dummy data
2 Create SnowPipe (wrapper around Copy Into)
3 Unload dummy data to Stage/Data Lake (S3) 
4 Snowpipe will automatically load data in ~1 minute

*/










-----------------------------------------------------
--If time allows: Bonus Demo

    //FOSS procedure automate JSON view creation
        use role sysadmin;
        select * from util.sf.colors;
            call util.sf.create_view_over_json('util.sf.colors', 'json_data', 'util.sf.colors_vw');
        select * from util.sf.colors_vw;

        --verify just created
        select table_name, table_type, created
        from util.information_schema.tables
        where table_schema = 'SF' and table_name = 'COLORS_VW';

        select get_ddl('view','util.sf.colors_vw'); 
            select get_ddl('procedure','util.sf.create_view_over_json(varchar, varchar, varchar)'); 
            
            






-----------------------------------------------------
--Optional: Data Load History
    select *
      from table(information_schema.pipe_usage_history(
        date_range_start=>dateadd('hour',-12,current_timestamp()),
        pipe_name=>'playdb.public.pipe_official_171'));











----------------------------------------------------------------------------------------------------------
--setup pipe
    //https://interworks.com/blog/hcalder/2020/01/23/snowpipe-101/    
    //note notification_channel
        show pipes;    
    //S3 | Properties | Events | Add notification | All object create events | Send to SQS Queue








-----------------------------------------------------
--context
    use role sysadmin; use warehouse playwh; use schema playdb.public;
    alter warehouse playwh set warehouse_size = 'xsmall';
