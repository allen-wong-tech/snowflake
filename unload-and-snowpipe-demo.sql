/*
Reference
    https://docs.snowflake.com/en/user-guide/data-load-snowpipe.html
    https://interworks.com/blog/hcalder/2020/01/23/snowpipe-101/ 
    https://github.com/allen-wong-tech/snowflake/blob/master/unload-and-snowpipe-demo.sql

AWS Notes:
    S3 | Properties | Events | Add notification | All object create events | Send to SQS Queue

Demo
    Unload to Stage (S3)
    Create SnowPipe
    Verify Data Automatically Pipes In via Simple Queue Service (SQS)

Benefits
    Stream Data in Serverless with per-second billing for low TCO and near-zero maintenance
    SnowPipe is a wrapper around Copy Into so easy to setup
    Enable near-real-time analytics on Snowflake
    

*/





--set context
use role sysadmin; use warehouse play_wh; use schema playdb.public;
alter warehouse play_wh set warehouse_size = 'xsmall';




--let's create some dummy data
  create or replace transient table pipedata as
      select 
          row_number() over (order by seq1()) n,
          RANDSTR(10, RANDOM()) r
      from table(generator(rowcount => 1000000)) order by 1;--1 million

  select top 300 * from pipedata order by 1 desc;







-----------------------------------------------------
--reset demo
  remove @stageofficial_171/nums_pipesource/;

  drop pipe if exists pipe_official_171;    





//PIPE is a wrapper around COPY INTO
  create pipe pipe_official_171 auto_ingest = true as 
      copy into nums_target_pipe from (select $1, $2, convert_timezone('America/New_York', current_timestamp()) 
                                       from @stageofficial_171/nums_pipesource/);






//create destination table
  drop table if exists nums_target_pipe;
  
  create transient table nums_target_pipe as 
      select *, convert_timezone('America/New_York', current_timestamp()) insert_ts from pipedata limit 0;





-----------------------------------------------------
--Export to cloud storage while changing the file name since SnowPipe has memory

    copy into @stageofficial_171/nums_pipesource/ModifyEachTime1 from 
        (select * from pipedata)
        max_file_size = 10000
        overwrite = false; 
        
        
        
        
        
        
    --verify files unloaded @ = stage
    ls @stageofficial_171/nums_pipesource/;

    --the pipe we created earlier pipe_official_171 is now loading; focus on pendingFileCount
    select system$pipe_status('pipe_official_171');                

--OPTION: while waiting for near real-time 1 minute
    ls @stageofficial_171/nums_pipesource/;    
    show pipes; 




-----------------------------------------------------
--target table
    select top 300 * from nums_target_pipe order by 1 desc;         

--count will match what we unloaded earlier
    select count(*) from nums_target_pipe;
    








/*
RECAP

Demo
    Unload to Stage (S3)
    Create SnowPipe
    Verify Data Automatically Pipes In via Simple Queue Service (SQS)

Benefits
    Stream Data in Serverless with per-second billing for low TCO and near-zero maintenance
    SnowPipe is a wrapper around Copy Into so easy to setup
    Enable near-real-time analytics on Snowflake
    

*/
