/*
Unload to Stage (S3)
Create SnowPipe
Verify Data Automatically Pipes In via SQS

*/

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
--change file name to simulate new file
--trigger snowpipe by adding file(s) with new name
    copy into @stageofficial_171/nums_pipesource/ModifyEachTime14 from 
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
