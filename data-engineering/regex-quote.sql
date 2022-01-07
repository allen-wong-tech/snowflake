/*
Challenge
    A CSV file is sent with unescaped double quotes within their double-quoted text fields.
    This is causing ingestion to break.
    
Fix
    We use Snowflake regex and open-source regex functions to load the data
   
Benefit
    Even upstream files with data-quality issues can be cleaned up automatically and ingested into Snowflake

prerequisite: run this FOSS regex library:
    https://github.com/GregPavlik/SnowflakeUDFs/blob/main/RegularExpressions/regexp2.sql
    https://github.com/allen-wong-tech/snowflake/blob/master/data-engineering/regex-quote.sql


*/

-----------------------------------------------------
--Test Driven Development: something and other thing have issues but we don't want to affect the valid quotes
create or replace temp table b (raw varchar);
insert into b values('"A","123","SOME"THING",other""thing,"DONALD"TACK"BAK","a","",,"foo"');
insert into b values('"B","123","SOME"THING",other""thing,"DONALD"TACK"BAK","a","",,"foo"');

--notice quotes not next to a comma
select * from b;

--use regex to remove invalid double-quotes
create or replace temp table c as
select
    --raw data 
    b.raw,
    --first character is always good and could be a double-quote so always include
    left(raw,1)     
    --Greg Pavlik's FOSS regex library to ignore valid double-quotes which will always be next to a comma:
    || official..regexp_replace2(
            substr(raw,2,len(raw)-2),
            '(?<!,)\"(?!,)','^^^')--temporarily replace invalid double-quotes with ^^^ --note: any Unicode
    --last character is always good and could be a double-quote so always include
    || right(raw,1) as conformed    
from b;

--persist again to see data pipeline
create or replace temp table d as
select
    *,
    replace(conformed,'"','') conformed2,
    replace(
        replace(conformed,'"','')    
          ,'^^^','"') conformed3
from c;

--see data pipeline before final version
select
    raw,
    conformed,      --^^^ replaces " that is not next to a comma
    conformed2,     --double-quotes used for quoted-delimiters now removed
    conformed3      --valid data
from d;

--create an end-user ready view ready to separate into columns
create or replace temp view end_user_view as
select
//    raw,
    split_part(d.conformed3,',',1) ID,
    split_part(d.conformed3,',',2) part_id, 
    split_part(d.conformed3,',',3) text1, 
    split_part(d.conformed3,',',4) text2, 
    split_part(d.conformed3,',',5) other_id, 
    split_part(d.conformed3,',',6) text3, 
    split_part(d.conformed3,',',7) text4, 
    split_part(d.conformed3,',',8) text5, 
    split_part(d.conformed3,',',9) text6
from d;

create or replace temp table end_user_tbl as
select * from end_user_view;

--present to user as either a table or view
select * from end_user_tbl;

select * from end_user_view;




/*
References

--negative lookahead assertion
--https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_Expressions/Cheatsheet
--https://community.snowflake.com/s/question/0D50Z00007ENLKsSAP/expanded-support-for-regular-expressions-regex

-- Running the UDF approximating the base function returns foo***barfoo
select official..regexp_replace2('foobarbarfoo', 'bar(?=bar)', '***');
  
-- Running the UDF approximating the base function returns TRUE
select official..rlike2('foobarbarfoo', 'bar(?=bar)');

--often good to store metadata with the raw version
create temp table b (raw varchar, rownum int, isError boolean, note varchar, insertDateTime timestamp);

--context
use role sysadmin; use warehouse play_wh; use schema official.public;
alter warehouse play_wh set warehouse_size = 'xsmall';

*/
