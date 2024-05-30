/*
Setup Document AI for accountadmin usage
https://docs.snowflake.com/en/user-guide/snowflake-cortex/document-ai/setting-up

*/

use role accountadmin;
create database if not exists doc_ai_db;
grant usage on database doc_ai_db to role accountadmin;
grant database role snowflake.document_intelligence_creator to role accountadmin;
create warehouse if not exists doc_ai_wh with warehouse_size = 'xsmall' auto_suspend = 120 initially_suspended = true;
create schema doc_ai_schema;
grant usage on schema doc_ai_db.doc_ai_schema to role accountadmin;
use schema doc_ai_db.doc_ai_schema;
create stage doc_ai_stage directory = (enable = true) encryption = (type = 'SNOWFLAKE_SSE');
grant create snowflake.ml.document_intelligence on schema doc_ai_db.doc_ai_schema to role accountadmin;
alter user john set default_namespace = doc_ai_db.doc_ai_schema;
alter user john set default_warehouse = doc_ai_wh;
--then relogin to Snowflake
