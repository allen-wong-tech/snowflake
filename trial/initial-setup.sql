/*
get compute_wh ready for stress-testing via multi-clustering
create accountadmin and sysadmin users for new account
*/

use role accountadmin;

--compute warehouse
create warehouse if not exists compute_wh with warehouse_size = 'xsmall' auto_suspend = 60 initially_suspended = true max_cluster_count = 10;
alter warehouse compute_wh set auto_suspend = 60 max_cluster_count = 10; 
grant modify, monitor, usage, operate on warehouse compute_wh to role sysadmin;
grant usage on warehouse compute_wh to role public;

--recommended to have at least two accountadmins in case one is unavailable
create user abc password = 'changeMe123', default_warehouse = compute_wh, default_role = accountadmin, MUST_CHANGE_PASSWORD = TRUE; 
grant role accountadmin to user abc;

--sysadmin
create or replace user xyz password = 'changeMe123', default_warehouse = compute_wh, default_role = sysadmin, MUST_CHANGE_PASSWORD = TRUE; 
grant role sysadmin to user xyz;
