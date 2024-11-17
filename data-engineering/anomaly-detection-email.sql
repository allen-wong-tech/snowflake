/*
Machine Learning-Based Alerts for Snowflake FinOps
https://medium.com/snowflake/machine-learning-based-alerts-for-snowflake-finops-8ec640fb1cee
*/

use schema play_db.public;


-- Build ML model using Snowflake Cortex ML-based functions
-- Step 1: Create the training dataset:

create or replace view warehouse_compute_usage_train as
  select
    to_timestamp_ntz(to_date(start_time)) as timestamp,
    sum(credits_used_compute) as credits_used
  from snowflake.account_usage.warehouse_metering_history
  where timestamp between dateadd(day,-365,current_date()) and dateadd(day,-61,current_date())
  group by all;

-- Step 2: Create the test dataset:

create or replace view warehouse_compute_usage_test as
  select
    to_timestamp_ntz(to_date(start_time)) as timestamp,
    sum(credits_used_compute) as credits_used
  from snowflake.account_usage.warehouse_metering_history
  where timestamp between dateadd(day,-60,current_date()) and current_date()
  group by all;

-- Step 3: Train an anomaly detection model using Snowflake Cortex ML-based functions:  xsmall 21sec

create or replace snowflake.ml.anomaly_detection warehouse_usage_analysis(
  input_data => system$reference('view', 'warehouse_compute_usage_train'),
  timestamp_colname => 'timestamp',
  target_colname => 'credits_used',
  label_colname => ''
  );
  
-- Step 4: Run model inference using the trained model:

call warehouse_usage_analysis!detect_anomalies(
  input_data => system$reference('view','warehouse_compute_usage_test')
  , timestamp_colname => 'timestamp'
  , target_colname => 'credits_used'
  );


-- Step 5: Visualize model results using Snowsight:
-- Chart | Add Columns (Upper Bound, Lower Bound, Forecast)

create table warehouse_usage_anomalies 
  as select * from table(result_scan(last_query_id()));

select * from warehouse_usage_anomalies 
  where is_anomaly = true;
  
-- Step 1: Create a task to retrain ML model on a weekly basis at 9 AM every Sunday:

create or replace task train_warehouse_usage_anomaly_task
warehouse = xsmall_const_wh
schedule = 'USING CRON 0 7 * * 0 America/New_York'
as
execute immediate
$$
begin
  create or replace snowflake.ml.anomaly_detection warehouse_usage_analysis(
    input_data => system$reference('view', 'warehouse_compute_usage_train'),
    timestamp_colname => 'timestamp',
    target_colname => 'credits_used',
    label_colname => ''
    );
end;
$$;

-- Step 2: Create a task to call the anomaly detection model on a daily basis at 7 AM LA time and insert the result into warehouse_usage_anomalies table:

create or replace task inference_warehouse_usage_anomaly_task
warehouse = compute_wh
schedule = 'USING CRON 0 8 * * 0 America/New_York'
as
execute immediate
$$
begin
  call warehouse_usage_analysis!detect_anomalies(
    input_data => system$reference('view','warehouse_compute_usage_test')
    , timestamp_colname => 'timestamp'
    , target_colname => 'credits_used'
    );
insert into warehouse_usage_anomalies
  select * from table(result_scan(last_query_id()));
end;
$$;


-- Step 3: Set up an alert to check every day at 9 AM if any new anomalies have been detected in warehouse compute usage:
set subj = current_account() || 'Warehouse compute usage anomaly detected';
select subj;

create or replace alert warehouse_usage_anomaly_alert
  warehouse = compute_wh
  schedule = 'USING CRON 0 9 * * 0 America/New_York'
  if (exists (select * from warehouse_usage_anomalies where is_anomaly=True and ts > dateadd('day',-1,current_timestamp())))
  then
    call  system$send_email(
    'email_integration',
    'replace@me.com',
    'Warehouse compute usage anomaly detected',
    concat(
      'Anomaly detected in the warehouse compute usage. ',
      'Value outside of confidence interval detected.'
          )
    );

CREATE or replace NOTIFICATION INTEGRATION email_integration
  TYPE=EMAIL
  ENABLED=TRUE
  ALLOWED_RECIPIENTS=('allen.wong@snowflake.com');

set subj = current_account() || 'Warehouse compute usage anomaly detected';
select subj;

call  system$send_email(
    'email_integration',
    'replace@me.com',
    'Warehouse compute usage anomaly detected',
    concat(
      'Anomaly detected in the warehouse compute usage. ',
      'Value outside of confidence interval detected.'
    )
    );
