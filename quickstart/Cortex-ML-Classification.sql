/*
Getting Started with Snowflake Cortex ML Classification

https://quickstarts.snowflake.com/guide/cortex_ml_classification/index.html?index=..%2F..index#1

https://archive.ics.uci.edu/dataset/222/bank+marketing?_fsi=zw9bettj&_fsi=zw9bettj&_fsi=zw9bettj&_fsi=zw9bettj
*/

----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
--SETUP - neither need to drop nor rerun

        -- Using accountadmin is often suggested for quickstarts, but any role with sufficient privledges can work
        USE ROLE ACCOUNTADMIN;
        
        -- Create development database, schema for our work: 
        CREATE OR REPLACE DATABASE quickstart;
        CREATE OR REPLACE SCHEMA ml_functions;
        
        -- Use appropriate resources: 
        USE DATABASE quickstart;
        USE SCHEMA ml_functions;
        
        -- Create warehouse to work with: 
        CREATE OR REPLACE WAREHOUSE quickstart_wh with warehouse_size = 'xsmall' auto_suspend = 60 initially_suspended = true;;
        USE WAREHOUSE quickstart_wh;
        
        -- Create a csv file format to be used to ingest from the stage: 
        CREATE OR REPLACE FILE FORMAT quickstart.ml_functions.csv_ff
            TYPE = 'csv'
            SKIP_HEADER = 1,
            COMPRESSION = AUTO;
        
        -- Create an external stage pointing to AWS S3 for loading our data:
        CREATE OR REPLACE STAGE s3load 
            COMMENT = 'Quickstart S3 Stage Connection'
            URL = 's3://sfquickstarts/frostbyte_tastybytes/mlpf_quickstart/'
            FILE_FORMAT = quickstart.ml_functions.csv_ff;
        
        -- Define our table schema
        CREATE OR REPLACE TABLE quickstart.ml_functions.bank_marketing(
            AGE NUMBER,
            JOB TEXT, 
            MARITAL TEXT, 
            EDUCATION TEXT, 
            DEFAULT TEXT, 
            HOUSING TEXT, 
            LOAN TEXT, 
            CONTACT TEXT, 
            MONTH TEXT, 
            DAY_OF_WEEK TEXT, 
            DURATION NUMBER(4, 0), 
            CAMPAIGN NUMBER(2, 0), 
            PDAYS NUMBER(3, 0), 
            PREVIOUS NUMBER(1, 0), 
            POUTCOME TEXT, 
            EMPLOYEE_VARIATION_RATE NUMBER(2, 1), 
            CONSUMER_PRICE_INDEX NUMBER(5, 3), 
            CONSUMER_CONFIDENCE_INDEX NUMBER(3,1), 
            EURIBOR_3_MONTH_RATE NUMBER(4, 3),
            NUMBER_EMPLOYEES NUMBER(5, 1),
            CLIENT_SUBSCRIBED BOOLEAN);
        
        -- Ingest data from S3 into our table:
        COPY INTO quickstart.ml_functions.bank_marketing
        FROM @s3load/cortex_ml_classification.csv;
        
        
        
        
        




--------------------------------------------------------
--DEMO

--Exploratory Data Analysis (filter on right sidebar)
SELECT * FROM bank_marketing;

-- 11 percent subscribed
SELECT
    client_subscribed, 
    COUNT(1) cnt,
    100 * RATIO_TO_REPORT(count(1)) OVER () percent
FROM bank_marketing
group by 1
order by 1 desc;

--target (prediction) variable
SELECT * FROM bank_marketing where client_subscribed = 'TRUE';


-- Count of subscribed vs not subscribed: 
SELECT client_subscribed, COUNT(1) as num_rows
FROM bank_marketing
GROUP BY 1;


-- Randomly assign 95% to training and 5% to inference
CREATE OR REPLACE VIEW partitioned_data as (
  SELECT *, 
        CASE WHEN UNIFORM(0::float, 1::float, RANDOM()) < .95 THEN 'training' ELSE 'inference' END AS split_group
  FROM bank_marketing
);

-- Training data view: 
CREATE OR REPLACE VIEW training_view AS (
  SELECT * EXCLUDE split_group
  FROM partitioned_data 
  WHERE split_group LIKE 'training');

-- Inference data view
CREATE OR REPLACE VIEW inference_view AS (
  SELECT * EXCLUDE split_group
  FROM partitioned_data 
  WHERE split_group LIKE 'inference');

-- Train our classifier 16s on xsmall
CREATE OR REPLACE snowflake.ml.classification bank_classifier(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'training_view'),
    TARGET_COLNAME => 'CLIENT_SUBSCRIBED',
    CONFIG_OBJECT => {'evaluate': TRUE , 'on_error': 'skip'});

SHOW snowflake.ml.classification;







-- Create the Predictions as JSON
SELECT bank_classifier!PREDICT(INPUT_DATA => object_construct(*))
    AS prediction FROM inference_view;

-- Manipulate JSON to structured
CREATE OR REPLACE TABLE predictions AS (
SELECT 
    CLIENT_SUBSCRIBED,
    prediction:class::boolean as prediction, 
    prediction:probability:False as false_probability,
    prediction:probability:True as true_probability
FROM
    (
    SELECT bank_classifier!PREDICT(object_construct(*)) AS prediction, CLIENT_SUBSCRIBED
    FROM inference_view
    ));

--raw predictions
select *
from predictions;






    
--heatgrid chart actual vs predicted
CALL bank_classifier!SHOW_CONFUSION_MATRIX();

--precision, recall, f1
CALL bank_classifier!SHOW_EVALUATION_METRICS();

--AUC: area under the curve
CALL bank_classifier!SHOW_GLOBAL_EVALUATION_METRICS();

--precision vs recall curve; what is the cost of false positive and false negative?
CALL bank_classifier!SHOW_THRESHOLD_METRICS();

--what contributed most to predicion?
CALL bank_classifier!SHOW_FEATURE_IMPORTANCE();

    --let's sort the score
    set q = last_query_id();

    select *
    from table(result_scan($q))
    order by "SCORE" desc;
    
