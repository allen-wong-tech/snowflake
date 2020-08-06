-----------------------------------------------------
--Executive Summary
--Materialized View (MV)
    --17x faster queries (17sec current state vs 1sec MV)
    
    /*Currently in Private Preview
        https://docs.snowflake.com/en/LIMITEDACCESS/view-materialized-query-auto-rewrite.html
    */

--Materialized View with Auto Rewrite
    --Just query base table and Snowflake will automatically use the MV when it will answer the query in a quicker fashion






--Hypothesis
    --Materialized View with Automatic Query Rewrite will help you minimize expensive developer time and compute costs while optimizing performance








//Challenges with Current State:
    //Customers have to manually rewrite existing queries to leverage MVs. This uses expensive developer time and has operational risk.  
    //Business Intelligence tools and business users might still query the base table.
    //Queries against base tables and views require more compute cost
    //Slower queries lead to poor end-user experience









//Future State:
    //Demonstrated 17x improvement on query duration with use of Materialized Views
    //Queries run significantly faster and users can make more decisions from their data
    //Compute costs are reduced
    //Developers can spend more time on high-value tasks by automating this task away













-----------------------------------------------------
--Setup

    --1.5B rows, 49GB
    CREATE TABLE feature_db.tpch_sf1000.orders AS SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."tpch_sf1000"."ORDERS" order by o_orderdate;

    CREATE MATERIALIZED VIEW feature_db.tpch_sf1000.orders_by_cust_mv
        cluster by (o_custkey) AS
    SELECT o_custkey, sum(o_totalprice) AS sum_total_price, count(1) AS num_orders
    FROM feature_db.tpch_sf1000.orders
    GROUP BY 1;








-----------------------------------------------------
--remove caching for fair test
    alter warehouse poc_wh set warehouse_size = 'small';
    alter session set use_cached_result=false;  //disable global cache
    alter warehouse poc_wh suspend;             //disable virtual warehouse cache
    alter warehouse poc_wh resume;









--Current Way: 17 seconds on Small Warehouse
    SELECT o_custkey, sum(o_totalprice) AS sum_total_price, count(1) AS num_orders
    FROM snowflake_sample_data.tpch_sf1000.orders
    where o_custkey = 88271155
    GROUP BY 1;

    --in execution plan notice there is no pruning








--notice base table still uses MV (with auto rewrite)
    SELECT o_custkey, sum(o_totalprice), count(1)
    FROM feature_db.tpch_sf1000.orders
    where o_custkey = 88271155
    GROUP BY 1
    order by 1;

    --notice
        --MV in execution plan, though hitting base table (no need to rewrite)
        --pruning
        --17x faster (17 seconds vs 1 second)













-----------------------------------------------------
--Summary
    //Ideal for write rarely, select frequently












-----------------------------------------------------
--Optional: setup
use schema feature_db.tpch_sf1000;
use role sysadmin;
use warehouse poc_wh;
create schema tpch_sf1000;
show tables;
