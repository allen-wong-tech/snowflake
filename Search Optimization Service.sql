-----------------------------------------------------
--Executive Summary:
    --Search Optimization Service gives 13x (104seconds vs 8 seconds) improvement in query speed
    --At one company, across 16M jobs, it has reduced files scanned by 99%
    --Analogous to online availability of non-clustered indexes

    /*Public Preview Documentation
        https://docs.snowflake.com/en/user-guide/search-optimization-service.html
    */





--Hypothesis
    --Cuts compute costs by optimizing performance at a fraction of current developer time














//Challenges with Current State:
    //Point (Select, Update, Delete, Merge) Queries are slow
    //Queries not searching on the clustering key are slow and analytics are delayed
    //Customers would use less-efficient approaches to speed up these queries, e.g. larger warehouses, Materialized Views, and clustering
    //Customers would buy and maintain a system (OLTP, NoSQL) outside of Snowflake









//Future State: 
    //Demonstrated 13x improvement on query duration with use of Search Optimization 
        //4 DML sped up: Select, Delete, Update, Merge
    //Users and apps get the data and answers they need in seconds; no user frustration from slow queries
    //Reduced costs by being able to use smaller virtual warehouses for these queries
    //Fewer data silos and complexity by running these queries in Snowflake and not another database
    
    
    
    
    
    
    
    
    
    

//Business Outcomes:
    //Cost optimization due to reduced compute needs
    //Significantly faster query performance
    //Better experience for users 







-----------------------------------------------------
--FYI: Setup Data
    create database feature_db; create schema search; use warehouse poc_wh;  

    use SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;

    --On 3XL 15min
    create transient table feature_db.search.store_sales_denorm_sos as
    select * 
    from store_sales
       inner join customer on ss_customer_sk = c_customer_sk
       inner join store on ss_store_sk = s_store_sk;







-----------------------------------------------------
--FYI: Setup Search Optimization Service
    use feature_db.search; 
    alter table store_sales_denorm_sos add search optimization;

    --notice Search Optimization Service 3 columns at end
    show tables like 'store_sales_denorm_sos';








-----------------------------------------------------
--Setup
use feature_db.search; alter warehouse poc_wh set warehouse_size = 'xlarge';

alter session set use_cached_result=false;      //disable query cache
alter warehouse poc_wh suspend;                 //disable virtual warehouse cache
alter warehouse poc_wh resume;         






    

-----------------------------------------------------
--see query plan: search optimization access --8 seconds
    select * 
    from feature_db.search.store_sales_denorm_sos
    where
        c_email_address = 'Evelyn.Lewis@bDOE.org'
        and s_store_name = 'ation'
    order by ss_item_sk;
        
    --notice:
        --Search Optimization Service step
        --pruning
        
        

-----------------------------------------------------
--compare with base --1min 44secs with xlarge
    use SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;
    
    select *
    from store_sales
        inner join customer on ss_customer_sk = c_customer_sk
        inner join store on ss_store_sk = s_store_sk
    where
        c_email_address = 'Evelyn.Lewis@bDOE.org'
        and s_store_name = 'ation'
    order by ss_item_sk;
        
    --notice:
        --13x (104 seconds vs 8 seconds) improvement in duration, bytes scanned













-----------------------------------------------------
--Optional
    use role sysadmin; 
    alter warehouse poc_wh set warehouse_size = 'small';