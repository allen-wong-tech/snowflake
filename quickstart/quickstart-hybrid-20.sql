/*
https://quickstarts.snowflake.com/guide/getting_started_with_hybrid_tables/index.html?index=..%2F..index#4

*/

-- Step 4.5
    USE ROLE HYBRID_QUICKSTART_ROLE;
    USE WAREHOUSE HYBRID_QUICKSTART_WH;
    USE SCHEMA HYBRID_QUICKSTART_DB.DATA;
    
    SET MIN_ORDER_ID = (SELECT min(order_id) from ORDER_HEADER);
    SELECT $MIN_ORDER_ID;

    
    show transactions;
    
    --success since row level locking
    UPDATE ORDER_HEADER
    SET order_status = 'COMPLETED'
    WHERE order_id = $MIN_ORDER_ID;

    
-- Open "Hybrid Table - QuickStart" worksheet and run a commit statement to commit the open transaction.
