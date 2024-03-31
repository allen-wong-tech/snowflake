/*
https://quickstarts.snowflake.com/guide/getting_started_with_hybrid_tables/index.html?index=..%2F..index#0
https://docs.snowflake.com/en/user-guide/tables-hybrid


2 SETUP

3 Explore hybrid tables
4 Prove Unique and Foreign Key constraints
5 Demo Row level locking
6 Consistency via transaction across hybrid and standard table 
7.Querying joined Hybrid & Standard Tables
8 Different Masks of Hybrid Tables based on RBAC; Same governance as Standard Tables

9 CLEANUP

*/


----------------------------------------------------------------------------------
--2 SETUP
    
    USE ROLE ACCOUNTADMIN;

    CREATE OR REPLACE ROLE HYBRID_QUICKSTART_ROLE;
    GRANT ROLE HYBRID_QUICKSTART_ROLE TO ROLE ACCOUNTADMIN ;

    CREATE OR REPLACE WAREHOUSE HYBRID_QUICKSTART_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 60, AUTO_RESUME= TRUE;
    GRANT OWNERSHIP ON WAREHOUSE HYBRID_QUICKSTART_WH TO ROLE HYBRID_QUICKSTART_ROLE;
    GRANT CREATE DATABASE ON ACCOUNT TO ROLE HYBRID_QUICKSTART_ROLE;
    
    CREATE OR REPLACE DATABASE HYBRID_QUICKSTART_DB;
    GRANT OWNERSHIP ON DATABASE HYBRID_QUICKSTART_DB TO ROLE HYBRID_QUICKSTART_ROLE;
    CREATE OR REPLACE SCHEMA DATA;
    GRANT OWNERSHIP ON SCHEMA HYBRID_QUICKSTART_DB.DATA TO ROLE HYBRID_QUICKSTART_ROLE;


    USE ROLE HYBRID_QUICKSTART_ROLE;
    USE SCHEMA HYBRID_QUICKSTART_DB.DATA;

    
    CREATE OR REPLACE FILE FORMAT CSV_FORMAT TYPE = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true;
    CREATE OR REPLACE STAGE FROSTBYTE_TASTY_BYTES_STAGE URL = 's3://sfquickstarts/hybrid_table_guide' FILE_FORMAT = CSV_FORMAT;
    
    list @FROSTBYTE_TASTY_BYTES_STAGE;


    SET CURRENT_TIMESTAMP = CURRENT_TIMESTAMP();
    
    --hybrid table CTAS is best practice  --27s
    CREATE OR REPLACE HYBRID TABLE TRUCK (
    	TRUCK_ID NUMBER(38,0) NOT NULL,
    	MENU_TYPE_ID NUMBER(38,0),
    	PRIMARY_CITY VARCHAR(16777216),
    	REGION VARCHAR(16777216),
    	ISO_REGION VARCHAR(16777216),
    	COUNTRY VARCHAR(16777216),
    	ISO_COUNTRY_CODE VARCHAR(16777216),
    	FRANCHISE_FLAG NUMBER(38,0),
    	YEAR NUMBER(38,0),
    	MAKE VARCHAR(16777216),
    	MODEL VARCHAR(16777216),
    	EV_FLAG NUMBER(38,0),
    	FRANCHISE_ID NUMBER(38,0),
    	TRUCK_OPENING_DATE DATE,
        	TRUCK_EMAIL VARCHAR NOT NULL UNIQUE,
        	RECORD_START_TIME TIMESTAMP,
    	primary key (TRUCK_ID) 
    	)
    	AS
    	SELECT 
    	t.$1 AS TRUCK_ID, 
    	t.$2 AS MENU_TYPE_ID,
    	t.$3 AS PRIMARY_CITY,
    	t.$4 AS REGION,
    	t.$5 AS ISO_REGION,
    	t.$6 AS COUNTRY,
    	t.$7 AS ISO_COUNTRY_CODE,
    	t.$8 AS FRANCHISE_FLAG,
    	t.$9 AS YEAR,
    	t.$10 AS MAKE,
    	t.$11 AS MODEL,
    	t.$12 AS EV_FLAG,
    	t.$13 AS FRANCHISE_ID,
    	t.$14 AS TRUCK_OPENING_DATE,
    	CONCAT(TRUCK_ID, '_truck@email.com') TRUCK_EMAIL,
        	$CURRENT_TIMESTAMP AS RECORD_START_TIME
    	FROM @FROSTBYTE_TASTY_BYTES_STAGE (pattern=>'.*TRUCK.csv') t;
    
    --standard table --2 sec
    CREATE OR REPLACE TABLE TRUCK_HISTORY (
    	TRUCK_ID NUMBER(38,0) NOT NULL,
    	MENU_TYPE_ID NUMBER(38,0),
    	PRIMARY_CITY VARCHAR(16777216),
    	REGION VARCHAR(16777216),
    	ISO_REGION VARCHAR(16777216),
    	COUNTRY VARCHAR(16777216),
    	ISO_COUNTRY_CODE VARCHAR(16777216),
    	FRANCHISE_FLAG NUMBER(38,0),
    	YEAR NUMBER(38,0),
    	MAKE VARCHAR(16777216),
    	MODEL VARCHAR(16777216),
    	EV_FLAG NUMBER(38,0),
    	FRANCHISE_ID NUMBER(38,0),
    	TRUCK_OPENING_DATE DATE,
        	TRUCK_EMAIL VARCHAR NOT NULL UNIQUE,
        	RECORD_START_TIME TIMESTAMP,
        	RECORD_END_TIME TIMESTAMP,
    	primary key (TRUCK_ID) 
    	)
    	AS
    	SELECT 
    	t.$1 AS TRUCK_ID, 
    	t.$2 AS MENU_TYPE_ID,
    	t.$3 AS PRIMARY_CITY,
    	t.$4 AS REGION,
    	t.$5 AS ISO_REGION,
    	t.$6 AS COUNTRY,
    	t.$7 AS ISO_COUNTRY_CODE,
    	t.$8 AS FRANCHISE_FLAG,
    	t.$9 AS YEAR,
    	t.$10 AS MAKE,
    	t.$11 AS MODEL,
    	t.$12 AS EV_FLAG,
    	t.$13 AS FRANCHISE_ID,
    	t.$14 AS TRUCK_OPENING_DATE,
    	CONCAT(TRUCK_ID, '_truck@email.com') TRUCK_EMAIL,
    	$CURRENT_TIMESTAMP AS RECORD_START_TIME,
    	NULL AS RECORD_END_TIME
    	FROM @FROSTBYTE_TASTY_BYTES_STAGE (pattern=>'.*TRUCK.csv') t;

    
    --hybrid table
    CREATE OR REPLACE HYBRID TABLE ORDER_HEADER (
    	ORDER_ID NUMBER(38,0) NOT NULL,
    	TRUCK_ID NUMBER(38,0),
    	LOCATION_ID NUMBER(19,0),
    	CUSTOMER_ID NUMBER(38,0),
    	DISCOUNT_ID FLOAT,
    	SHIFT_ID NUMBER(38,0),
    	SHIFT_START_TIME TIME(9),
    	SHIFT_END_TIME TIME(9),
    	ORDER_CHANNEL VARCHAR(16777216),
    	ORDER_TS TIMESTAMP_NTZ(9),
    	SERVED_TS VARCHAR(16777216),
    	ORDER_CURRENCY VARCHAR(3),
    	ORDER_AMOUNT NUMBER(38,4),
    	ORDER_TAX_AMOUNT VARCHAR(16777216),
    	ORDER_DISCOUNT_AMOUNT VARCHAR(16777216),
    	ORDER_TOTAL NUMBER(38,4),
    	ORDER_STATUS VARCHAR(16777216) DEFAULT 'INQUEUE',
    	primary key (ORDER_ID),
    	foreign key (TRUCK_ID) references TRUCK(TRUCK_ID) ,
    	index IDX01_ORDER_TS(ORDER_TS)
    );

    
    insert into ORDER_HEADER (
    	ORDER_ID,
    	TRUCK_ID,
    	LOCATION_ID,
    	CUSTOMER_ID,
    	DISCOUNT_ID,
    	SHIFT_ID,
    	SHIFT_START_TIME,
    	SHIFT_END_TIME,
    	ORDER_CHANNEL,
    	ORDER_TS,
    	SERVED_TS,
    	ORDER_CURRENCY,
    	ORDER_AMOUNT,
    	ORDER_TAX_AMOUNT,
    	ORDER_DISCOUNT_AMOUNT,
    	ORDER_TOTAL,
    	ORDER_STATUS)
    	SELECT
    	t.$1 AS ORDER_ID,
    	t.$2 AS TRUCK_ID,
    	t.$3 AS LOCATION_ID,
    	t.$4 AS CUSTOMER_ID,
    	t.$5 AS DISCOUNT_ID,
    	t.$6 AS SHIFT_ID,
    	t.$7 AS SHIFT_START_TIME,
    	t.$8 AS SHIFT_END_TIME,
    	t.$9 AS ORDER_CHANNEL,
    	t.$10 AS ORDER_TS,
    	t.$11 AS SERVED_TS,
    	t.$12 AS ORDER_CURRENCY,
    	t.$13 AS ORDER_AMOUNT,
    	t.$14 AS ORDER_TAX_AMOUNT,
    	t.$15 AS ORDER_DISCOUNT_AMOUNT,
    	t.$16 AS ORDER_TOTAL,
    	'' as ORDER_STATUS 
    	FROM @FROSTBYTE_TASTY_BYTES_STAGE (pattern=>'.*ORDER_HEADER.csv') t;
    








----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
--3  EXPLORE DATA

    USE ROLE HYBRID_QUICKSTART_ROLE;
    USE WAREHOUSE HYBRID_QUICKSTART_WH;
    USE SCHEMA HYBRID_QUICKSTART_DB.DATA;

    --is_hybrid penultimate column
    SHOW TABLES LIKE '%TRUCK%';
    SHOW TABLES LIKE '%ORDER_HEADER%';

    
    DESC TABLE TRUCK;
    DESC TABLE ORDER_HEADER;
    
    SHOW HYBRID TABLES;

    
    --Hybrid: is_unique
    SHOW INDEXES;

    

    select * from TRUCK;--450
    select * from TRUCK_HISTORY;--450
    select * from ORDER_HEADER;--1000







----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
--4. Prove Unique and Foreign Keys Constraints
    
    --"primary key", "unique key"
    DESC TABLE TRUCK;

    --NEW_TRUCK_ID = MAX + 1
    SET TRUCK_EMAIL = (SELECT TRUCK_EMAIL FROM TRUCK LIMIT 1);
    SET MAX_TRUCK_ID = (SELECT MAX(TRUCK_ID) FROM TRUCK);
    SET NEW_TRUCK_ID = $MAX_TRUCK_ID+1;
    
    --fail because duplicate TRUCK_EMAIL
    insert into TRUCK values ($NEW_TRUCK_ID,2,'Stockholm','Stockholm län','Stockholm','Sweden','SE',1,2001,'Freightliner','MT45 Utilimaster',0,276,'2020-10-01',$TRUCK_EMAIL,CURRENT_TIMESTAMP());
    

    
    --succeed unique email
    SET NEW_UNIQUE_EMAIL = CONCAT($NEW_TRUCK_ID, '_truck@email.com');
    insert into TRUCK values ($NEW_TRUCK_ID,2,'Stockholm','Stockholm län','Stockholm','Sweden','SE',1,2001,'Freightliner','MT45 Utilimaster',0,276,'2020-10-01',$NEW_UNIQUE_EMAIL,CURRENT_TIMESTAMP());






    --Prove Foreign Keys Constraints
    select get_ddl('table', 'ORDER_HEADER');


    SET MAX_ORDER_ID = (SELECT MAX(ORDER_ID) FROM ORDER_HEADER);
    SET NEW_ORDER_ID = ($MAX_ORDER_ID +1);
    SET NONE_EXIST_TRUCK_ID = -1;

    --fail NONE_EXIST_TRUCK_ID violate foreign key constraint
    insert into ORDER_HEADER values ($NEW_ORDER_ID,$NONE_EXIST_TRUCK_ID,6090,0,0,0,'16:00:00','23:00:00','','2022-02-18 21:38:46.000','','USD',17.0000,'','',17.0000,'');
    
    --succeed
    insert into ORDER_HEADER values ($NEW_ORDER_ID,$NEW_TRUCK_ID,6090,0,0,0,'16:00:00','23:00:00','','2022-02-18 21:38:46.000','','USD',17.0000,'','',17.0000,'');






    --fail foreign key exists
    TRUNCATE TABLE TRUCK;


    --fail Foreign Key exists
    DELETE FROM TRUCK WHERE TRUCK_ID = $NEW_TRUCK_ID;

    --success when we remove the foreign key
    DELETE FROM ORDER_HEADER WHERE ORDER_ID = $NEW_ORDER_ID;
    DELETE FROM TRUCK WHERE TRUCK_ID = $NEW_TRUCK_ID;







----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
--5. Row Level Locking

    SELECT * from ORDER_HEADER where order_status = 'COMPLETED';

    SET MAX_ORDER_ID = (SELECT max(order_id) from ORDER_HEADER);
    
    --begin transaction
    BEGIN;
    
        UPDATE ORDER_HEADER
        SET order_status = 'COMPLETED'
        WHERE order_id = $MAX_ORDER_ID;
    
        SHOW TRANSACTIONS;

        -- RUN WORKSHEET "QUICKSTART HYBRID 20"






    --on return
    COMMIT;

    
    SELECT * from ORDER_HEADER where order_status = 'COMPLETED';











----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
--6 Consistency via transaction across hybrid and standard table

    -- Step 6.1 Run Multi Statement Transaction
    
    USE ROLE HYBRID_QUICKSTART_ROLE;
    USE WAREHOUSE HYBRID_QUICKSTART_WH;
    USE SCHEMA HYBRID_QUICKSTART_DB.DATA;
    
    --begin transaction
    begin;
        SET CURRENT_TIMESTAMP = CURRENT_TIMESTAMP();

        update TRUCK
            set
                YEAR = '2024',
                RECORD_START_TIME=$CURRENT_TIMESTAMP
        where
            TRUCK_ID = 1;
        
        update TRUCK_HISTORY
            set
                RECORD_END_TIME=$CURRENT_TIMESTAMP
        where
            TRUCK_ID = 1 and 
            RECORD_END_TIME IS NULL;
            
        insert into TRUCK_HISTORY
            select *,NULL AS RECORD_END_TIME 
            from TRUCK
            where TRUCK_ID = 1;

    commit;


    --Slowly Changing Dimension (SCD) Type 2
    select * from TRUCK_HISTORY where TRUCK_ID = 1;

    --SCD Type 1 current record
    select * from TRUCK where TRUCK_ID = 1;








----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
--7. Querying joined Hybrid & Standard Tables
    

    show tables in database hybrid_quickstart_db;
    
    select "name", "is_hybrid" from table(result_scan(last_query_id()));

    
    select * from TRUCK_HISTORY;--standard table
    select * from ORDER_HEADER;--hybrid table
    
    --Join Hybrid & Standard Table without ELT tool and time
    set ORDER_ID = (select order_id from ORDER_HEADER limit 1);
    
    select HY.*,ST.* 
    from ORDER_HEADER as HY 
    inner join TRUCK_HISTORY as ST on HY.truck_id = ST.TRUCK_ID 
    where HY.ORDER_ID = $ORDER_ID and ST.RECORD_END_TIME IS NULL;
    








----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
--8. Security & Governance
    -- RBAC is same
    
    --HYBRID_QUICKSTART_BI_USER_ROLE will have limited power
    USE ROLE ACCOUNTADMIN;
    CREATE ROLE HYBRID_QUICKSTART_BI_USER_ROLE;
    SET MY_USER = CURRENT_USER();
    GRANT ROLE HYBRID_QUICKSTART_BI_USER_ROLE TO USER IDENTIFIER($MY_USER);
    
    -- Use powerful HYBRID_QUICKSTART_ROLE role to grant privileges
    USE ROLE HYBRID_QUICKSTART_ROLE;
    GRANT USAGE ON WAREHOUSE HYBRID_QUICKSTART_WH TO ROLE HYBRID_QUICKSTART_BI_USER_ROLE;
    GRANT USAGE ON DATABASE HYBRID_QUICKSTART_DB TO ROLE HYBRID_QUICKSTART_BI_USER_ROLE;
    GRANT USAGE ON ALL SCHEMAS IN DATABASE HYBRID_QUICKSTART_DB TO HYBRID_QUICKSTART_BI_USER_ROLE;

    
    -- Use weaker role
    USE ROLE HYBRID_QUICKSTART_BI_USER_ROLE;


    --fail can't see it
    select * from ORDER_HEADER limit 10;

    
    --we grant weaker BI role to see it
    USE ROLE HYBRID_QUICKSTART_ROLE;
    GRANT SELECT ON ALL TABLES IN SCHEMA DATA TO ROLE HYBRID_QUICKSTART_BI_USER_ROLE;


    --now we can see it
    USE ROLE HYBRID_QUICKSTART_BI_USER_ROLE;
    select * from ORDER_HEADER limit 10;

    
    -- Step 8.2 Hybrid Table Masking Policy
    USE ROLE HYBRID_QUICKSTART_ROLE;
    
    --full column masking version, always masks
    create masking policy hide_column_values as
    (col_value varchar) returns varchar ->
      case
         WHEN current_role() IN ('HYBRID_QUICKSTART_ROLE') THEN col_value
        else '***MASKED***'
      end;

    --apply mask to TRUCK_EMAIL
    alter table TRUCK modify column TRUCK_EMAIL
        set masking policy hide_column_values using (TRUCK_EMAIL);

    --TRUCK_EMAIL unmasked for powerful role
    select * from TRUCK limit 10;
    
    ----TRUCK_EMAIL Masked for weaker role
    USE ROLE HYBRID_QUICKSTART_BI_USER_ROLE;
    select * from TRUCK limit 10;




/*
RECAP

2 SETUP

3 Explore hybrid tables
4 Prove Unique and Foreign Key constraints
5 Demo Row level locking
6 Consistency via transaction across hybrid and standard table 
7.Querying joined Hybrid & Standard Tables
8 Different Masks of Hybrid Tables based on RBAC; Same governance as Standard Tables

9 CLEANUP

*/




----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------  ----------
--9 CLEANUP

USE ROLE HYBRID_QUICKSTART_ROLE;
USE WAREHOUSE HYBRID_QUICKSTART_WH;
USE DATABASE HYBRID_QUICKSTART_DB;
USE SCHEMA DATA;

DROP DATABASE HYBRID_QUICKSTART_DB;
DROP WAREHOUSE HYBRID_QUICKSTART_WH;
USE ROLE ACCOUNTADMIN;
DROP ROLE HYBRID_QUICKSTART_ROLE;
DROP ROLE HYBRID_QUICKSTART_BI_USER_ROLE;
