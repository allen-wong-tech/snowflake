/*
Redshift Migration to Snowflake by Allen Wong, Sales Engineer ~10 minutes
    Agenda
        With Free Tools
        With "Paid" Tools
        General Principles

    Note:
        We will send you this script and other documents - so you can focus on content

    Questions on Agenda?
*/









----------------------------------------------------------------------------------------------------------
--WITH FREE TOOLS

    /*Get DDL via v_generate_tbl_ddl
        github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_generate_tbl_ddl.sql


    //Translate SQL via sql2sf.py
        community.snowflake.com/s/scripts
        //Snowflake: Create Tables via DDL
        //Questions?









    //Redshift: Unload to S3    */
        unload ('select * from employee order by
            [DISTRIBUTE, ORGANIZE ON, FILTER]') //Snowflake auto-optimizes based in file order ie YYYYMMDD
            to 's3://mybucket/mypath/employee-'  //filename = tablename; Used by Snowflake Copy Into later
        credentials 'aws_access_key_id=XXX;aws_secret_access_key=XXX'
            delimiter '\001' null '\\N'
            escape allowoverwrite
            maxfilesize 100 mb      //Snowflake recommends 10-100MB Compressed; XSmall = 8 Parallel, Small = 16
            gzip;

       //Questions?










    //Snowflake: Auto-Generate COPY INTO for all tables | Execute */
        use role dba_citibike; use warehouse load_wh; use schema citibike.public;
        create or replace sequence my_seq start 1 increment by 1;
        with a as (
          select table_catalog database_name, table_schema schema_name, table_name
          from information_schema.tables
          where table_catalog = 'CITIBIKE' //replaceMe
          and schema_name <> 'INFORMATION_SCHEMA'
          order by 1,2,3
        )
        select
          '-- Table #' || my_seq.nextval || '\n' ||
          'copy into ' || a.database_name || '.' || schema_name || '.' || table_name || '\n' ||
          ' from @source_data/' || table_name || '/ ' || '\n' ||
          ' on_error = ''abort_statement''' || '\n' || ' purge = false' || '\n' ||
          ' --validation_mode = ''return_all_errors''' || '\n' ||
          ' --pattern = ''.*' || table_name || '.*''' || '\n' || ';' sql_string
        from a;

        //Questions?










    /*Recap - Without Tools
        v_generate_tbl_ddl
        sql2sf.py
        Redshift Unload to S3
        Snowflake Copy Into

        Questions?          */













/*-------------------------------------------------------------------------------------------------------
--WITH "PAID" TOOLS (often pays for itself)
    Partner Connect
        FiveTran, Matillion, Stitch (free forever tier)
    Other Partners
        DBT

    //Questions?













----------------------------------------------------------------------------------------------------------
--GENERAL PRINCIPLES

    Team Management
        1 or more working on it (Full-time or Half-time)
        Coordinate via a Shared Doc, Jira, or Project Plan
            Prioritize the easiest first, the hard become less hard
        Clone as-is for easier regression tests; can refactor later
            Mini-milestones: Celebrate early wins (1 process at a time)

    Public Case Studies
        Customers
            Easy migration - numerous within 1 week
            Bowery Farming - Youtube - 387 DBT Data Models
            Instacart - Medium - Very Complex but paid itself quickly

        Vendors who Migrated - likely to help you
            DBT - Medium - migrated 25K SQL lines
            Stitch extremely complex Redshift but only 2 months

    Other Internal Documentation - we will send you
        Snowflake vs. Redshift SQL Syntax Differences
            (339 functions in Snowflake vs 175, mappings, translations)

    Support
        community.snowflake.com - search for Redshift Migration
        Stack Overflow - Snowflake Data Platform
        Us for now / In addition Snowflake Support - when go live
            We are also reachable / rare escalation

    Questions?
*/












/*Recap
        With Free Tools
            v_generate_tbl_ddl
            sql2sf.py
            Redshift Unload to S3
            Snowflake Copy Into

        With "Paid" Tools
            Often pays for itself
            Vendors often helpful especially if they have migrated

        General Principles
            clone as-is
            celebrate mini milestones

        Questions?
*/
