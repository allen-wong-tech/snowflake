/*
PREREQUISITE
Private Sharing | Mount SAMPLE_DATA share as SNOWFLAKE_SAMPLE_DATA and grant access to PUBLIC 

https://other-docs.snowflake.com/en/collaboration/provider-listings-creating-publishing#create-a-free-private-listing
*/
--------------------------------------------------------
--Ask the Snowflake subscriber to run this command:
    select current_organization_name() || '-' || current_account_name();
    
--------------------------------------------------------
--Replace the share_target with what customer tells you from aforementioned SQL
    set share_target = 'DNOOATJ-OX69249';

--------------------------------------------------------
--user context
    use role accountadmin;
    create database share_db;

--------------------------------------------------------
--populate table
    --13 sec on xsmall
    create or replace table store_sales2 as
    select *
    from snowflake_sample_data.tpcds_sf10tcl.store_sales
    where ss_sold_date_sk = 2452570
    order by ss_customer_sk, ss_item_sk;
    
    select top 300 * from store_sales2 order by 4,3;
    

--------------------------------------------------------
--mapping table
    create or replace table share_map as
    select distinct
        ss_customer_sk,
        ''::varchar snow_account
    from store_sales2
    order by 1;

    set min_ss_customer_sk = (select min(ss_customer_sk) from share_map);

    --target_share    
    update share_map
    set snow_account = $share_target
    where ss_customer_sk = $min_ss_customer_sk;


    --test own account
    set share_target_self = (select current_organization_name() || '-' || current_account_name());

    set max_ss_customer_sk = (select max(ss_customer_sk) from share_map);

    --target_share    
    update share_map
    set snow_account = $share_target_self
    where ss_customer_sk = $max_ss_customer_sk;

    --where are we sharing to
    select * from share_map order by 1;

--------------------------------------------------------
--shared view
    create or replace secure view store_sales_for_customer
    as
    select ss.*
    from store_sales2 ss
    inner join share_map sm on ss.ss_customer_sk = sm.ss_customer_sk
    where sm.snow_account = current_organization_name() || '-' || current_account_name();

    --test the next view against our own account
    select * from share_map order by 1 desc;

    --we should only see data for which we are mapped to in share_map
    select * from store_sales_for_customer;

--------------------------------------------------------
--Share the listing
