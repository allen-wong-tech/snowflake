/*
querying semi-structured

https://docs.snowflake.com/en/user-guide/querying-semistructured.html

Based on the above Snowflake documentation, we expand a bit on it including using get_path to

Snowflake's Variant DataType allows us to treat semi-structured data such as JSON as a first-class citizen
get_path allows us to access nested arrays without lateral_flatten

*/

use role sysadmin; use warehouse play_wh; use schema playdb.public;


----------------------------------------------------------------------------------------------------------
--populate table with variant       --notice parse_json to convert string to json

    create or replace table car_sales
    (
      src variant
    )
    as
    select parse_json(column1) as src
    from values
    ('{
        "date" : "2017-04-28",
        "dealership" : "Valley View Auto Sales",
        "salesperson" : {
          "id": "55",
          "name": "Frank Beasley"
        },
        "customer" : [
          {"name": "Joyce Ridgely", "phone": "16504378889", "address": "San Francisco, CA"}
        ],
        "vehicle" : [
          {"make": "Honda", "model": "Civic", "year": "2017", "price": "20275", "extras":["ext warranty", "paint protection"]}
        ]
    }'),
    ('{
        "date" : "2017-04-28",
        "dealership" : "Tindel Toyota",
        "salesperson" : {
          "id": "274",
          "name": "Greg Northrup"
        },
        "customer" : [
          {"name": "Bradley Greenbloom", "phone": "12127593751", "address": "New York, NY"}
        ],
        "vehicle" : [
          {"make": "Toyota", "model": "Camry", "year": "2017", "price": "23500", "extras":["ext warranty", "rust proofing", "fabric protection"]}
        ]
    }') v;


    
-----------------------------------------------------
--notice nesting
    select * from car_sales;
    
    --
    select src:dealership from car_sales;
    
-----------------------------------------------------
--dot notation to go deeper into nesting
    select src:salesperson.name from car_sales;

    --[] means array so use [0] zero-bound array to dive -in
    select src:vehicle from car_sales;
        
        --notice brackets [] in output disappear
        select src:vehicle[0] from car_sales;

    --and so you can query the elements
    select src:vehicle[0].price from car_sales;
    
    --cast with :: to get rid of double-quotes
    select
        src:salesperson.name::string saleperson_name, 
        src:vehicle[0].price::number 
    from car_sales;
    
-----------------------------------------------------
--flatten

--Parse an array using LATERAL FLATTEN
--returns a row for each object, and the LATERAL modifier joins the data with any information outside of the object.
    select
      value:name::string as CustomerName,
      value:address::string as Address
      from
        car_sales
      , lateral flatten(input => src:customer);

  
  
//Add a second FLATTEN clause to flatten the extras array within the flattened vehicle array and retrieve the “extras” purchased for each car sold:
    select
      vm.value:make::string as make,
      vm.value:model::string as model,
      ve.value::string as "Extras Purchased"
      from
        car_sales
      , lateral flatten(input => src:vehicle) vm
      , lateral flatten(input => vm.value:extras) ve;




-----------------------------------------------------
--get_path: if you want to pivot out without additional lateral flatten of 'extras'
    select
      vm.value:make::string as make,
      get_path(vm.value,'extras[0]')::string Extra0,
      get_path(vm.value,'extras[1]')::string Extra1,
      get_path(vm.value,'extras[2]')::string Extra2,
      get_path(vm.value,'extras[3]')::string Extra3     //can future-proof
      from
        car_sales
      , lateral flatten(input => src:vehicle) vm;
      
      
-----------------------------------------------------
--lateral flatten without using get_path
    select
    c.value:name::string as CustomerName
    ,c.value:address::string as Address
    ,c.value:phone::number as Phone
    
    ,src:date::datetime date
    ,src:dealership::string dealership
    
//                ,src:salesperson        //notice {} which means JSON object so dot-notation
    ,src:salesperson.id::number salesperson_id
    ,src:salesperson.name::string salesperson_name

    ,vm.value:make::string as make
    ,vm.value:model::string as model
    ,ve.value::string as "Extras Purchased"
    ,src:customer[0].address::string address
    ,src:customer[0].name::string name
    ,src:customer[0].phone::number phone
    ,src:date::datetime date
    ,src:dealership::string dealership
//                ,src:vehicle            //notice [] which means array so array notation
    ,get_path(src:vehicle[0],'extras[0]')::string Extra0
    ,get_path(src:vehicle[0],'extras[1]')::string Extra1
    ,get_path(src:vehicle[0],'extras[2]')::string Extra2
    ,get_path(src:vehicle[0],'extras[3]')::string Extra3    //future-proof variable length when pivoted out
    ,get_path(src:vehicle[0],'make')::string make
    ,get_path(src:vehicle[0],'model')::string model
    ,get_path(src:vehicle[0],'price')::string price
    ,get_path(src:vehicle[0],'year')::string year
    from car_sales
    , lateral flatten(input => src:customer) c
    , lateral flatten(input => src:vehicle) vm
    , lateral flatten(input => vm.value:extras) ve;



-----------------------------------------------------
--get_path to completely avoid lateral flatten
    select
    src:customer[0].address::string address
    ,src:customer[0].name::string name
    ,src:customer[0].phone::number phone
    ,src:date::datetime date
    ,src:dealership::string dealership
//                ,src:salesperson        //notice {} which means JSON object so dot-notation
    ,src:salesperson.id::number salesperson_id
    ,src:salesperson.name::string salesperson_name
//                ,src:vehicle            //notice [] which means array so array notation
    ,get_path(src:vehicle[0],'extras[0]')::string Extra0
    ,get_path(src:vehicle[0],'extras[1]')::string Extra1
    ,get_path(src:vehicle[0],'extras[2]')::string Extra2
    ,get_path(src:vehicle[0],'extras[3]')::string Extra3    //future-proof variable length when pivoted out
    ,get_path(src:vehicle[0],'make')::string make
    ,get_path(src:vehicle[0],'model')::string model
    ,get_path(src:vehicle[0],'price')::string price
    ,get_path(src:vehicle[0],'year')::string year
    from car_sales;
    
/*

Snowflake's Variant DataType allows us to treat semi-structured data such as JSON as a first-class citizen
get_path allows us to access nested arrays without lateral_flatten

*/
