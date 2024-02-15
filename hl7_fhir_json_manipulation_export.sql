/*
Change Log
Feb 15 2024     AWong   Changed to @play_stage internal stage and added create DB and warehouse

References
    https://www.hl7.org/fhir/patient-examples.html
    https://www.hl7.org/fhir/patient-example-c.html
    https://docs.snowflake.com/en/sql-reference/functions-semistructured.html

What we will see:
    Using Snowflake for Data Engineering.  
    Create two Fast Healthcare Interoperability Resources (FHIR) record for deceased patient in Variant DataType
    Create two structured data records from Snowflake Shared Data
    Join those two records based on their IDs
    Use Semi-Structured functions like object_construct and array_agg to manipulate the FHIR / JSON data
    Verify records exported as JSON

Benefits
    Snowflake for your Data Lake and Data Engineering workloads
    High-performance, governed ETL / ELT with only SQL and no separate tools or data silos
    Lower TCO with fewer tools, fewer data silos, and less operational risk

*/
--------------------------------------------------------
--setup
    use role sysadmin;

    --warehouse, database, schema
    create warehouse if not exists compute_wh with warehouse_size = 'xsmall' auto_suspend = 60 initially_suspended = true;
    
    create database if not exists play_db;
    create schema if not exists play_db.public;

--set context
  use role sysadmin;  use warehouse compute_wh;  use schema play_db.public;





//create FHIR / JSON patient record for Sherry Small
    create or replace transient table patient
    (
      deceased variant
    )
    as
    select parse_json(column1) as src
    from values
($$
{
  "resourceType": "Patient",
  "id": "63081838",
  "text": {
    "status": "generated",
    "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">\n\t\t\t<p>Patient Sherry Small @ Acme Healthcare, Inc. MR = 123458, DECEASED</p>\n\t\t</div>"
  },
  "identifier": [
    {
      "use": "usual",
      "type": {
        "coding": [
          {
            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
            "code": "MR"
          }
        ]
      },
      "system": "urn:oid:0.1.2.3.4.5.6.7",
      "value": "123458"
    }
  ],
  "active": true,
  "name": [
    {
      "use": "official",
      "family": "Small",
      "given": [
        "Sherry"
      ]
    }
  ],
  "gender": "female",
  "birthDate": "1928-12-28",
  "deceasedBoolean": true,
  "managingOrganization": {
    "reference": "Organization/1",
    "display": "ACME Healthcare, Inc"
  }
}
 
$$);








-----------------------------------------------------
--add patient record for Bobby Harmon
insert into patient
    select parse_json(
$$
{
  "resourceType": "Patient",
  "id": "51991432",
  "text": {
    "status": "generated",
    "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">\n\t\t\t<p>Patient Bobby Harmon @ Acme Healthcare, Inc. MR = 123458, DECEASED</p>\n\t\t</div>"
  },
  "identifier": [
    {
      "use": "usual",
      "type": {
        "coding": [
          {
            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
            "code": "MR"
          }
        ]
      },
      "system": "urn:oid:0.1.2.3.4.5.6.7",
      "value": "123458"
    }
  ],
  "active": true,
  "name": [
    {
      "use": "official",
      "family": "Harmon",
      "given": [
        "Bobby"
      ]
    }
  ],
  "gender": "male",
  "birthDate": "1982-08-02",
  "deceasedBoolean": true,
  "managingOrganization": {
    "reference": "Organization/1",
    "display": "ACME Healthcare, Inc"
  }
} 
$$);










//Variant stores raw JSON / FHIR
select top 300 * 
from patient;







-----------------------------------------------------
--create relational table from Snowflake Share with matching ID
create or replace temp table customer as                       
select
    c.c_customer_sk, c_first_name, c_last_name, c_birth_country, c_email_address, ca_street_number, ca_street_name, 
    ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country
from snowflake_sample_data.tpcds_sf10tcl.customer c
inner join snowflake_sample_data.tpcds_sf10tcl.customer_address ca on c.c_current_addr_sk = ca.ca_address_sk
where c_customer_sk in (63081838    --Sherry Small
                       ,51991432);   --Bobby Harmon                       
                       







--FHIR / JSON joined with relational on customer key
select *
from patient p
inner join customer c on p.deceased:id::integer = c.c_customer_sk;






--combine into new FHIR / JSON as-is
--Snowflake for your Data Engineering and Data Lake
--notice all columns without nesting
select object_construct(*)
from patient p
inner join
(
  select object_construct(*) customer
  from customer
) c on p.deceased:id::integer = c.customer:C_CUSTOMER_SK;








--Advanced Data Engineering: let's nest the address and include fewer columns
select object_construct(
'c_address', array_agg(
    object_construct
    (
        'ca_street_number', ca_street_number,
        'ca_street_name', ca_street_name,
        'ca_street_type', ca_street_type,
        'ca_suite_number', ca_suite_number,
        'ca_city', ca_city,
        'ca_county', ca_county,
        'ca_state', ca_state,
        'ca_zip', ca_zip,
        'ca_country', ca_country
    )
),
'c_birth_country', C_BIRTH_COUNTRY,
'c_customer_sk', C_CUSTOMER_SK,
'c_email_address', C_EMAIL_ADDRESS
) customer
from customer
group by C_BIRTH_COUNTRY, C_CUSTOMER_SK, C_EMAIL_ADDRESS
;









-----------------------------------------------------
--combine it all

create or replace transient table patient_export (v variant);





insert into patient_export
select object_construct(*)
from patient p
inner join
(
      select object_construct(
      'c_address', array_agg(
          object_construct
          (
              'ca_street_number', ca_street_number,
              'ca_street_name', ca_street_name,
              'ca_street_type', ca_street_type,
              'ca_suite_number', ca_suite_number,
              'ca_city', ca_city,
              'ca_county', ca_county,
              'ca_state', ca_state,
              'ca_zip', ca_zip,
              'ca_country', ca_country
          )
      ),
      'c_birth_country', C_BIRTH_COUNTRY,
      'c_customer_sk', C_CUSTOMER_SK,
      'c_email_address', C_EMAIL_ADDRESS
      ) customer
      from customer
      group by C_BIRTH_COUNTRY, C_CUSTOMER_SK, C_EMAIL_ADDRESS
) c
on p.deceased:id::integer = c.CUSTOMER:c_customer_sk;





select * from patient_export;









-----------------------------------------------------
--export as JSON file to data lake

create stage if not exists play_stage;

    --see what is in our cloud storage
    ls @play_stage;
    
    --optional: remove file(s)
    rm @play_stage;
    
    
    
    

    --export to cloud storage
    copy into @play_db.public.play_stage from 
        (select * from patient_export)
        file_format=(type=json)
        overwrite = true; 





    --verify files here or in cloud storage
    ls @play_db.public.play_stage;
    
    
    select $1, $2, $3, $4, $5, $6, $7, $8 from @play_db.public.play_stage;
    







/*
What we saw:
    Using Snowflake for Data Engineering.  
    Create two Fast Healthcare Interoperability Resources (FHIR) record for deceased patient in Variant DataType
    Create two structured data records from Snowflake Shared Data
    Join those two records based on their IDs
    Use Semi-Structured functions like object_construct and array_agg to manipulate the FHIR / JSON data
    Verify records exported as JSON

Benefits
    Snowflake for your Data Lake and Data Engineering workloads
    High-performance, governed ETL / ELT with only SQL and no separate tools or data silos
    Lower TCO with fewer tools, fewer data silos, and less operational risk


*/













--reset
-- rm  @play_db.public.play_stage;
