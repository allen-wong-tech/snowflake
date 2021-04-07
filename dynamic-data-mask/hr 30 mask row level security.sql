use database pii_demo;  use warehouse adhoc;

/*
bonus => lets use masking to provide row level security

In the masks, with:
    invoker_role() the owner of the view that calls the policy will be used 
    current_role() it's who is connected right now, regardless of the view owner

We are essentially using the view v_mask_emp_info as a security mapping table

*/

USE ROLE RL_PRIVACY_ADMIN;

--create view with suffix of RLS: Row-Level Security
CREATE or REPLACE SECURE VIEW NONPII.V_MASK_EMP_INFO_RLS AS
SELECT EMPLOYEE_ID, REPORTS_TO, DEPARTMENT, SALARY
FROM NONPII.V_MASK_EMP_INFO
where salary is not null;   //filters out the nulls

GRANT SELECT ON PII_DEMO.NONPII.V_MASK_EMP_INFO_RLS to PUBLIC;
  








--Row-level security: Only see the data that they can see unmasked
USE ROLE RL_EMPLOYEE;
SELECT * FROM PII_DEMO.NONPII.V_MASK_EMP_INFO_RLS;
  
USE ROLE RL_MANAGER;
SELECT * FROM PII_DEMO.NONPII.V_MASK_EMP_INFO_RLS;
  
USE ROLE RL_HR_REP;
SELECT * FROM PII_DEMO.NONPII.V_MASK_EMP_INFO_RLS;
  
USE ROLE RL_PRIVACY_ADMIN;
SELECT * FROM PII_DEMO.NONPII.V_MASK_EMP_INFO_RLS;
  
  
  
/*
Recap

We create a second view - on top of the view where the mask is applied - 
That view only shows where the salary is not null so it gives us row-level security.

Use Cases:
    In Financial Services, Portfolio Managers, can only see their data
    In Healthcare, Doctors can only see their patients

*/
  
  
