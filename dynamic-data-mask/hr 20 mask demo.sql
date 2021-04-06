use database pii_demo;  use role sysadmin; use warehouse adhoc;

/*
What we will see

Personally Identifiable Information (PII)
Create Dynamic Data Mask
4 different roles and how they have the PII masked

*/

  

----------------------------------------------------------------------------------------------------------
--Dynamic Data Masking Demo: 

 -- Let's mask this PII
 select Department, Employee_Id, Reports_to, Salary, SSN
 from PII.EMPLOYEES order by 1,2,3;
 









 
  
  
  
--Reusable mask
    CREATE OR REPLACE MASKING POLICY PII.SALARY_MASK AS
       (VAL NUMBER) RETURNS NUMBER ->
       CASE 
            WHEN INVOKER_ROLE() IN ('RL_PRIVACY_ADMIN') THEN VAL 
       ELSE NULL
    END;

    CREATE OR REPLACE MASKING POLICY PII.SSN_MASK AS
       (VAL VARCHAR) RETURNS VARCHAR ->
       CASE
            WHEN INVOKER_ROLE() IN ('RL_PRIVACY_ADMIN') THEN VAL 
       ELSE '***-**-' || substring(VAL,8,4)
    END;

  
--Apply mask to tables  
    ALTER TABLE PII.EMPLOYEES MODIFY COLUMN SALARY SET MASKING POLICY PII.SALARY_MASK;
    ALTER TABLE PII.EMPLOYEES MODIFY COLUMN SSN SET MASKING POLICY PII.SSN_MASK;
  

  
-- PII will be masked even to the most powerful role
    use role accountadmin;
    SELECT * FROM PII.EMPLOYEES;
    
  
-- with authorized role, as-if nothing has changed
    USE ROLE RL_PRIVACY_ADMIN;
    SELECT * FROM PII.EMPLOYEES;
    
  
  
  
  
-- If they report to me, I can see their data
    USE ROLE SYSADMIN;
    CREATE OR REPLACE FUNCTION NONPII.REPORTS_TO_UDF(REPORTS_TO STRING, CURRENT_USER STRING)
    RETURNS BOOLEAN
    AS
    $$
    SELECT
    EXISTS (
    SELECT *
    FROM PII.EMPLOYEES
    WHERE CURRENT_USER()=UPPER(REPORTS_TO))
    $$
    ;
 
  
-- I am John so User-Defined Function (UDF) reports TRUE
    SELECT
      EMPLOYEE_ID,
      REPORTS_TO, 
      NONPII.REPORTS_TO_UDF(REPORTS_TO,CURRENT_USER()) 
    FROM PII.EMPLOYEES order by 2;
 
    GRANT USAGE ON FUNCTION NONPII.REPORTS_TO_UDF(STRING, STRING) TO PUBLIC;
  

USE ROLE RL_PRIVACY_ADMIN;
 
CREATE or REPLACE SECURE VIEW NONPII.V_MASK_EMP_INFO AS
SELECT EMPLOYEE_ID, REPORTS_TO, DEPARTMENT,
CASE
    WHEN (CURRENT_ROLE() = 'RL_EMPLOYEE' and CURRENT_USER()=UPPER(EMPLOYEE_ID))
    THEN SALARY
    WHEN (CURRENT_ROLE() = 'RL_MANAGER' and NONPII.REPORTS_TO_UDF(REPORTS_TO,CURRENT_USER()))
    THEN SALARY
    WHEN (CURRENT_ROLE() = 'RL_HR_REP' and DEPARTMENT<>'HR')
    THEN SALARY
    WHEN (CURRENT_ROLE() = 'RL_PRIVACY_ADMIN')
    THEN SALARY
    ELSE NULL
    END AS "SALARY",
CASE
    WHEN (CURRENT_ROLE() = 'RL_PRIVACY_ADMIN')
    THEN SSN
    ELSE '***-**-' || substring(SSN,8,4)
    END AS "SSN"
FROM PII.EMPLOYEES;
  
GRANT SELECT ON NONPII.V_MASK_EMP_INFO to PUBLIC;
  



----------------------------------------------------------------------------------------------------------
--Dynamic Data Masking

      -- 1) a regular employee is allowed to see only her own data
      USE ROLE RL_EMPLOYEE;
      SELECT * FROM NONPII.V_MASK_EMP_INFO;

      -- 2) a manager can see data related to her direct and in-direct reports
      USE ROLE RL_MANAGER;
      SELECT * FROM PII_DEMO.NONPII.V_MASK_EMP_INFO;

      -- 3) members of hr can see everybody’s data except the data about other hr people
      USE ROLE RL_HR_REP;
      SELECT * FROM PII_DEMO.NONPII.V_MASK_EMP_INFO;

      -- and the rl_privacy_admin can see all of the data
      USE ROLE RL_PRIVACY_ADMIN;
      SELECT * FROM PII_DEMO.NONPII.V_MASK_EMP_INFO;




------------- ADMIN STUFF ---------------
USE ROLE SYSADMIN;
USE SCHEMA PII;
SHOW MASKING POLICIES;
DESC MASKING POLICY SALARY_MASK;
  
-- SHOW COLUMNS WHERE THE POLICY IS APPLIED
SELECT * FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(POLICY_NAME=>'SALARY_MASK'));
  
  


/*
Recap

Personally Identifiable Information (PII)
Create Dynamic Data Mask
4 different roles and how they have the PII masked

*/

  
