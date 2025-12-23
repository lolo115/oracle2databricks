-- ============================================================================
-- File: 08_conversion_conditional_functions.sql
-- Description: Type conversion, NULL handling, and conditional functions
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. CAST Function
-- -----------------------------------------------------------------------------

-- CAST between data types
SELECT CAST('123' AS NUMBER) AS str_to_num FROM DUAL;
SELECT CAST(123.456 AS NUMBER(10,2)) AS num_precision FROM DUAL;
SELECT CAST(123 AS VARCHAR2(10)) AS num_to_str FROM DUAL;
SELECT CAST(SYSDATE AS TIMESTAMP) AS date_to_timestamp FROM DUAL;
SELECT CAST(SYSTIMESTAMP AS DATE) AS timestamp_to_date FROM DUAL;
SELECT CAST('2024-06-15' AS DATE) AS str_to_date FROM DUAL;

-- CAST with INTERVAL
SELECT CAST('100' AS INTERVAL YEAR TO MONTH) AS interval_ym FROM DUAL;
SELECT CAST(NUMTODSINTERVAL(100, 'DAY') AS INTERVAL DAY TO SECOND) AS interval_ds FROM DUAL;

-- Applied to columns
SELECT employee_id,
       CAST(salary AS VARCHAR2(20)) AS salary_str,
       CAST(hire_date AS TIMESTAMP) AS hire_timestamp
FROM employees;

-- -----------------------------------------------------------------------------
-- 2. CONVERT Function (Character Set Conversion)
-- -----------------------------------------------------------------------------

-- Convert between character sets
SELECT CONVERT('Ä Ö Ü', 'US7ASCII', 'WE8ISO8859P1') AS converted FROM DUAL;
SELECT CONVERT('Hello', 'WE8ISO8859P1', 'AL32UTF8') AS converted FROM DUAL;

-- -----------------------------------------------------------------------------
-- 3. TO_CHAR, TO_NUMBER, TO_DATE (covered in other files, quick reference)
-- -----------------------------------------------------------------------------

-- TO_CHAR from number
SELECT TO_CHAR(12345.67, 'FM$99,999.00') AS formatted FROM DUAL;
SELECT TO_CHAR(employee_id, '00000') AS emp_code FROM employees;

-- TO_CHAR from date
SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS formatted FROM DUAL;

-- TO_NUMBER
SELECT TO_NUMBER('$12,345.67', '$99,999.99') AS amount FROM DUAL;

-- TO_DATE
SELECT TO_DATE('2024-06-15', 'YYYY-MM-DD') AS parsed FROM DUAL;

-- -----------------------------------------------------------------------------
-- 4. NVL Function
-- -----------------------------------------------------------------------------

-- Basic NVL
SELECT NVL(NULL, 'Default') AS result FROM DUAL;           -- 'Default'
SELECT NVL('Value', 'Default') AS result FROM DUAL;        -- 'Value'

-- NVL with columns
SELECT employee_id, first_name,
       commission_pct,
       NVL(commission_pct, 0) AS commission_with_default
FROM employees;

-- NVL for calculations
SELECT employee_id, salary, commission_pct,
       salary + (salary * NVL(commission_pct, 0)) AS total_compensation
FROM employees;

-- NVL with different types (must be compatible)
SELECT employee_id,
       NVL(manager_id, 0) AS manager_with_default,
       NVL(TO_CHAR(manager_id), 'No Manager') AS manager_text
FROM employees;

-- -----------------------------------------------------------------------------
-- 5. NVL2 Function
-- -----------------------------------------------------------------------------

-- NVL2(expr, value_if_not_null, value_if_null)
SELECT NVL2(NULL, 'Has Value', 'Is Null') AS result FROM DUAL;     -- 'Is Null'
SELECT NVL2('X', 'Has Value', 'Is Null') AS result FROM DUAL;      -- 'Has Value'

-- NVL2 with columns
SELECT employee_id, commission_pct,
       NVL2(commission_pct, 'Earns Commission', 'No Commission') AS commission_status
FROM employees;

-- NVL2 for conditional calculations
SELECT employee_id, salary, commission_pct,
       NVL2(commission_pct, 
            salary * (1 + commission_pct), 
            salary) AS effective_salary
FROM employees;

-- -----------------------------------------------------------------------------
-- 6. COALESCE Function
-- -----------------------------------------------------------------------------

-- COALESCE returns first non-null value
SELECT COALESCE(NULL, NULL, 'Third', 'Fourth') AS result FROM DUAL;  -- 'Third'
SELECT COALESCE('First', NULL, 'Third') AS result FROM DUAL;         -- 'First'

-- COALESCE with multiple columns
SELECT employee_id,
       COALESCE(commission_pct, 0) AS commission,
       COALESCE(manager_id, department_id, 0) AS reference_id
FROM employees;

-- COALESCE vs NVL (COALESCE is ANSI standard, can take multiple args)
SELECT 
    NVL(NULL, 'NVL Default') AS nvl_result,
    COALESCE(NULL, 'COALESCE Default') AS coalesce_result
FROM DUAL;

-- -----------------------------------------------------------------------------
-- 7. NULLIF Function
-- -----------------------------------------------------------------------------

-- NULLIF returns NULL if both arguments are equal
SELECT NULLIF(100, 100) AS result FROM DUAL;               -- NULL
SELECT NULLIF(100, 200) AS result FROM DUAL;               -- 100

-- Use NULLIF to avoid division by zero
SELECT 10 / NULLIF(0, 0) AS safe_division FROM DUAL;       -- NULL instead of error

-- NULLIF with columns
SELECT employee_id, job_id,
       NULLIF(job_id, 'IT_PROG') AS non_it_job
FROM employees;

-- Replace specific value with NULL
SELECT salary,
       NULLIF(salary, 2500) AS salary_not_2500
FROM employees;

-- -----------------------------------------------------------------------------
-- 8. CASE Expression
-- -----------------------------------------------------------------------------

-- Simple CASE
SELECT employee_id, department_id,
       CASE department_id
           WHEN 10 THEN 'Administration'
           WHEN 20 THEN 'Marketing'
           WHEN 30 THEN 'Purchasing'
           WHEN 40 THEN 'Human Resources'
           WHEN 50 THEN 'Shipping'
           WHEN 60 THEN 'IT'
           WHEN 70 THEN 'Public Relations'
           WHEN 80 THEN 'Sales'
           WHEN 90 THEN 'Executive'
           WHEN 100 THEN 'Finance'
           WHEN 110 THEN 'Accounting'
           ELSE 'Other'
       END AS department_name
FROM employees;

-- Searched CASE
SELECT employee_id, salary,
       CASE
           WHEN salary < 5000 THEN 'Low'
           WHEN salary < 10000 THEN 'Medium'
           WHEN salary < 15000 THEN 'High'
           ELSE 'Very High'
       END AS salary_grade
FROM employees;

-- CASE with NULL handling
SELECT employee_id, commission_pct,
       CASE
           WHEN commission_pct IS NULL THEN 'No Commission'
           WHEN commission_pct < 0.1 THEN 'Low Commission'
           WHEN commission_pct < 0.2 THEN 'Medium Commission'
           ELSE 'High Commission'
       END AS commission_level
FROM employees;

-- CASE in ORDER BY
SELECT employee_id, first_name, department_id
FROM employees
ORDER BY CASE department_id
             WHEN 90 THEN 1
             WHEN 60 THEN 2
             ELSE 3
         END,
         salary DESC;

-- CASE in aggregate functions
SELECT 
    COUNT(CASE WHEN salary < 5000 THEN 1 END) AS low_salary_count,
    COUNT(CASE WHEN salary >= 5000 AND salary < 10000 THEN 1 END) AS medium_salary_count,
    COUNT(CASE WHEN salary >= 10000 THEN 1 END) AS high_salary_count
FROM employees;

-- CASE for conditional aggregation
SELECT department_id,
       SUM(CASE WHEN job_id LIKE '%CLERK%' THEN salary ELSE 0 END) AS clerk_salaries,
       SUM(CASE WHEN job_id LIKE '%MGR%' OR job_id LIKE '%MAN%' THEN salary ELSE 0 END) AS manager_salaries
FROM employees
GROUP BY department_id;

-- Nested CASE
SELECT employee_id, salary, commission_pct,
       CASE
           WHEN commission_pct IS NOT NULL THEN
               CASE
                   WHEN salary > 10000 THEN 'High Earner with Commission'
                   ELSE 'Commission Earner'
               END
           ELSE
               CASE
                   WHEN salary > 10000 THEN 'High Earner'
                   ELSE 'Standard'
               END
       END AS employee_category
FROM employees;

-- -----------------------------------------------------------------------------
-- 9. DECODE Function (Oracle-specific)
-- -----------------------------------------------------------------------------

-- Simple DECODE
SELECT employee_id, department_id,
       DECODE(department_id,
              10, 'Administration',
              20, 'Marketing',
              30, 'Purchasing',
              50, 'Shipping',
              60, 'IT',
              80, 'Sales',
              90, 'Executive',
              'Other') AS department_name
FROM employees;

-- DECODE with NULL
SELECT employee_id, commission_pct,
       DECODE(commission_pct, NULL, 'No Commission', 'Has Commission') AS commission_status
FROM employees;

-- DECODE for conditional logic
SELECT employee_id, salary,
       DECODE(SIGN(salary - 10000), 
              1, 'Above 10000',
              0, 'Exactly 10000',
              -1, 'Below 10000') AS salary_level
FROM employees;

-- DECODE vs CASE comparison
SELECT employee_id,
       DECODE(department_id, 10, 'Admin', 20, 'Marketing', 'Other') AS decode_result,
       CASE department_id WHEN 10 THEN 'Admin' WHEN 20 THEN 'Marketing' ELSE 'Other' END AS case_result
FROM employees;

-- Nested DECODE
SELECT employee_id,
       DECODE(department_id, 
              50, DECODE(job_id, 'SH_CLERK', 'Shipping Clerk', 'ST_CLERK', 'Stock Clerk', 'Other Shipping'),
              60, DECODE(job_id, 'IT_PROG', 'Programmer', 'Other IT'),
              'Other Department') AS job_category
FROM employees;

-- -----------------------------------------------------------------------------
-- 10. LNNVL Function
-- -----------------------------------------------------------------------------

-- LNNVL returns TRUE if condition is FALSE or UNKNOWN (NULL)
-- Useful in WHERE clause with NULL-aware logic
SELECT * FROM employees
WHERE LNNVL(commission_pct > 0);  -- Returns rows where commission_pct <= 0 OR IS NULL

-- Compare with regular condition
SELECT employee_id, commission_pct FROM employees WHERE commission_pct > 0;        -- Only non-NULL > 0
SELECT employee_id, commission_pct FROM employees WHERE NOT (commission_pct > 0);  -- Only non-NULL <= 0
SELECT employee_id, commission_pct FROM employees WHERE LNNVL(commission_pct > 0); -- <= 0 OR NULL

-- -----------------------------------------------------------------------------
-- 11. DUMP Function
-- -----------------------------------------------------------------------------

-- DUMP shows internal representation
SELECT DUMP('ABC') AS char_dump FROM DUAL;
SELECT DUMP(123) AS num_dump FROM DUAL;
SELECT DUMP(SYSDATE) AS date_dump FROM DUAL;

-- DUMP with format
SELECT DUMP('ABC', 10) AS decimal_dump FROM DUAL;          -- Decimal
SELECT DUMP('ABC', 16) AS hex_dump FROM DUAL;              -- Hexadecimal
SELECT DUMP('ABC', 8) AS octal_dump FROM DUAL;             -- Octal
SELECT DUMP('ABC', 17) AS char_dump FROM DUAL;             -- Character

-- Detect data issues
SELECT employee_id, first_name, DUMP(first_name) AS name_dump
FROM employees
WHERE ROWNUM <= 5;

-- -----------------------------------------------------------------------------
-- 12. SYS_CONTEXT Function
-- -----------------------------------------------------------------------------

-- Get session context information
SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') AS session_user FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER') AS current_user FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS db_name FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'HOST') AS host FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'IP_ADDRESS') AS ip_address FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'OS_USER') AS os_user FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'MODULE') AS module FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') AS client_id FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'NLS_TERRITORY') AS territory FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'NLS_LANGUAGE') AS language FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'INSTANCE') AS instance FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'SID') AS sid FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS schema FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'SESSION_EDITION_NAME') AS edition FROM DUAL;

-- All useful USERENV attributes
SELECT 
    SYS_CONTEXT('USERENV', 'SESSION_USER') AS session_user,
    SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS current_schema,
    SYS_CONTEXT('USERENV', 'DB_NAME') AS database_name,
    SYS_CONTEXT('USERENV', 'INSTANCE_NAME') AS instance_name,
    SYS_CONTEXT('USERENV', 'HOST') AS client_host,
    SYS_CONTEXT('USERENV', 'IP_ADDRESS') AS client_ip,
    SYS_CONTEXT('USERENV', 'OS_USER') AS os_user
FROM DUAL;

-- -----------------------------------------------------------------------------
-- 13. USER, UID, and Session Functions
-- -----------------------------------------------------------------------------

-- Current user
SELECT USER FROM DUAL;
SELECT UID FROM DUAL;

-- USERENV function (deprecated, use SYS_CONTEXT)
SELECT USERENV('SESSIONID') AS session_id FROM DUAL;
SELECT USERENV('LANGUAGE') AS language FROM DUAL;
SELECT USERENV('TERMINAL') AS terminal FROM DUAL;

-- -----------------------------------------------------------------------------
-- 14. Type Functions
-- -----------------------------------------------------------------------------

-- TREAT (for object types)
-- SELECT TREAT(value AS subtype) FROM table;

-- SYS_TYPEID (for object types)
-- SELECT SYS_TYPEID(object_column) FROM table;

-- -----------------------------------------------------------------------------
-- 15. Complex Conditional Logic
-- -----------------------------------------------------------------------------

-- Multiple conditions with CASE
SELECT employee_id, salary, department_id, job_id,
       CASE
           WHEN department_id = 90 AND job_id LIKE '%PRES%' THEN 'Executive - President'
           WHEN department_id = 90 AND job_id LIKE '%VP%' THEN 'Executive - VP'
           WHEN department_id = 80 AND salary > 10000 THEN 'Senior Sales'
           WHEN department_id = 80 THEN 'Sales'
           WHEN department_id = 60 THEN 'IT'
           WHEN salary > 10000 THEN 'Senior Staff'
           ELSE 'Staff'
       END AS employee_category
FROM employees;

-- CASE with subqueries
SELECT employee_id, salary, department_id,
       CASE
           WHEN salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.department_id = employees.department_id) 
           THEN 'Above Department Average'
           ELSE 'Below Department Average'
       END AS salary_comparison
FROM employees;

-- Conditional joins using CASE
SELECT e.employee_id, e.first_name, e.department_id,
       CASE e.department_id
           WHEN 80 THEN (SELECT region_name FROM regions WHERE region_id = 2)
           WHEN 50 THEN (SELECT region_name FROM regions WHERE region_id = 1)
           ELSE 'Unknown'
       END AS region
FROM employees e;

-- Pivot-like with CASE
SELECT department_id,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 1 THEN 1 ELSE 0 END) AS jan_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 2 THEN 1 ELSE 0 END) AS feb_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 3 THEN 1 ELSE 0 END) AS mar_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 4 THEN 1 ELSE 0 END) AS apr_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 5 THEN 1 ELSE 0 END) AS may_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 6 THEN 1 ELSE 0 END) AS jun_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 7 THEN 1 ELSE 0 END) AS jul_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 8 THEN 1 ELSE 0 END) AS aug_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 9 THEN 1 ELSE 0 END) AS sep_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 10 THEN 1 ELSE 0 END) AS oct_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 11 THEN 1 ELSE 0 END) AS nov_hires,
       SUM(CASE WHEN EXTRACT(MONTH FROM hire_date) = 12 THEN 1 ELSE 0 END) AS dec_hires
FROM employees
GROUP BY department_id;

