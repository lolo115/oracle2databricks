-- ============================================================================
-- File: 01_simple_queries.sql
-- Description: Basic SQL SELECT statements and fundamental query patterns
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Basic SELECT statements
-- -----------------------------------------------------------------------------

-- Select all columns from a table
SELECT * FROM employees;

-- Select specific columns
SELECT employee_id, first_name, last_name, salary FROM employees;

-- Select with column aliases
SELECT 
    employee_id AS emp_id,
    first_name || ' ' || last_name AS full_name,
    salary AS monthly_salary,
    salary * 12 AS annual_salary
FROM employees;

-- Select with table alias
SELECT e.employee_id, e.first_name, e.department_id
FROM employees e;

-- Select distinct values
SELECT DISTINCT department_id FROM employees;

SELECT DISTINCT job_id, department_id FROM employees;

-- -----------------------------------------------------------------------------
-- 2. WHERE clause - Basic filtering
-- -----------------------------------------------------------------------------

-- Equality condition
SELECT * FROM employees WHERE department_id = 10;

-- Inequality conditions
SELECT * FROM employees WHERE salary > 5000;
SELECT * FROM employees WHERE salary >= 5000;
SELECT * FROM employees WHERE salary < 10000;
SELECT * FROM employees WHERE salary <= 10000;
SELECT * FROM employees WHERE salary <> 5000;
SELECT * FROM employees WHERE salary != 5000;

-- BETWEEN condition
SELECT * FROM employees WHERE salary BETWEEN 5000 AND 10000;

-- NOT BETWEEN
SELECT * FROM employees WHERE salary NOT BETWEEN 5000 AND 10000;

-- IN condition
SELECT * FROM employees WHERE department_id IN (10, 20, 30);

-- NOT IN condition
SELECT * FROM employees WHERE department_id NOT IN (10, 20, 30);

-- LIKE pattern matching
SELECT * FROM employees WHERE last_name LIKE 'S%';
SELECT * FROM employees WHERE last_name LIKE '%son';
SELECT * FROM employees WHERE last_name LIKE '%an%';
SELECT * FROM employees WHERE last_name LIKE 'S___';
SELECT * FROM employees WHERE email LIKE '%@%' ESCAPE '@';

-- IS NULL / IS NOT NULL
SELECT * FROM employees WHERE commission_pct IS NULL;
SELECT * FROM employees WHERE commission_pct IS NOT NULL;
SELECT * FROM employees WHERE manager_id IS NULL;

-- -----------------------------------------------------------------------------
-- 3. Compound WHERE conditions
-- -----------------------------------------------------------------------------

-- AND operator
SELECT * FROM employees 
WHERE department_id = 50 AND salary > 5000;

-- OR operator
SELECT * FROM employees 
WHERE department_id = 50 OR department_id = 80;

-- Combined AND/OR with parentheses
SELECT * FROM employees 
WHERE (department_id = 50 OR department_id = 80) 
AND salary > 5000;

-- NOT operator
SELECT * FROM employees 
WHERE NOT (department_id = 50 OR department_id = 80);

-- Complex conditions
SELECT * FROM employees 
WHERE department_id IN (10, 20, 30)
AND salary BETWEEN 5000 AND 15000
AND commission_pct IS NOT NULL
AND last_name LIKE 'K%';

-- -----------------------------------------------------------------------------
-- 4. ORDER BY clause
-- -----------------------------------------------------------------------------

-- Simple ordering (ascending - default)
SELECT * FROM employees ORDER BY last_name;

-- Explicit ascending
SELECT * FROM employees ORDER BY last_name ASC;

-- Descending order
SELECT * FROM employees ORDER BY salary DESC;

-- Multiple column ordering
SELECT * FROM employees ORDER BY department_id ASC, salary DESC;

-- Order by column position
SELECT employee_id, first_name, salary FROM employees ORDER BY 3 DESC;

-- Order by alias
SELECT employee_id, first_name, salary * 12 AS annual_salary 
FROM employees 
ORDER BY annual_salary DESC;

-- NULLS FIRST / NULLS LAST
SELECT * FROM employees ORDER BY commission_pct NULLS FIRST;
SELECT * FROM employees ORDER BY commission_pct DESC NULLS LAST;

-- -----------------------------------------------------------------------------
-- 5. FETCH / LIMIT rows (Oracle 12c+)
-- -----------------------------------------------------------------------------

-- Fetch first N rows
SELECT * FROM employees ORDER BY salary DESC FETCH FIRST 10 ROWS ONLY;

-- Fetch with ties
SELECT * FROM employees ORDER BY salary DESC FETCH FIRST 10 ROWS WITH TIES;

-- Offset and fetch
SELECT * FROM employees ORDER BY employee_id OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY;

-- Fetch percentage
SELECT * FROM employees ORDER BY salary DESC FETCH FIRST 10 PERCENT ROWS ONLY;

-- Traditional ROWNUM approach (pre-12c)
SELECT * FROM (
    SELECT * FROM employees ORDER BY salary DESC
) WHERE ROWNUM <= 10;

-- -----------------------------------------------------------------------------
-- 6. Dual table and expressions
-- -----------------------------------------------------------------------------

-- Select from DUAL
SELECT SYSDATE FROM DUAL;
SELECT USER FROM DUAL;
SELECT 1 + 1 FROM DUAL;
SELECT 'Hello World' FROM DUAL;

-- Arithmetic expressions
SELECT 100 + 50 AS addition,
       100 - 50 AS subtraction,
       100 * 2 AS multiplication,
       100 / 4 AS division,
       MOD(100, 3) AS modulo
FROM DUAL;

-- String concatenation
SELECT 'Hello' || ' ' || 'World' AS greeting FROM DUAL;

-- -----------------------------------------------------------------------------
-- 7. Comments in SQL
-- -----------------------------------------------------------------------------

-- This is a single-line comment
SELECT /* This is an inline comment */ employee_id FROM employees;

/*
   This is a 
   multi-line comment
*/
SELECT employee_id, 
       first_name,  -- Employee's first name
       last_name    -- Employee's last name
FROM employees;

-- -----------------------------------------------------------------------------
-- 8. Case sensitivity and quoting
-- -----------------------------------------------------------------------------

-- Standard identifiers (case-insensitive)
SELECT EMPLOYEE_ID, employee_id, Employee_Id FROM EMPLOYEES;

-- Quoted identifiers (case-sensitive)
SELECT "Employee_Id", "EMPLOYEE_ID" FROM "EMPLOYEES";

-- Mixed case column aliases
SELECT employee_id AS "Employee ID",
       salary AS "Annual Salary"
FROM employees;

-- -----------------------------------------------------------------------------
-- 9. Schema-qualified objects
-- -----------------------------------------------------------------------------

-- Fully qualified table name
SELECT * FROM hr.employees;
SELECT * FROM hr.departments;

-- With database link (example syntax)
-- SELECT * FROM employees@remote_db;

-- -----------------------------------------------------------------------------
-- 10. Set operators basics
-- -----------------------------------------------------------------------------

-- UNION (removes duplicates)
SELECT department_id FROM employees
UNION
SELECT department_id FROM departments;

-- UNION ALL (keeps duplicates)
SELECT department_id FROM employees
UNION ALL
SELECT department_id FROM departments;

-- INTERSECT
SELECT department_id FROM employees
INTERSECT
SELECT department_id FROM departments;

-- MINUS
SELECT department_id FROM departments
MINUS
SELECT department_id FROM employees;

-- Combined set operations
SELECT employee_id, job_id FROM employees WHERE department_id = 10
UNION
SELECT employee_id, job_id FROM employees WHERE department_id = 20
MINUS
SELECT employee_id, job_id FROM job_history;

