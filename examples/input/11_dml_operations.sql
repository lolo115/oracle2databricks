-- ============================================================================
-- File: 11_dml_operations.sql
-- Description: INSERT, UPDATE, DELETE, MERGE statements
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Basic INSERT Statements
-- -----------------------------------------------------------------------------

-- Insert single row with all columns
INSERT INTO employees (employee_id, first_name, last_name, email, phone_number,
                       hire_date, job_id, salary, commission_pct, manager_id, department_id)
VALUES (300, 'John', 'Doe', 'JDOE', '515.123.4567',
        SYSDATE, 'IT_PROG', 6000, NULL, 103, 60);

-- Insert with column list (not all columns)
INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary)
VALUES (301, 'Jane', 'Smith', 'JSMITH', DATE '2024-06-15', 'SA_REP', 7500);

-- Insert with sequence
INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary)
VALUES (employees_seq.NEXTVAL, 'Bob', 'Johnson', 'BJOHNSON', SYSDATE, 'ST_CLERK', 3500);

-- Insert with DEFAULT values
INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary, department_id)
VALUES (302, 'Alice', 'Brown', 'ABROWN', DEFAULT, 'HR_REP', 5000, DEFAULT);

-- Insert with NULL explicitly
INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary, commission_pct)
VALUES (303, 'Charlie', 'Wilson', 'CWILSON', SYSDATE, 'SA_REP', 8000, NULL);

-- -----------------------------------------------------------------------------
-- 2. INSERT with Subquery
-- -----------------------------------------------------------------------------

-- Insert from SELECT
INSERT INTO emp_backup (employee_id, first_name, last_name, salary, department_id)
SELECT employee_id, first_name, last_name, salary, department_id
FROM employees
WHERE department_id = 60;

-- Insert with transformation
INSERT INTO salary_history (employee_id, old_salary, change_date, change_type)
SELECT employee_id, salary, SYSDATE, 'INITIAL'
FROM employees
WHERE hire_date = TRUNC(SYSDATE);

-- Insert with aggregation
INSERT INTO department_stats (department_id, emp_count, total_salary, avg_salary, stat_date)
SELECT department_id, 
       COUNT(*), 
       SUM(salary), 
       AVG(salary),
       SYSDATE
FROM employees
GROUP BY department_id;

-- Insert with joins
INSERT INTO emp_dept_snapshot (employee_id, employee_name, department_name, salary, snapshot_date)
SELECT e.employee_id, 
       e.first_name || ' ' || e.last_name,
       d.department_name,
       e.salary,
       SYSDATE
FROM employees e
JOIN departments d ON e.department_id = d.department_id;

-- -----------------------------------------------------------------------------
-- 3. Multi-table INSERT
-- -----------------------------------------------------------------------------

-- Unconditional INSERT ALL
INSERT ALL
    INTO emp_names (employee_id, full_name) VALUES (employee_id, first_name || ' ' || last_name)
    INTO emp_salaries (employee_id, salary, commission) VALUES (employee_id, salary, commission_pct)
    INTO emp_jobs (employee_id, job_id, hire_date) VALUES (employee_id, job_id, hire_date)
SELECT employee_id, first_name, last_name, salary, commission_pct, job_id, hire_date
FROM employees;

-- Conditional INSERT ALL (all matching)
INSERT ALL
    WHEN salary < 5000 THEN
        INTO low_salary_emp (employee_id, salary) VALUES (employee_id, salary)
    WHEN salary >= 5000 AND salary < 10000 THEN
        INTO mid_salary_emp (employee_id, salary) VALUES (employee_id, salary)
    WHEN salary >= 10000 THEN
        INTO high_salary_emp (employee_id, salary) VALUES (employee_id, salary)
SELECT employee_id, salary FROM employees;

-- Conditional INSERT FIRST (first matching only)
INSERT FIRST
    WHEN commission_pct IS NOT NULL THEN
        INTO commission_employees (employee_id, commission_pct)
        VALUES (employee_id, commission_pct)
    WHEN salary > 10000 THEN
        INTO high_earners (employee_id, salary)
        VALUES (employee_id, salary)
    ELSE
        INTO other_employees (employee_id, salary)
        VALUES (employee_id, salary)
SELECT employee_id, salary, commission_pct FROM employees;

-- Pivoting INSERT
INSERT ALL
    INTO quarterly_sales (product_id, quarter, amount) VALUES (product_id, 'Q1', q1_sales)
    INTO quarterly_sales (product_id, quarter, amount) VALUES (product_id, 'Q2', q2_sales)
    INTO quarterly_sales (product_id, quarter, amount) VALUES (product_id, 'Q3', q3_sales)
    INTO quarterly_sales (product_id, quarter, amount) VALUES (product_id, 'Q4', q4_sales)
SELECT product_id, q1_sales, q2_sales, q3_sales, q4_sales FROM yearly_sales;

-- -----------------------------------------------------------------------------
-- 4. INSERT with RETURNING
-- -----------------------------------------------------------------------------

-- Insert with RETURNING into variables (PL/SQL)
/*
DECLARE
    v_emp_id NUMBER;
    v_hire_date DATE;
BEGIN
    INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary)
    VALUES (employees_seq.NEXTVAL, 'Test', 'User', 'TUSER', SYSDATE, 'IT_PROG', 5000)
    RETURNING employee_id, hire_date INTO v_emp_id, v_hire_date;
    
    DBMS_OUTPUT.PUT_LINE('Created employee ' || v_emp_id || ' on ' || v_hire_date);
END;
*/

-- -----------------------------------------------------------------------------
-- 5. Basic UPDATE Statements
-- -----------------------------------------------------------------------------

-- Update single column
UPDATE employees
SET salary = 7000
WHERE employee_id = 100;

-- Update multiple columns
UPDATE employees
SET salary = salary * 1.10,
    commission_pct = NVL(commission_pct, 0) + 0.05
WHERE department_id = 80;

-- Update with expression
UPDATE employees
SET salary = ROUND(salary * 1.05, -2)
WHERE hire_date < ADD_MONTHS(SYSDATE, -60);

-- Update with CASE
UPDATE employees
SET salary = CASE
    WHEN job_id LIKE '%CLERK%' THEN salary * 1.05
    WHEN job_id LIKE '%REP%' THEN salary * 1.08
    WHEN job_id LIKE '%MGR%' THEN salary * 1.10
    ELSE salary * 1.03
END;

-- Update with NULL
UPDATE employees
SET commission_pct = NULL
WHERE department_id NOT IN (80);

-- Update with DEFAULT
UPDATE employees
SET manager_id = DEFAULT
WHERE manager_id = 100;

-- -----------------------------------------------------------------------------
-- 6. UPDATE with Subquery
-- -----------------------------------------------------------------------------

-- Update from subquery (scalar)
UPDATE employees
SET salary = (SELECT AVG(salary) FROM employees)
WHERE salary < 3000;

-- Update with correlated subquery
UPDATE employees e
SET salary = (
    SELECT AVG(salary) * 1.1
    FROM employees e2
    WHERE e2.department_id = e.department_id
)
WHERE salary < (
    SELECT AVG(salary)
    FROM employees e3
    WHERE e3.department_id = e.department_id
);

-- Update multiple columns with subquery
UPDATE employees e
SET (salary, commission_pct) = (
    SELECT AVG(salary), AVG(commission_pct)
    FROM employees e2
    WHERE e2.job_id = e.job_id
)
WHERE department_id = 80;

-- Update with EXISTS
UPDATE employees e
SET salary = salary * 1.15
WHERE EXISTS (
    SELECT 1 FROM departments d
    WHERE d.department_id = e.department_id
    AND d.location_id = 1700
);

-- Update with IN subquery
UPDATE employees
SET department_id = 60
WHERE employee_id IN (
    SELECT employee_id FROM employees
    WHERE job_id = 'IT_PROG' AND department_id IS NULL
);

-- -----------------------------------------------------------------------------
-- 7. UPDATE with JOIN (Oracle syntax)
-- -----------------------------------------------------------------------------

-- Update with inline view
UPDATE (
    SELECT e.employee_id, e.salary, d.department_name
    FROM employees e
    JOIN departments d ON e.department_id = d.department_id
    WHERE d.department_name = 'IT'
)
SET salary = salary * 1.10;

-- Update with MERGE (see MERGE section)

-- -----------------------------------------------------------------------------
-- 8. UPDATE with RETURNING
-- -----------------------------------------------------------------------------

/*
DECLARE
    v_old_salary NUMBER;
    v_new_salary NUMBER;
BEGIN
    UPDATE employees
    SET salary = salary * 1.10
    WHERE employee_id = 100
    RETURNING salary INTO v_new_salary;
    
    DBMS_OUTPUT.PUT_LINE('New salary: ' || v_new_salary);
END;
*/

-- -----------------------------------------------------------------------------
-- 9. Basic DELETE Statements
-- -----------------------------------------------------------------------------

-- Delete specific rows
DELETE FROM employees WHERE employee_id = 300;

-- Delete with multiple conditions
DELETE FROM employees
WHERE department_id = 50
AND hire_date < DATE '2005-01-01';

-- Delete with IN
DELETE FROM employees
WHERE employee_id IN (301, 302, 303);

-- Delete with subquery
DELETE FROM employees
WHERE department_id IN (
    SELECT department_id FROM departments WHERE location_id = 1400
);

-- Delete with EXISTS
DELETE FROM employees e
WHERE EXISTS (
    SELECT 1 FROM job_history jh
    WHERE jh.employee_id = e.employee_id
    AND jh.end_date < ADD_MONTHS(SYSDATE, -120)
);

-- Delete with correlated subquery
DELETE FROM employees e
WHERE salary > (
    SELECT AVG(salary) * 2
    FROM employees e2
    WHERE e2.department_id = e.department_id
);

-- -----------------------------------------------------------------------------
-- 10. DELETE with JOIN (Oracle syntax)
-- -----------------------------------------------------------------------------

-- Delete using inline view
DELETE FROM (
    SELECT e.employee_id
    FROM employees e
    JOIN departments d ON e.department_id = d.department_id
    WHERE d.department_name = 'Contracting'
);

-- -----------------------------------------------------------------------------
-- 11. TRUNCATE (DDL, not DML but related)
-- -----------------------------------------------------------------------------

-- Truncate table (fast delete all rows)
TRUNCATE TABLE temp_employees;

-- Truncate with REUSE STORAGE
TRUNCATE TABLE temp_employees REUSE STORAGE;

-- Truncate with DROP STORAGE (default)
TRUNCATE TABLE temp_employees DROP STORAGE;

-- -----------------------------------------------------------------------------
-- 12. MERGE Statement (UPSERT)
-- -----------------------------------------------------------------------------

-- Basic MERGE
MERGE INTO employees_target t
USING employees_source s
ON (t.employee_id = s.employee_id)
WHEN MATCHED THEN
    UPDATE SET 
        t.first_name = s.first_name,
        t.last_name = s.last_name,
        t.salary = s.salary
WHEN NOT MATCHED THEN
    INSERT (employee_id, first_name, last_name, email, hire_date, job_id, salary)
    VALUES (s.employee_id, s.first_name, s.last_name, s.email, s.hire_date, s.job_id, s.salary);

-- MERGE with DELETE clause
MERGE INTO employees_target t
USING employees_source s
ON (t.employee_id = s.employee_id)
WHEN MATCHED THEN
    UPDATE SET 
        t.salary = s.salary,
        t.last_updated = SYSDATE
    DELETE WHERE t.status = 'INACTIVE'
WHEN NOT MATCHED THEN
    INSERT (employee_id, first_name, last_name, salary, status)
    VALUES (s.employee_id, s.first_name, s.last_name, s.salary, 'ACTIVE');

-- MERGE with conditions
MERGE INTO employees_target t
USING employees_source s
ON (t.employee_id = s.employee_id)
WHEN MATCHED AND s.salary > t.salary THEN
    UPDATE SET t.salary = s.salary
WHEN MATCHED AND s.salary <= t.salary THEN
    UPDATE SET t.last_reviewed = SYSDATE
WHEN NOT MATCHED AND s.salary > 5000 THEN
    INSERT (employee_id, first_name, last_name, salary)
    VALUES (s.employee_id, s.first_name, s.last_name, s.salary);

-- MERGE with subquery source
MERGE INTO department_stats t
USING (
    SELECT department_id,
           COUNT(*) AS emp_count,
           SUM(salary) AS total_salary,
           AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department_id
) s
ON (t.department_id = s.department_id)
WHEN MATCHED THEN
    UPDATE SET 
        t.emp_count = s.emp_count,
        t.total_salary = s.total_salary,
        t.avg_salary = s.avg_salary,
        t.last_updated = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (department_id, emp_count, total_salary, avg_salary, created_date)
    VALUES (s.department_id, s.emp_count, s.total_salary, s.avg_salary, SYSDATE);

-- MERGE only update (no insert)
MERGE INTO employees t
USING (SELECT employee_id, salary * 1.10 AS new_salary FROM high_performers) s
ON (t.employee_id = s.employee_id)
WHEN MATCHED THEN
    UPDATE SET t.salary = s.new_salary;

-- MERGE only insert (no update)
MERGE INTO employees_archive t
USING employees_current s
ON (t.employee_id = s.employee_id)
WHEN NOT MATCHED THEN
    INSERT (employee_id, first_name, last_name, archive_date)
    VALUES (s.employee_id, s.first_name, s.last_name, SYSDATE);

-- -----------------------------------------------------------------------------
-- 13. Transaction Control with DML
-- -----------------------------------------------------------------------------

-- Commit changes
INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary)
VALUES (400, 'Test', 'User', 'TUSER', SYSDATE, 'IT_PROG', 5000);
COMMIT;

-- Rollback changes
UPDATE employees SET salary = salary * 2 WHERE department_id = 60;
ROLLBACK;

-- Savepoint
INSERT INTO emp_audit (action, emp_id, action_date) VALUES ('INSERT', 400, SYSDATE);
SAVEPOINT after_audit;
UPDATE employees SET salary = 10000 WHERE employee_id = 400;
-- If something goes wrong:
ROLLBACK TO after_audit;
-- Continue with commit:
COMMIT;

-- -----------------------------------------------------------------------------
-- 14. DML with Hints
-- -----------------------------------------------------------------------------

-- Insert with append hint (direct path)
INSERT /*+ APPEND */ INTO employees_archive
SELECT * FROM employees WHERE hire_date < DATE '2000-01-01';

-- Insert with parallel hint
INSERT /*+ APPEND PARALLEL(4) */ INTO large_table
SELECT * FROM source_table;

-- Update with full table scan
UPDATE /*+ FULL(e) */ employees e
SET salary = salary * 1.05
WHERE department_id = 50;

-- Delete with index hint
DELETE /*+ INDEX(e emp_dept_idx) */ FROM employees e
WHERE department_id = 999;

-- MERGE with hints
MERGE /*+ USE_HASH(t s) */ INTO employees_target t
USING employees_source s
ON (t.employee_id = s.employee_id)
WHEN MATCHED THEN UPDATE SET t.salary = s.salary;

-- -----------------------------------------------------------------------------
-- 15. Error Logging in DML
-- -----------------------------------------------------------------------------

-- Create error log table
-- EXEC DBMS_ERRLOG.CREATE_ERROR_LOG('EMPLOYEES', 'ERR$_EMPLOYEES');

-- DML with error logging
INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary)
SELECT employee_id, first_name, last_name, email, hire_date, job_id, salary
FROM staging_employees
LOG ERRORS INTO err$_employees ('Load ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD'))
REJECT LIMIT UNLIMITED;

-- UPDATE with error logging
UPDATE employees
SET salary = TO_NUMBER(salary_string)
LOG ERRORS INTO err$_employees REJECT LIMIT 100;

-- MERGE with error logging
MERGE INTO employees t
USING staging_employees s
ON (t.employee_id = s.employee_id)
WHEN MATCHED THEN
    UPDATE SET t.salary = s.salary
WHEN NOT MATCHED THEN
    INSERT (employee_id, first_name, last_name, email, hire_date, job_id, salary)
    VALUES (s.employee_id, s.first_name, s.last_name, s.email, s.hire_date, s.job_id, s.salary)
LOG ERRORS INTO err$_employees ('MERGE_' || TO_CHAR(SYSDATE, 'YYYYMMDD'))
REJECT LIMIT 1000;

