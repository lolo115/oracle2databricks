-- ============================================================================
-- File: 03_aggregate_functions.sql
-- Description: Aggregate functions, GROUP BY, HAVING, and grouping extensions
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Basic Aggregate Functions
-- -----------------------------------------------------------------------------

-- COUNT variations
SELECT COUNT(*) AS total_rows FROM employees;
SELECT COUNT(employee_id) AS count_emp_id FROM employees;
SELECT COUNT(commission_pct) AS count_commission FROM employees;  -- excludes NULLs
SELECT COUNT(DISTINCT department_id) AS distinct_depts FROM employees;
SELECT COUNT(DISTINCT job_id) AS distinct_jobs FROM employees;
SELECT COUNT(ALL department_id) AS all_depts FROM employees;  -- includes duplicates

-- SUM
SELECT SUM(salary) AS total_salary FROM employees;
SELECT SUM(DISTINCT salary) AS sum_distinct_salary FROM employees;
SELECT SUM(salary * 12) AS annual_payroll FROM employees;
SELECT SUM(salary + NVL(commission_pct, 0) * salary) AS total_compensation FROM employees;

-- AVG
SELECT AVG(salary) AS avg_salary FROM employees;
SELECT AVG(DISTINCT salary) AS avg_distinct_salary FROM employees;
SELECT AVG(NVL(commission_pct, 0)) AS avg_commission FROM employees;

-- MIN and MAX
SELECT MIN(salary) AS min_salary, MAX(salary) AS max_salary FROM employees;
SELECT MIN(hire_date) AS first_hire, MAX(hire_date) AS last_hire FROM employees;
SELECT MIN(first_name) AS min_name, MAX(first_name) AS max_name FROM employees;

-- VARIANCE and STDDEV
SELECT VARIANCE(salary) AS salary_variance FROM employees;
SELECT STDDEV(salary) AS salary_stddev FROM employees;
SELECT VAR_POP(salary) AS population_variance FROM employees;
SELECT VAR_SAMP(salary) AS sample_variance FROM employees;
SELECT STDDEV_POP(salary) AS population_stddev FROM employees;
SELECT STDDEV_SAMP(salary) AS sample_stddev FROM employees;

-- Median
SELECT MEDIAN(salary) AS median_salary FROM employees;

-- STATS functions
SELECT STATS_MODE(department_id) AS most_common_dept FROM employees;

-- -----------------------------------------------------------------------------
-- 2. GROUP BY basics
-- -----------------------------------------------------------------------------

-- Simple GROUP BY
SELECT department_id, COUNT(*) AS emp_count
FROM employees
GROUP BY department_id;

-- GROUP BY with multiple aggregates
SELECT department_id,
       COUNT(*) AS emp_count,
       SUM(salary) AS total_salary,
       AVG(salary) AS avg_salary,
       MIN(salary) AS min_salary,
       MAX(salary) AS max_salary
FROM employees
GROUP BY department_id;

-- GROUP BY multiple columns
SELECT department_id, job_id, COUNT(*) AS emp_count
FROM employees
GROUP BY department_id, job_id
ORDER BY department_id, job_id;

-- GROUP BY with expressions
SELECT EXTRACT(YEAR FROM hire_date) AS hire_year,
       COUNT(*) AS emp_count
FROM employees
GROUP BY EXTRACT(YEAR FROM hire_date)
ORDER BY hire_year;

SELECT TRUNC(salary, -3) AS salary_range,
       COUNT(*) AS emp_count
FROM employees
GROUP BY TRUNC(salary, -3)
ORDER BY salary_range;

-- GROUP BY with CASE
SELECT 
    CASE 
        WHEN salary < 5000 THEN 'Low'
        WHEN salary < 10000 THEN 'Medium'
        ELSE 'High'
    END AS salary_band,
    COUNT(*) AS emp_count,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY 
    CASE 
        WHEN salary < 5000 THEN 'Low'
        WHEN salary < 10000 THEN 'Medium'
        ELSE 'High'
    END;

-- -----------------------------------------------------------------------------
-- 3. HAVING clause
-- -----------------------------------------------------------------------------

-- Simple HAVING
SELECT department_id, COUNT(*) AS emp_count
FROM employees
GROUP BY department_id
HAVING COUNT(*) > 5;

-- HAVING with multiple conditions
SELECT department_id, 
       AVG(salary) AS avg_salary,
       COUNT(*) AS emp_count
FROM employees
GROUP BY department_id
HAVING COUNT(*) > 5 AND AVG(salary) > 5000;

-- HAVING with subquery
SELECT department_id, AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id
HAVING AVG(salary) > (SELECT AVG(salary) FROM employees);

-- WHERE and HAVING together
SELECT department_id, AVG(salary) AS avg_salary
FROM employees
WHERE job_id NOT LIKE '%CLERK%'
GROUP BY department_id
HAVING AVG(salary) > 8000;

-- -----------------------------------------------------------------------------
-- 4. ROLLUP - Hierarchical subtotals
-- -----------------------------------------------------------------------------

-- Single column ROLLUP
SELECT department_id, SUM(salary) AS total_salary
FROM employees
GROUP BY ROLLUP(department_id);

-- Multi-column ROLLUP
SELECT department_id, job_id, SUM(salary) AS total_salary
FROM employees
GROUP BY ROLLUP(department_id, job_id);

-- ROLLUP with formatting
SELECT 
    COALESCE(TO_CHAR(department_id), 'All Departments') AS department,
    COALESCE(job_id, 'All Jobs') AS job,
    SUM(salary) AS total_salary
FROM employees
GROUP BY ROLLUP(department_id, job_id);

-- Partial ROLLUP
SELECT department_id, job_id, manager_id, SUM(salary)
FROM employees
GROUP BY department_id, ROLLUP(job_id, manager_id);

-- -----------------------------------------------------------------------------
-- 5. CUBE - All possible subtotals
-- -----------------------------------------------------------------------------

-- Simple CUBE
SELECT department_id, job_id, SUM(salary) AS total_salary
FROM employees
GROUP BY CUBE(department_id, job_id);

-- CUBE with three dimensions
SELECT department_id, job_id, 
       EXTRACT(YEAR FROM hire_date) AS hire_year,
       COUNT(*) AS emp_count,
       SUM(salary) AS total_salary
FROM employees
GROUP BY CUBE(department_id, job_id, EXTRACT(YEAR FROM hire_date));

-- Partial CUBE
SELECT department_id, job_id, manager_id, SUM(salary)
FROM employees
GROUP BY department_id, CUBE(job_id, manager_id);

-- -----------------------------------------------------------------------------
-- 6. GROUPING SETS - Custom groupings
-- -----------------------------------------------------------------------------

-- Define specific groupings
SELECT department_id, job_id, SUM(salary)
FROM employees
GROUP BY GROUPING SETS (
    (department_id, job_id),
    (department_id),
    (job_id),
    ()
);

-- Complex grouping sets
SELECT department_id, job_id, manager_id, SUM(salary)
FROM employees
GROUP BY GROUPING SETS (
    (department_id, job_id),
    (department_id, manager_id),
    (job_id)
);

-- Combining ROLLUP and CUBE in GROUPING SETS
SELECT department_id, job_id, manager_id, SUM(salary)
FROM employees
GROUP BY GROUPING SETS (
    ROLLUP(department_id, job_id),
    CUBE(manager_id)
);

-- -----------------------------------------------------------------------------
-- 7. GROUPING and GROUPING_ID functions
-- -----------------------------------------------------------------------------

-- GROUPING function to identify subtotal rows
SELECT 
    department_id,
    job_id,
    SUM(salary) AS total_salary,
    GROUPING(department_id) AS is_dept_subtotal,
    GROUPING(job_id) AS is_job_subtotal
FROM employees
GROUP BY ROLLUP(department_id, job_id);

-- Using GROUPING for conditional labels
SELECT 
    CASE GROUPING(department_id)
        WHEN 1 THEN 'All Departments'
        ELSE TO_CHAR(department_id)
    END AS department,
    CASE GROUPING(job_id)
        WHEN 1 THEN 'All Jobs'
        ELSE job_id
    END AS job,
    SUM(salary) AS total_salary
FROM employees
GROUP BY ROLLUP(department_id, job_id);

-- GROUPING_ID for identifying grouping level
SELECT 
    department_id,
    job_id,
    SUM(salary) AS total_salary,
    GROUPING_ID(department_id, job_id) AS grouping_level
FROM employees
GROUP BY CUBE(department_id, job_id)
ORDER BY GROUPING_ID(department_id, job_id);

-- GROUP_ID to eliminate duplicate rows
SELECT department_id, job_id, SUM(salary), GROUP_ID()
FROM employees
GROUP BY GROUPING SETS (
    (department_id, job_id),
    (department_id, job_id)
)
HAVING GROUP_ID() = 0;

-- -----------------------------------------------------------------------------
-- 8. LIST Aggregation (LISTAGG)
-- -----------------------------------------------------------------------------

-- Simple LISTAGG
SELECT department_id,
       LISTAGG(first_name, ', ') WITHIN GROUP (ORDER BY first_name) AS employees
FROM employees
GROUP BY department_id;

-- LISTAGG with DISTINCT (Oracle 19c+)
SELECT department_id,
       LISTAGG(DISTINCT job_id, ', ') WITHIN GROUP (ORDER BY job_id) AS jobs
FROM employees
GROUP BY department_id;

-- LISTAGG with overflow handling (Oracle 12c R2+)
SELECT department_id,
       LISTAGG(first_name, ', ' ON OVERFLOW TRUNCATE '...' WITH COUNT) 
           WITHIN GROUP (ORDER BY first_name) AS employees
FROM employees
GROUP BY department_id;

-- Full LISTAGG overflow options
SELECT department_id,
       LISTAGG(first_name || ' ' || last_name, '; ' 
               ON OVERFLOW TRUNCATE '... and more' WITHOUT COUNT) 
           WITHIN GROUP (ORDER BY last_name) AS employee_list
FROM employees
GROUP BY department_id;

-- -----------------------------------------------------------------------------
-- 9. COLLECT - Array aggregation
-- -----------------------------------------------------------------------------

-- Create nested table type first
-- CREATE TYPE varchar2_ntt AS TABLE OF VARCHAR2(100);

-- COLLECT function
SELECT department_id,
       COLLECT(first_name) AS employee_names
FROM employees
GROUP BY department_id;

-- COLLECT with DISTINCT
SELECT department_id,
       COLLECT(DISTINCT job_id) AS jobs
FROM employees
GROUP BY department_id;

-- -----------------------------------------------------------------------------
-- 10. Statistical Aggregates
-- -----------------------------------------------------------------------------

-- Correlation
SELECT CORR(salary, commission_pct) AS salary_commission_correlation
FROM employees
WHERE commission_pct IS NOT NULL;

-- Covariance
SELECT COVAR_POP(salary, commission_pct) AS population_covariance,
       COVAR_SAMP(salary, commission_pct) AS sample_covariance
FROM employees
WHERE commission_pct IS NOT NULL;

-- Linear regression
SELECT 
    REGR_SLOPE(salary, commission_pct) AS slope,
    REGR_INTERCEPT(salary, commission_pct) AS intercept,
    REGR_R2(salary, commission_pct) AS r_squared,
    REGR_COUNT(salary, commission_pct) AS count,
    REGR_AVGX(salary, commission_pct) AS avg_x,
    REGR_AVGY(salary, commission_pct) AS avg_y
FROM employees
WHERE commission_pct IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 11. Aggregate with KEEP (FIRST/LAST)
-- -----------------------------------------------------------------------------

-- Get salary of first/last hired employee per department
SELECT department_id,
       MIN(hire_date) AS first_hire_date,
       MAX(hire_date) AS last_hire_date,
       MIN(salary) KEEP (DENSE_RANK FIRST ORDER BY hire_date) AS first_hired_salary,
       MAX(salary) KEEP (DENSE_RANK LAST ORDER BY hire_date) AS last_hired_salary
FROM employees
GROUP BY department_id;

-- Get name of highest paid employee per department
SELECT department_id,
       MAX(salary) AS max_salary,
       MAX(first_name) KEEP (DENSE_RANK FIRST ORDER BY salary DESC) AS top_earner
FROM employees
GROUP BY department_id;

-- Multiple KEEP aggregates
SELECT department_id,
       MIN(first_name) KEEP (DENSE_RANK FIRST ORDER BY salary) AS lowest_paid_name,
       MIN(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) AS min_salary,
       MAX(first_name) KEEP (DENSE_RANK LAST ORDER BY salary) AS highest_paid_name,
       MAX(salary) KEEP (DENSE_RANK LAST ORDER BY salary) AS max_salary
FROM employees
GROUP BY department_id;

-- -----------------------------------------------------------------------------
-- 12. PERCENTILE functions
-- -----------------------------------------------------------------------------

-- Percentile continuous
SELECT 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) AS q1_salary,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) AS q3_salary
FROM employees;

-- Percentile discrete
SELECT 
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY salary) AS q1_salary,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY salary) AS q3_salary
FROM employees;

-- Percentile by group
SELECT department_id,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees
GROUP BY department_id;

-- -----------------------------------------------------------------------------
-- 13. ANY_VALUE (Oracle 21c+)
-- -----------------------------------------------------------------------------

-- ANY_VALUE returns any non-null value from group
SELECT department_id,
       ANY_VALUE(department_name) AS dept_name,
       COUNT(*) AS emp_count
FROM employees e
JOIN departments d USING (department_id)
GROUP BY department_id;

-- -----------------------------------------------------------------------------
-- 14. Complex aggregate queries
-- -----------------------------------------------------------------------------

-- Top N per group using aggregate
SELECT department_id,
       MAX(salary) AS top1_salary,
       MAX(CASE WHEN salary_rank = 2 THEN salary END) AS top2_salary,
       MAX(CASE WHEN salary_rank = 3 THEN salary END) AS top3_salary
FROM (
    SELECT department_id, salary,
           DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank
    FROM employees
)
WHERE salary_rank <= 3
GROUP BY department_id;

-- Running totals using GROUP BY with windowing
SELECT department_id,
       hire_date,
       SUM(salary) AS daily_total,
       SUM(SUM(salary)) OVER (PARTITION BY department_id ORDER BY hire_date) AS running_total
FROM employees
GROUP BY department_id, hire_date
ORDER BY department_id, hire_date;

-- Comparing to previous period
WITH monthly_sales AS (
    SELECT TRUNC(hire_date, 'MM') AS month,
           COUNT(*) AS hire_count
    FROM employees
    GROUP BY TRUNC(hire_date, 'MM')
)
SELECT month,
       hire_count,
       LAG(hire_count) OVER (ORDER BY month) AS prev_month,
       hire_count - LAG(hire_count) OVER (ORDER BY month) AS change
FROM monthly_sales
ORDER BY month;

