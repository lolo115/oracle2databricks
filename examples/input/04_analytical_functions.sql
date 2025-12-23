-- ============================================================================
-- File: 04_analytical_functions.sql
-- Description: Window/Analytical functions - ranking, offset, aggregate windows
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. ROW_NUMBER
-- -----------------------------------------------------------------------------

-- Basic row numbering
SELECT employee_id, first_name, salary,
       ROW_NUMBER() OVER (ORDER BY salary DESC) AS salary_rank
FROM employees;

-- Row number within partitions
SELECT employee_id, first_name, department_id, salary,
       ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dept_rank
FROM employees;

-- Using ROW_NUMBER for pagination
SELECT * FROM (
    SELECT employee_id, first_name, salary,
           ROW_NUMBER() OVER (ORDER BY employee_id) AS rn
    FROM employees
)
WHERE rn BETWEEN 11 AND 20;

-- Top N per group
SELECT * FROM (
    SELECT employee_id, first_name, department_id, salary,
           ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rn
    FROM employees
)
WHERE rn <= 3;

-- -----------------------------------------------------------------------------
-- 2. RANK and DENSE_RANK
-- -----------------------------------------------------------------------------

-- RANK - gaps in ranking for ties
SELECT employee_id, first_name, salary,
       RANK() OVER (ORDER BY salary DESC) AS salary_rank
FROM employees;

-- DENSE_RANK - no gaps for ties
SELECT employee_id, first_name, salary,
       DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_dense_rank
FROM employees;

-- Comparing ROW_NUMBER, RANK, DENSE_RANK
SELECT employee_id, first_name, salary,
       ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num,
       RANK() OVER (ORDER BY salary DESC) AS rank_val,
       DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank_val
FROM employees
ORDER BY salary DESC;

-- Rank within partitions
SELECT employee_id, first_name, department_id, salary,
       RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dept_rank,
       DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dept_dense_rank
FROM employees;

-- -----------------------------------------------------------------------------
-- 3. NTILE - Distribution into buckets
-- -----------------------------------------------------------------------------

-- Divide into quartiles
SELECT employee_id, first_name, salary,
       NTILE(4) OVER (ORDER BY salary) AS salary_quartile
FROM employees;

-- Divide into deciles
SELECT employee_id, first_name, salary,
       NTILE(10) OVER (ORDER BY salary) AS salary_decile
FROM employees;

-- NTILE within partitions
SELECT employee_id, first_name, department_id, salary,
       NTILE(3) OVER (PARTITION BY department_id ORDER BY salary) AS salary_tercile
FROM employees;

-- -----------------------------------------------------------------------------
-- 4. CUME_DIST and PERCENT_RANK
-- -----------------------------------------------------------------------------

-- CUME_DIST - cumulative distribution
SELECT employee_id, first_name, salary,
       CUME_DIST() OVER (ORDER BY salary) AS cume_dist,
       ROUND(CUME_DIST() OVER (ORDER BY salary) * 100, 2) AS percentile
FROM employees;

-- PERCENT_RANK
SELECT employee_id, first_name, salary,
       PERCENT_RANK() OVER (ORDER BY salary) AS percent_rank,
       ROUND(PERCENT_RANK() OVER (ORDER BY salary) * 100, 2) AS percent
FROM employees;

-- Comparing CUME_DIST and PERCENT_RANK
SELECT employee_id, first_name, salary,
       ROUND(CUME_DIST() OVER (ORDER BY salary), 4) AS cume_dist,
       ROUND(PERCENT_RANK() OVER (ORDER BY salary), 4) AS percent_rank
FROM employees
ORDER BY salary;

-- -----------------------------------------------------------------------------
-- 5. LAG and LEAD - Offset functions
-- -----------------------------------------------------------------------------

-- LAG - access previous row
SELECT employee_id, first_name, hire_date, salary,
       LAG(salary) OVER (ORDER BY hire_date) AS prev_salary,
       LAG(first_name) OVER (ORDER BY hire_date) AS prev_employee
FROM employees;

-- LAG with offset and default
SELECT employee_id, first_name, hire_date, salary,
       LAG(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_salary,
       LAG(salary, 2, 0) OVER (ORDER BY hire_date) AS prev2_salary
FROM employees;

-- LEAD - access next row
SELECT employee_id, first_name, hire_date, salary,
       LEAD(salary) OVER (ORDER BY hire_date) AS next_salary,
       LEAD(first_name) OVER (ORDER BY hire_date) AS next_employee
FROM employees;

-- LEAD with offset and default
SELECT employee_id, first_name, hire_date, salary,
       LEAD(salary, 1, 0) OVER (ORDER BY hire_date) AS next_salary,
       LEAD(salary, 2, 0) OVER (ORDER BY hire_date) AS next2_salary
FROM employees;

-- LAG/LEAD within partitions
SELECT employee_id, first_name, department_id, hire_date, salary,
       LAG(salary) OVER (PARTITION BY department_id ORDER BY hire_date) AS prev_dept_salary,
       LEAD(salary) OVER (PARTITION BY department_id ORDER BY hire_date) AS next_dept_salary
FROM employees;

-- Calculate salary changes
SELECT employee_id, first_name, hire_date, salary,
       LAG(salary) OVER (ORDER BY hire_date) AS prev_salary,
       salary - LAG(salary) OVER (ORDER BY hire_date) AS salary_change,
       ROUND((salary - LAG(salary) OVER (ORDER BY hire_date)) / 
             NULLIF(LAG(salary) OVER (ORDER BY hire_date), 0) * 100, 2) AS pct_change
FROM employees;

-- -----------------------------------------------------------------------------
-- 6. FIRST_VALUE and LAST_VALUE
-- -----------------------------------------------------------------------------

-- FIRST_VALUE
SELECT employee_id, first_name, department_id, salary,
       FIRST_VALUE(first_name) OVER (PARTITION BY department_id ORDER BY salary DESC) AS top_earner,
       FIRST_VALUE(salary) OVER (PARTITION BY department_id ORDER BY salary DESC) AS top_salary
FROM employees;

-- LAST_VALUE (requires proper window frame)
SELECT employee_id, first_name, department_id, salary,
       LAST_VALUE(first_name) OVER (
           PARTITION BY department_id 
           ORDER BY salary DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS lowest_earner,
       LAST_VALUE(salary) OVER (
           PARTITION BY department_id 
           ORDER BY salary DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS lowest_salary
FROM employees;

-- FIRST_VALUE with IGNORE NULLS
SELECT employee_id, first_name, commission_pct,
       FIRST_VALUE(commission_pct IGNORE NULLS) OVER (ORDER BY employee_id) AS first_commission
FROM employees;

-- -----------------------------------------------------------------------------
-- 7. NTH_VALUE
-- -----------------------------------------------------------------------------

-- Get second highest salary
SELECT employee_id, first_name, department_id, salary,
       NTH_VALUE(salary, 2) OVER (
           PARTITION BY department_id 
           ORDER BY salary DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS second_highest_salary
FROM employees;

-- Get 3rd value
SELECT employee_id, first_name, salary,
       NTH_VALUE(first_name, 3) OVER (
           ORDER BY salary DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS third_highest_earner
FROM employees;

-- -----------------------------------------------------------------------------
-- 8. Aggregate functions as window functions
-- -----------------------------------------------------------------------------

-- Running SUM
SELECT employee_id, first_name, hire_date, salary,
       SUM(salary) OVER (ORDER BY hire_date) AS running_total
FROM employees;

-- Running COUNT
SELECT employee_id, first_name, hire_date,
       COUNT(*) OVER (ORDER BY hire_date) AS running_count
FROM employees;

-- Running AVG
SELECT employee_id, first_name, hire_date, salary,
       ROUND(AVG(salary) OVER (ORDER BY hire_date), 2) AS running_avg
FROM employees;

-- Partition aggregates
SELECT employee_id, first_name, department_id, salary,
       SUM(salary) OVER (PARTITION BY department_id) AS dept_total,
       COUNT(*) OVER (PARTITION BY department_id) AS dept_count,
       ROUND(AVG(salary) OVER (PARTITION BY department_id), 2) AS dept_avg
FROM employees;

-- Compare to partition total
SELECT employee_id, first_name, department_id, salary,
       SUM(salary) OVER (PARTITION BY department_id) AS dept_total,
       ROUND(salary / SUM(salary) OVER (PARTITION BY department_id) * 100, 2) AS pct_of_dept
FROM employees;

-- Compare to overall total
SELECT employee_id, first_name, department_id, salary,
       SUM(salary) OVER () AS total_salary,
       SUM(salary) OVER (PARTITION BY department_id) AS dept_total,
       ROUND(salary / SUM(salary) OVER () * 100, 2) AS pct_of_total
FROM employees;

-- -----------------------------------------------------------------------------
-- 9. Window frame specifications
-- -----------------------------------------------------------------------------

-- ROWS BETWEEN - physical rows
SELECT employee_id, first_name, hire_date, salary,
       SUM(salary) OVER (ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum_3_rows,
       AVG(salary) OVER (ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_3_rows
FROM employees;

-- Moving average (3-day)
SELECT employee_id, first_name, hire_date, salary,
       ROUND(AVG(salary) OVER (
           ORDER BY hire_date 
           ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
       ), 2) AS moving_avg_3
FROM employees;

-- ROWS UNBOUNDED PRECEDING
SELECT employee_id, first_name, hire_date, salary,
       SUM(salary) OVER (ORDER BY hire_date ROWS UNBOUNDED PRECEDING) AS running_total
FROM employees;

-- ROWS between current and following
SELECT employee_id, first_name, hire_date, salary,
       SUM(salary) OVER (
           ORDER BY hire_date 
           ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
       ) AS next_3_sum
FROM employees;

-- RANGE BETWEEN - logical range
SELECT employee_id, first_name, salary,
       COUNT(*) OVER (ORDER BY salary RANGE BETWEEN 1000 PRECEDING AND 1000 FOLLOWING) AS similar_salary_count
FROM employees;

-- RANGE for date intervals
SELECT employee_id, first_name, hire_date, salary,
       SUM(salary) OVER (
           ORDER BY hire_date 
           RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW
       ) AS last_30_days_salary
FROM employees;

-- Full range specification
SELECT employee_id, first_name, department_id, salary,
       MIN(salary) OVER (
           PARTITION BY department_id 
           ORDER BY salary
           RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS dept_min,
       MAX(salary) OVER (
           PARTITION BY department_id 
           ORDER BY salary
           RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS dept_max
FROM employees;

-- -----------------------------------------------------------------------------
-- 10. WINDOW clause (named windows)
-- -----------------------------------------------------------------------------

-- Using named window
SELECT employee_id, first_name, department_id, salary,
       SUM(salary) OVER w AS dept_running_total,
       AVG(salary) OVER w AS dept_running_avg,
       COUNT(*) OVER w AS dept_running_count
FROM employees
WINDOW w AS (PARTITION BY department_id ORDER BY hire_date);

-- Multiple named windows
SELECT employee_id, first_name, department_id, salary,
       SUM(salary) OVER dept_window AS dept_total,
       SUM(salary) OVER overall_window AS overall_total,
       ROW_NUMBER() OVER dept_window AS dept_row
FROM employees
WINDOW 
    dept_window AS (PARTITION BY department_id ORDER BY salary DESC),
    overall_window AS (ORDER BY salary DESC);

-- -----------------------------------------------------------------------------
-- 11. RATIO_TO_REPORT
-- -----------------------------------------------------------------------------

-- Percentage of total
SELECT employee_id, first_name, department_id, salary,
       ROUND(RATIO_TO_REPORT(salary) OVER () * 100, 2) AS pct_of_total
FROM employees;

-- Percentage within partition
SELECT employee_id, first_name, department_id, salary,
       ROUND(RATIO_TO_REPORT(salary) OVER (PARTITION BY department_id) * 100, 2) AS pct_of_dept
FROM employees;

-- -----------------------------------------------------------------------------
-- 12. Complex analytical queries
-- -----------------------------------------------------------------------------

-- Year-over-year comparison
WITH yearly_data AS (
    SELECT EXTRACT(YEAR FROM hire_date) AS year,
           COUNT(*) AS hire_count,
           SUM(salary) AS total_salary
    FROM employees
    GROUP BY EXTRACT(YEAR FROM hire_date)
)
SELECT year, hire_count, total_salary,
       LAG(hire_count) OVER (ORDER BY year) AS prev_year_hires,
       hire_count - LAG(hire_count) OVER (ORDER BY year) AS hire_change,
       LAG(total_salary) OVER (ORDER BY year) AS prev_year_salary,
       ROUND((total_salary - LAG(total_salary) OVER (ORDER BY year)) / 
             NULLIF(LAG(total_salary) OVER (ORDER BY year), 0) * 100, 2) AS salary_pct_change
FROM yearly_data;

-- Salary bands with distribution
SELECT salary_band, emp_count,
       SUM(emp_count) OVER (ORDER BY salary_band) AS cumulative_count,
       ROUND(RATIO_TO_REPORT(emp_count) OVER () * 100, 2) AS percentage,
       ROUND(SUM(RATIO_TO_REPORT(emp_count) OVER ()) 
             OVER (ORDER BY salary_band) * 100, 2) AS cumulative_pct
FROM (
    SELECT 
        CASE 
            WHEN salary < 5000 THEN '0-5000'
            WHEN salary < 10000 THEN '5000-10000'
            WHEN salary < 15000 THEN '10000-15000'
            ELSE '15000+'
        END AS salary_band,
        COUNT(*) AS emp_count
    FROM employees
    GROUP BY 
        CASE 
            WHEN salary < 5000 THEN '0-5000'
            WHEN salary < 10000 THEN '5000-10000'
            WHEN salary < 15000 THEN '10000-15000'
            ELSE '15000+'
        END
);

-- Gap analysis
SELECT employee_id, first_name, hire_date,
       LAG(hire_date) OVER (ORDER BY hire_date) AS prev_hire_date,
       hire_date - LAG(hire_date) OVER (ORDER BY hire_date) AS days_since_last_hire
FROM employees
ORDER BY hire_date;

-- Consecutive ranking
SELECT employee_id, first_name, department_id, salary,
       employee_id - ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY employee_id) AS grp
FROM employees;

-- Identifying streaks
WITH employee_changes AS (
    SELECT employee_id, hire_date, department_id,
           LAG(department_id) OVER (ORDER BY employee_id) AS prev_dept,
           CASE WHEN department_id = LAG(department_id) OVER (ORDER BY employee_id) 
                THEN 0 ELSE 1 END AS dept_change
    FROM employees
)
SELECT employee_id, hire_date, department_id,
       SUM(dept_change) OVER (ORDER BY employee_id) AS streak_id
FROM employee_changes;

-- Running distinct count (approximation)
SELECT hire_date, department_id,
       COUNT(DISTINCT department_id) OVER (ORDER BY hire_date) AS cumulative_depts
FROM employees;

-- Median using analytical functions
SELECT DISTINCT department_id,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) 
           OVER (PARTITION BY department_id) AS median_salary
FROM employees;

-- Multiple window function calculations in single query
SELECT 
    employee_id,
    first_name,
    department_id,
    salary,
    hire_date,
    -- Ranking functions
    ROW_NUMBER() OVER (ORDER BY salary DESC) AS overall_rank,
    ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dept_rank,
    -- Comparison functions
    salary - LAG(salary) OVER (ORDER BY hire_date) AS salary_vs_prev_hire,
    salary - FIRST_VALUE(salary) OVER (PARTITION BY department_id ORDER BY hire_date) AS salary_vs_first_in_dept,
    -- Aggregate windows
    SUM(salary) OVER (PARTITION BY department_id) AS dept_total,
    ROUND(AVG(salary) OVER (PARTITION BY department_id), 2) AS dept_avg,
    -- Running calculations
    SUM(salary) OVER (ORDER BY hire_date) AS running_payroll,
    -- Percentages
    ROUND(salary / SUM(salary) OVER (PARTITION BY department_id) * 100, 2) AS pct_of_dept
FROM employees
ORDER BY department_id, salary DESC;

