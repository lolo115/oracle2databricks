-- ============================================================================
-- File: 10_pivot_unpivot_model.sql
-- Description: PIVOT, UNPIVOT operations and MODEL clause
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Basic PIVOT
-- -----------------------------------------------------------------------------

-- Pivot department counts by job
SELECT *
FROM (
    SELECT job_id, department_id
    FROM employees
)
PIVOT (
    COUNT(*) 
    FOR department_id IN (10, 20, 30, 50, 60, 80, 90, 100, 110)
);

-- Pivot with column aliases
SELECT *
FROM (
    SELECT job_id, department_id
    FROM employees
)
PIVOT (
    COUNT(*) AS cnt
    FOR department_id IN (
        10 AS admin,
        20 AS marketing,
        30 AS purchasing,
        50 AS shipping,
        60 AS it,
        80 AS sales,
        90 AS executive
    )
);

-- -----------------------------------------------------------------------------
-- 2. PIVOT with Multiple Aggregates
-- -----------------------------------------------------------------------------

-- Multiple aggregate functions
SELECT *
FROM (
    SELECT job_id, department_id, salary
    FROM employees
)
PIVOT (
    COUNT(*) AS cnt,
    SUM(salary) AS total_sal,
    AVG(salary) AS avg_sal
    FOR department_id IN (50 AS shipping, 60 AS it, 80 AS sales)
);

-- Pivot salary statistics by department
SELECT *
FROM (
    SELECT department_id, salary
    FROM employees
)
PIVOT (
    MIN(salary) AS min_sal,
    MAX(salary) AS max_sal,
    AVG(salary) AS avg_sal,
    COUNT(*) AS emp_count
    FOR department_id IN (50, 60, 80, 90)
);

-- -----------------------------------------------------------------------------
-- 3. PIVOT with Multiple Columns
-- -----------------------------------------------------------------------------

-- Pivot on multiple columns (job and year)
SELECT *
FROM (
    SELECT job_id, 
           EXTRACT(YEAR FROM hire_date) AS hire_year,
           employee_id
    FROM employees
)
PIVOT (
    COUNT(employee_id)
    FOR hire_year IN (2005, 2006, 2007, 2008)
);

-- Cross-tab of department and job
SELECT *
FROM (
    SELECT department_id, job_id, salary
    FROM employees
)
PIVOT (
    SUM(salary)
    FOR job_id IN (
        'IT_PROG' AS it_prog,
        'SA_REP' AS sa_rep,
        'ST_CLERK' AS st_clerk,
        'SH_CLERK' AS sh_clerk
    )
)
ORDER BY department_id;

-- -----------------------------------------------------------------------------
-- 4. Dynamic PIVOT (using XML)
-- -----------------------------------------------------------------------------

-- PIVOT XML for dynamic columns
SELECT *
FROM (
    SELECT job_id, department_id
    FROM employees
)
PIVOT XML (
    COUNT(*) AS cnt
    FOR department_id IN (ANY)
);

-- Pivot XML with subquery
SELECT *
FROM (
    SELECT job_id, department_id, salary
    FROM employees
)
PIVOT XML (
    SUM(salary) AS total
    FOR department_id IN (SELECT DISTINCT department_id FROM employees)
);

-- -----------------------------------------------------------------------------
-- 5. Basic UNPIVOT
-- -----------------------------------------------------------------------------

-- Create pivoted data then unpivot
WITH pivoted_data AS (
    SELECT *
    FROM (
        SELECT department_id, job_id
        FROM employees
    )
    PIVOT (
        COUNT(*)
        FOR job_id IN (
            'IT_PROG' AS it_prog,
            'SA_REP' AS sa_rep,
            'ST_CLERK' AS st_clerk
        )
    )
)
SELECT *
FROM pivoted_data
UNPIVOT (
    emp_count FOR job_id IN (it_prog, sa_rep, st_clerk)
);

-- Unpivot salary columns (example structure)
/*
WITH salary_columns AS (
    SELECT employee_id, 
           salary AS base_salary,
           salary * 0.1 AS bonus,
           salary * 0.05 AS allowance
    FROM employees
)
SELECT employee_id, pay_type, amount
FROM salary_columns
UNPIVOT (
    amount FOR pay_type IN (base_salary, bonus, allowance)
);
*/

-- -----------------------------------------------------------------------------
-- 6. UNPIVOT with INCLUDE/EXCLUDE NULLS
-- -----------------------------------------------------------------------------

-- Include nulls in unpivot
/*
SELECT *
FROM table_with_nulls
UNPIVOT INCLUDE NULLS (
    value FOR column_name IN (col1, col2, col3)
);

-- Exclude nulls (default)
SELECT *
FROM table_with_nulls
UNPIVOT EXCLUDE NULLS (
    value FOR column_name IN (col1, col2, col3)
);
*/

-- -----------------------------------------------------------------------------
-- 7. MODEL Clause Basics
-- -----------------------------------------------------------------------------

-- Simple spreadsheet-style calculation
SELECT product, country, year, sales, running_total
FROM (
    SELECT 'Widget' AS product, 'USA' AS country, 2023 AS year, 1000 AS sales FROM DUAL
    UNION ALL
    SELECT 'Widget', 'USA', 2024, 1500 FROM DUAL
    UNION ALL
    SELECT 'Widget', 'UK', 2023, 800 FROM DUAL
    UNION ALL
    SELECT 'Widget', 'UK', 2024, 900 FROM DUAL
)
MODEL
    PARTITION BY (product)
    DIMENSION BY (country, year)
    MEASURES (sales, 0 AS running_total)
    RULES (
        running_total['USA', 2023] = sales['USA', 2023],
        running_total['USA', 2024] = running_total['USA', 2023] + sales['USA', 2024],
        running_total['UK', 2023] = sales['UK', 2023],
        running_total['UK', 2024] = running_total['UK', 2023] + sales['UK', 2024]
    );

-- MODEL with employee data
SELECT department_id, employee_id, salary, bonus, total_comp
FROM employees
WHERE department_id IN (60, 90)
MODEL
    PARTITION BY (department_id)
    DIMENSION BY (employee_id)
    MEASURES (salary, 0 AS bonus, 0 AS total_comp)
    RULES (
        bonus[ANY] = CASE WHEN salary[CV()] > 10000 THEN salary[CV()] * 0.1 ELSE salary[CV()] * 0.05 END,
        total_comp[ANY] = salary[CV()] + bonus[CV()]
    );

-- -----------------------------------------------------------------------------
-- 8. MODEL with Iteration
-- -----------------------------------------------------------------------------

-- Iterative calculation (compound interest)
SELECT year_num, principal, interest, balance
FROM DUAL
MODEL
    DIMENSION BY (0 AS year_num)
    MEASURES (10000 AS principal, 0 AS interest, 10000 AS balance)
    RULES ITERATE (10) (
        interest[ITERATION_NUMBER + 1] = balance[ITERATION_NUMBER] * 0.05,
        balance[ITERATION_NUMBER + 1] = balance[ITERATION_NUMBER] + interest[ITERATION_NUMBER + 1]
    );

-- Fibonacci sequence with MODEL
SELECT n, fib
FROM DUAL
MODEL
    DIMENSION BY (1 AS n)
    MEASURES (1 AS fib)
    RULES ITERATE (20) (
        fib[ITERATION_NUMBER + 1] = CASE 
            WHEN ITERATION_NUMBER < 2 THEN 1
            ELSE fib[ITERATION_NUMBER] + fib[ITERATION_NUMBER - 1]
        END
    )
ORDER BY n;

-- -----------------------------------------------------------------------------
-- 9. MODEL REFERENCE
-- -----------------------------------------------------------------------------

-- Using reference model
SELECT employee_id, first_name, department_id, salary, dept_avg
FROM employees
MODEL
    REFERENCE dept_avg_model ON (
        SELECT department_id, AVG(salary) AS avg_sal
        FROM employees
        GROUP BY department_id
    )
    DIMENSION BY (department_id AS dept_id)
    MEASURES (avg_sal)
    
    MAIN emp_model
    DIMENSION BY (employee_id)
    MEASURES (first_name, department_id, salary, 0 AS dept_avg)
    RULES (
        dept_avg[ANY] = dept_avg_model.avg_sal[department_id[CV()]]
    );

-- -----------------------------------------------------------------------------
-- 10. MODEL with Symbolic References
-- -----------------------------------------------------------------------------

-- Using CV() - Current Value
SELECT employee_id, first_name, salary, salary_rank, new_salary
FROM employees
WHERE department_id = 60
MODEL
    DIMENSION BY (ROW_NUMBER() OVER (ORDER BY salary DESC) AS salary_rank)
    MEASURES (employee_id, first_name, salary, 0 AS new_salary)
    RULES (
        new_salary[ANY] = CASE 
            WHEN salary_rank[CV()] = 1 THEN salary[CV()] * 1.10  -- Top earner: 10% raise
            WHEN salary_rank[CV()] <= 3 THEN salary[CV()] * 1.08  -- Top 3: 8% raise
            ELSE salary[CV()] * 1.05  -- Others: 5% raise
        END
    );

-- -----------------------------------------------------------------------------
-- 11. MODEL FOR Loop
-- -----------------------------------------------------------------------------

-- Generate time series
SELECT month_num, month_name, projected_value
FROM DUAL
MODEL
    DIMENSION BY (1 AS month_num)
    MEASURES (
        CAST(NULL AS VARCHAR2(20)) AS month_name,
        1000 AS projected_value
    )
    RULES (
        projected_value[FOR month_num FROM 1 TO 12 INCREMENT 1] = 
            1000 * POWER(1.05, CV(month_num) - 1),
        month_name[FOR month_num FROM 1 TO 12 INCREMENT 1] = 
            TO_CHAR(ADD_MONTHS(DATE '2024-01-01', CV(month_num) - 1), 'Month')
    )
ORDER BY month_num;

-- -----------------------------------------------------------------------------
-- 12. MODEL SEQUENTIAL vs AUTOMATIC Order
-- -----------------------------------------------------------------------------

-- Sequential order (rules execute in order)
SELECT id, val1, val2
FROM (SELECT 1 AS id, 100 AS val1, 0 AS val2 FROM DUAL)
MODEL
    DIMENSION BY (id)
    MEASURES (val1, val2)
    RULES SEQUENTIAL ORDER (
        val2[1] = val1[1] * 2,
        val1[1] = val1[1] + 50
    );
-- Result: val1 = 150, val2 = 200 (original val1)

-- Automatic order (optimized by Oracle)
SELECT id, val1, val2
FROM (SELECT 1 AS id, 100 AS val1, 0 AS val2 FROM DUAL)
MODEL
    DIMENSION BY (id)
    MEASURES (val1, val2)
    RULES AUTOMATIC ORDER (
        val2[1] = val1[1] * 2,
        val1[1] = val1[1] + 50
    );

-- -----------------------------------------------------------------------------
-- 13. Advanced PIVOT Patterns
-- -----------------------------------------------------------------------------

-- Pivot with running totals
SELECT job_id, 
       dept_50, dept_60, dept_80,
       dept_50 + NVL(dept_60, 0) + NVL(dept_80, 0) AS total
FROM (
    SELECT job_id, department_id, salary
    FROM employees
)
PIVOT (
    SUM(salary)
    FOR department_id IN (50 AS dept_50, 60 AS dept_60, 80 AS dept_80)
);

-- Pivot with percentage
WITH base_data AS (
    SELECT job_id, department_id, COUNT(*) AS cnt
    FROM employees
    GROUP BY job_id, department_id
),
total_by_job AS (
    SELECT job_id, SUM(cnt) AS total
    FROM base_data
    GROUP BY job_id
)
SELECT p.*, 
       ROUND(NVL(dept_50, 0) / t.total * 100, 1) AS pct_50,
       ROUND(NVL(dept_60, 0) / t.total * 100, 1) AS pct_60,
       ROUND(NVL(dept_80, 0) / t.total * 100, 1) AS pct_80
FROM (
    SELECT job_id, department_id, cnt
    FROM base_data
)
PIVOT (
    SUM(cnt)
    FOR department_id IN (50 AS dept_50, 60 AS dept_60, 80 AS dept_80)
) p
JOIN total_by_job t ON p.job_id = t.job_id;

-- -----------------------------------------------------------------------------
-- 14. UNPIVOT Patterns
-- -----------------------------------------------------------------------------

-- Transform row data to column data
WITH employee_attributes AS (
    SELECT employee_id,
           first_name AS attr_name,
           last_name AS attr_lastname,
           email AS attr_email,
           phone_number AS attr_phone
    FROM employees
    WHERE employee_id = 100
)
SELECT employee_id, attribute_name, attribute_value
FROM employee_attributes
UNPIVOT (
    attribute_value FOR attribute_name IN (
        attr_name AS 'First Name',
        attr_lastname AS 'Last Name',
        attr_email AS 'Email',
        attr_phone AS 'Phone'
    )
);

-- Multiple column unpivot
/*
SELECT id, metric_type, metric_date, metric_value
FROM metrics_table
UNPIVOT (
    (metric_value, metric_date) FOR metric_type IN (
        (sales_q1, date_q1) AS 'Q1',
        (sales_q2, date_q2) AS 'Q2',
        (sales_q3, date_q3) AS 'Q3',
        (sales_q4, date_q4) AS 'Q4'
    )
);
*/

-- -----------------------------------------------------------------------------
-- 15. Complex MODEL Examples
-- -----------------------------------------------------------------------------

-- Sales forecasting model
WITH sales_data AS (
    SELECT 'ProductA' AS product, 2021 AS year, 1000 AS sales FROM DUAL
    UNION ALL SELECT 'ProductA', 2022, 1200 FROM DUAL
    UNION ALL SELECT 'ProductA', 2023, 1500 FROM DUAL
    UNION ALL SELECT 'ProductB', 2021, 500 FROM DUAL
    UNION ALL SELECT 'ProductB', 2022, 600 FROM DUAL
    UNION ALL SELECT 'ProductB', 2023, 750 FROM DUAL
)
SELECT product, year, sales, growth_rate, forecast
FROM sales_data
MODEL
    PARTITION BY (product)
    DIMENSION BY (year)
    MEASURES (
        sales,
        CAST(NULL AS NUMBER) AS growth_rate,
        CAST(NULL AS NUMBER) AS forecast
    )
    RULES (
        growth_rate[year > 2021] = (sales[CV()] - sales[CV() - 1]) / sales[CV() - 1],
        forecast[2024] = sales[2023] * (1 + AVG(growth_rate)[year BETWEEN 2022 AND 2023]),
        forecast[2025] = forecast[2024] * (1 + AVG(growth_rate)[year BETWEEN 2022 AND 2023])
    )
ORDER BY product, year;

-- Allocating budgets with MODEL
WITH budget_data AS (
    SELECT 'IT' AS dept, 1000000 AS budget, 0.3 AS hw_pct, 0.5 AS sw_pct, 0.2 AS svc_pct FROM DUAL
    UNION ALL SELECT 'HR', 500000, 0.1, 0.3, 0.6 FROM DUAL
    UNION ALL SELECT 'Sales', 800000, 0.2, 0.4, 0.4 FROM DUAL
)
SELECT dept, budget, hardware, software, services, total_check
FROM budget_data
MODEL
    DIMENSION BY (dept)
    MEASURES (
        budget,
        hw_pct, sw_pct, svc_pct,
        0 AS hardware,
        0 AS software,
        0 AS services,
        0 AS total_check
    )
    RULES (
        hardware[ANY] = budget[CV()] * hw_pct[CV()],
        software[ANY] = budget[CV()] * sw_pct[CV()],
        services[ANY] = budget[CV()] * svc_pct[CV()],
        total_check[ANY] = hardware[CV()] + software[CV()] + services[CV()]
    );

