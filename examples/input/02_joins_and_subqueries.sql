-- ============================================================================
-- File: 02_joins_and_subqueries.sql
-- Description: Various JOIN types, subqueries, and correlated subqueries
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. INNER JOIN
-- -----------------------------------------------------------------------------

-- ANSI SQL syntax (recommended)
SELECT e.employee_id, e.first_name, e.last_name, d.department_name
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id;

-- Implicit join (Oracle traditional syntax)
SELECT e.employee_id, e.first_name, e.last_name, d.department_name
FROM employees e, departments d
WHERE e.department_id = d.department_id;

-- Join with multiple conditions
SELECT e.employee_id, e.first_name, j.job_title, d.department_name
FROM employees e
INNER JOIN jobs j ON e.job_id = j.job_id
INNER JOIN departments d ON e.department_id = d.department_id;

-- NATURAL JOIN (joins on all columns with same name)
SELECT employee_id, first_name, department_name
FROM employees
NATURAL JOIN departments;

-- JOIN USING clause
SELECT employee_id, first_name, department_id, department_name
FROM employees
JOIN departments USING (department_id);

-- -----------------------------------------------------------------------------
-- 2. LEFT OUTER JOIN
-- -----------------------------------------------------------------------------

-- All employees, including those without departments
SELECT e.employee_id, e.first_name, d.department_name
FROM employees e
LEFT OUTER JOIN departments d ON e.department_id = d.department_id;

-- Short syntax
SELECT e.employee_id, e.first_name, d.department_name
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id;

-- Oracle traditional syntax (+)
SELECT e.employee_id, e.first_name, d.department_name
FROM employees e, departments d
WHERE e.department_id = d.department_id(+);

-- Left join with filter
SELECT e.employee_id, e.first_name, d.department_name
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id
WHERE d.department_id IS NULL;  -- Employees without department

-- -----------------------------------------------------------------------------
-- 3. RIGHT OUTER JOIN
-- -----------------------------------------------------------------------------

-- All departments, including those without employees
SELECT e.employee_id, e.first_name, d.department_name
FROM employees e
RIGHT OUTER JOIN departments d ON e.department_id = d.department_id;

-- Oracle traditional syntax (+)
SELECT e.employee_id, e.first_name, d.department_name
FROM employees e, departments d
WHERE e.department_id(+) = d.department_id;

-- -----------------------------------------------------------------------------
-- 4. FULL OUTER JOIN
-- -----------------------------------------------------------------------------

-- All employees and all departments
SELECT e.employee_id, e.first_name, d.department_name
FROM employees e
FULL OUTER JOIN departments d ON e.department_id = d.department_id;

-- Full join with condition
SELECT e.employee_id, e.first_name, d.department_name
FROM employees e
FULL JOIN departments d ON e.department_id = d.department_id
WHERE e.employee_id IS NULL OR d.department_id IS NULL;

-- -----------------------------------------------------------------------------
-- 5. CROSS JOIN (Cartesian Product)
-- -----------------------------------------------------------------------------

-- ANSI syntax
SELECT e.first_name, d.department_name
FROM employees e
CROSS JOIN departments d;

-- Traditional syntax
SELECT e.first_name, d.department_name
FROM employees e, departments d;

-- Cross join with filter (becomes inner join)
SELECT e.first_name, d.department_name
FROM employees e
CROSS JOIN departments d
WHERE e.department_id = d.department_id;

-- -----------------------------------------------------------------------------
-- 6. SELF JOIN
-- -----------------------------------------------------------------------------

-- Employee and manager relationship
SELECT e.employee_id,
       e.first_name || ' ' || e.last_name AS employee_name,
       m.first_name || ' ' || m.last_name AS manager_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id;

-- Find employees who earn more than their manager
SELECT e.first_name || ' ' || e.last_name AS employee_name,
       e.salary AS employee_salary,
       m.first_name || ' ' || m.last_name AS manager_name,
       m.salary AS manager_salary
FROM employees e
JOIN employees m ON e.manager_id = m.employee_id
WHERE e.salary > m.salary;

-- -----------------------------------------------------------------------------
-- 7. Multiple table joins
-- -----------------------------------------------------------------------------

-- Join 4 tables
SELECT e.employee_id,
       e.first_name,
       e.last_name,
       j.job_title,
       d.department_name,
       l.city,
       c.country_name
FROM employees e
JOIN jobs j ON e.job_id = j.job_id
JOIN departments d ON e.department_id = d.department_id
JOIN locations l ON d.location_id = l.location_id
JOIN countries c ON l.country_id = c.country_id;

-- Mixed join types
SELECT e.employee_id,
       e.first_name,
       d.department_name,
       l.city
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id
LEFT JOIN locations l ON d.location_id = l.location_id;

-- -----------------------------------------------------------------------------
-- 8. Scalar Subqueries
-- -----------------------------------------------------------------------------

-- Subquery in SELECT clause
SELECT employee_id,
       first_name,
       salary,
       (SELECT AVG(salary) FROM employees) AS avg_salary,
       salary - (SELECT AVG(salary) FROM employees) AS diff_from_avg
FROM employees;

-- Subquery in WHERE clause (single value)
SELECT * FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

-- Subquery with MAX
SELECT * FROM employees
WHERE salary = (SELECT MAX(salary) FROM employees);

-- -----------------------------------------------------------------------------
-- 9. Single-row Subqueries
-- -----------------------------------------------------------------------------

-- Find employee with highest salary in department 50
SELECT * FROM employees
WHERE salary = (
    SELECT MAX(salary) FROM employees WHERE department_id = 50
);

-- Find employees hired on the same day as employee 100
SELECT * FROM employees
WHERE hire_date = (
    SELECT hire_date FROM employees WHERE employee_id = 100
);

-- Nested subqueries
SELECT * FROM employees
WHERE department_id = (
    SELECT department_id FROM departments
    WHERE location_id = (
        SELECT location_id FROM locations WHERE city = 'Seattle'
    )
);

-- -----------------------------------------------------------------------------
-- 10. Multi-row Subqueries (IN, ANY, ALL)
-- -----------------------------------------------------------------------------

-- IN operator
SELECT * FROM employees
WHERE department_id IN (
    SELECT department_id FROM departments WHERE location_id = 1700
);

-- NOT IN (be careful with NULLs)
SELECT * FROM employees
WHERE department_id NOT IN (
    SELECT department_id FROM departments 
    WHERE location_id = 1700
    AND department_id IS NOT NULL
);

-- ANY operator (= ANY is equivalent to IN)
SELECT * FROM employees
WHERE salary > ANY (
    SELECT salary FROM employees WHERE department_id = 50
);

-- ALL operator
SELECT * FROM employees
WHERE salary > ALL (
    SELECT salary FROM employees WHERE department_id = 50
);

-- < ALL (less than the minimum)
SELECT * FROM employees
WHERE salary < ALL (
    SELECT salary FROM employees WHERE department_id = 80
);

-- -----------------------------------------------------------------------------
-- 11. Correlated Subqueries
-- -----------------------------------------------------------------------------

-- Find employees who earn more than the average of their department
SELECT e.employee_id, e.first_name, e.salary, e.department_id
FROM employees e
WHERE e.salary > (
    SELECT AVG(salary) 
    FROM employees 
    WHERE department_id = e.department_id
);

-- Find employees who are managers
SELECT * FROM employees e
WHERE EXISTS (
    SELECT 1 FROM employees WHERE manager_id = e.employee_id
);

-- Find employees who are NOT managers
SELECT * FROM employees e
WHERE NOT EXISTS (
    SELECT 1 FROM employees WHERE manager_id = e.employee_id
);

-- Find departments with at least one employee earning > 10000
SELECT * FROM departments d
WHERE EXISTS (
    SELECT 1 FROM employees e 
    WHERE e.department_id = d.department_id 
    AND e.salary > 10000
);

-- -----------------------------------------------------------------------------
-- 12. Inline Views (Subquery in FROM clause)
-- -----------------------------------------------------------------------------

-- Simple inline view
SELECT * FROM (
    SELECT employee_id, first_name, salary,
           RANK() OVER (ORDER BY salary DESC) AS salary_rank
    FROM employees
)
WHERE salary_rank <= 10;

-- Multiple inline views
SELECT dept_stats.department_id,
       dept_stats.dept_avg_salary,
       emp_count.employee_count
FROM (
    SELECT department_id, AVG(salary) AS dept_avg_salary
    FROM employees
    GROUP BY department_id
) dept_stats
JOIN (
    SELECT department_id, COUNT(*) AS employee_count
    FROM employees
    GROUP BY department_id
) emp_count ON dept_stats.department_id = emp_count.department_id;

-- Inline view with ORDER BY for top-N query
SELECT * FROM (
    SELECT employee_id, first_name, salary
    FROM employees
    ORDER BY salary DESC
)
WHERE ROWNUM <= 5;

-- -----------------------------------------------------------------------------
-- 13. WITH clause (Common Table Expressions - CTE)
-- -----------------------------------------------------------------------------

-- Simple CTE
WITH high_earners AS (
    SELECT employee_id, first_name, last_name, salary
    FROM employees
    WHERE salary > 10000
)
SELECT * FROM high_earners ORDER BY salary DESC;

-- Multiple CTEs
WITH 
dept_salary AS (
    SELECT department_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department_id
),
high_salary_depts AS (
    SELECT department_id, avg_salary
    FROM dept_salary
    WHERE avg_salary > 8000
)
SELECT d.department_name, h.avg_salary
FROM high_salary_depts h
JOIN departments d ON h.department_id = d.department_id;

-- CTE referencing another CTE
WITH 
all_employees AS (
    SELECT employee_id, first_name, salary, department_id
    FROM employees
),
dept_averages AS (
    SELECT department_id, AVG(salary) AS avg_salary
    FROM all_employees
    GROUP BY department_id
)
SELECT e.first_name, e.salary, d.avg_salary,
       e.salary - d.avg_salary AS diff
FROM all_employees e
JOIN dept_averages d ON e.department_id = d.department_id;

-- Recursive CTE (for hierarchical data)
WITH org_hierarchy (employee_id, first_name, manager_id, level_num, path) AS (
    -- Anchor member
    SELECT employee_id, first_name, manager_id, 1, first_name
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    -- Recursive member
    SELECT e.employee_id, e.first_name, e.manager_id, h.level_num + 1,
           h.path || ' -> ' || e.first_name
    FROM employees e
    JOIN org_hierarchy h ON e.manager_id = h.employee_id
)
SELECT * FROM org_hierarchy ORDER BY level_num, first_name;

-- -----------------------------------------------------------------------------
-- 14. Subquery Factoring with Materialized hint
-- -----------------------------------------------------------------------------

WITH dept_summary AS (
    SELECT /*+ MATERIALIZE */ 
           department_id,
           COUNT(*) AS emp_count,
           SUM(salary) AS total_salary
    FROM employees
    GROUP BY department_id
)
SELECT d.department_name, s.emp_count, s.total_salary
FROM dept_summary s
JOIN departments d ON s.department_id = d.department_id;

-- -----------------------------------------------------------------------------
-- 15. Lateral Inline Views (Oracle 12c+)
-- -----------------------------------------------------------------------------

-- LATERAL allows correlation in inline views
SELECT d.department_name, e.employee_id, e.salary
FROM departments d,
LATERAL (
    SELECT employee_id, salary
    FROM employees
    WHERE department_id = d.department_id
    ORDER BY salary DESC
    FETCH FIRST 3 ROWS ONLY
) e;

-- CROSS APPLY (similar to LATERAL)
SELECT d.department_name, top_emp.first_name, top_emp.salary
FROM departments d
CROSS APPLY (
    SELECT first_name, salary
    FROM employees
    WHERE department_id = d.department_id
    ORDER BY salary DESC
    FETCH FIRST 1 ROW ONLY
) top_emp;

-- OUTER APPLY (like LEFT JOIN with LATERAL)
SELECT d.department_name, top_emp.first_name, top_emp.salary
FROM departments d
OUTER APPLY (
    SELECT first_name, salary
    FROM employees
    WHERE department_id = d.department_id
    ORDER BY salary DESC
    FETCH FIRST 1 ROW ONLY
) top_emp;

-- -----------------------------------------------------------------------------
-- 16. Anti-joins and Semi-joins
-- -----------------------------------------------------------------------------

-- Anti-join using NOT EXISTS (preferred)
SELECT * FROM departments d
WHERE NOT EXISTS (
    SELECT 1 FROM employees e WHERE e.department_id = d.department_id
);

-- Anti-join using NOT IN (watch for NULLs)
SELECT * FROM departments
WHERE department_id NOT IN (
    SELECT department_id FROM employees WHERE department_id IS NOT NULL
);

-- Anti-join using LEFT JOIN
SELECT d.*
FROM departments d
LEFT JOIN employees e ON d.department_id = e.department_id
WHERE e.employee_id IS NULL;

-- Semi-join using EXISTS
SELECT * FROM departments d
WHERE EXISTS (
    SELECT 1 FROM employees e WHERE e.department_id = d.department_id
);

-- Semi-join using IN
SELECT * FROM departments
WHERE department_id IN (SELECT department_id FROM employees);

-- -----------------------------------------------------------------------------
-- 17. Complex multi-level subqueries
-- -----------------------------------------------------------------------------

-- Find departments where average salary is higher than company average
SELECT d.department_name, avg_sal.avg_salary
FROM departments d
JOIN (
    SELECT department_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department_id
    HAVING AVG(salary) > (SELECT AVG(salary) FROM employees)
) avg_sal ON d.department_id = avg_sal.department_id;

-- Find employees in top 3 highest paying departments
SELECT e.*
FROM employees e
WHERE e.department_id IN (
    SELECT department_id FROM (
        SELECT department_id, AVG(salary) AS avg_sal
        FROM employees
        GROUP BY department_id
        ORDER BY avg_sal DESC
    )
    WHERE ROWNUM <= 3
);

-- Multi-level correlated subquery
SELECT e.first_name, e.department_id, e.salary,
       (SELECT COUNT(*) 
        FROM employees e2 
        WHERE e2.department_id = e.department_id 
        AND e2.salary > e.salary) AS higher_paid_in_dept
FROM employees e;

