-- ============================================================================
-- File: 09_hierarchical_queries.sql
-- Description: CONNECT BY hierarchical queries and tree traversal
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Basic Hierarchical Query
-- -----------------------------------------------------------------------------

-- Employee hierarchy (manager-employee relationship)
SELECT employee_id, first_name, last_name, manager_id, LEVEL
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- Same with indentation to show hierarchy
SELECT LEVEL,
       LPAD(' ', (LEVEL - 1) * 4) || first_name || ' ' || last_name AS employee_tree,
       job_id,
       salary
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- -----------------------------------------------------------------------------
-- 2. LEVEL Pseudo-column
-- -----------------------------------------------------------------------------

-- Using LEVEL for depth tracking
SELECT employee_id, first_name, manager_id,
       LEVEL AS depth,
       CASE LEVEL
           WHEN 1 THEN 'CEO'
           WHEN 2 THEN 'Executive'
           WHEN 3 THEN 'Senior Manager'
           WHEN 4 THEN 'Manager'
           ELSE 'Staff'
       END AS hierarchy_level
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- Filter by level
SELECT employee_id, first_name, job_id, LEVEL
FROM employees
WHERE LEVEL <= 3
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- Count by level
SELECT LEVEL, COUNT(*) AS employee_count
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id
GROUP BY LEVEL
ORDER BY LEVEL;

-- -----------------------------------------------------------------------------
-- 3. SYS_CONNECT_BY_PATH
-- -----------------------------------------------------------------------------

-- Build full path from root to node
SELECT employee_id, first_name,
       SYS_CONNECT_BY_PATH(first_name, '/') AS path
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- Path with different delimiter
SELECT employee_id, first_name,
       SYS_CONNECT_BY_PATH(first_name || ' ' || last_name, ' -> ') AS full_path
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- Path showing job hierarchy
SELECT employee_id,
       SYS_CONNECT_BY_PATH(job_id, '/') AS job_path,
       SYS_CONNECT_BY_PATH(TO_CHAR(salary), ' > ') AS salary_path
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- -----------------------------------------------------------------------------
-- 4. CONNECT_BY_ROOT
-- -----------------------------------------------------------------------------

-- Get root node value
SELECT employee_id, first_name, manager_id,
       CONNECT_BY_ROOT first_name AS root_name,
       CONNECT_BY_ROOT employee_id AS root_id,
       LEVEL
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- Multiple hierarchies (multiple roots)
SELECT employee_id, first_name, department_id,
       CONNECT_BY_ROOT first_name AS dept_head,
       LEVEL
FROM employees
START WITH manager_id IS NULL OR manager_id NOT IN (SELECT employee_id FROM employees WHERE department_id = employees.department_id)
CONNECT BY PRIOR employee_id = manager_id;

-- -----------------------------------------------------------------------------
-- 5. CONNECT_BY_ISLEAF
-- -----------------------------------------------------------------------------

-- Identify leaf nodes (employees with no subordinates)
SELECT employee_id, first_name, manager_id,
       CONNECT_BY_ISLEAF AS is_leaf,
       CASE CONNECT_BY_ISLEAF
           WHEN 1 THEN 'Leaf (No subordinates)'
           ELSE 'Branch (Has subordinates)'
       END AS node_type
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- Count leaf vs non-leaf nodes
SELECT 
    CASE CONNECT_BY_ISLEAF WHEN 1 THEN 'Leaf' ELSE 'Non-Leaf' END AS node_type,
    COUNT(*) AS count
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id
GROUP BY CONNECT_BY_ISLEAF;

-- Only leaf nodes
SELECT employee_id, first_name, job_id
FROM employees
WHERE CONNECT_BY_ISLEAF = 1
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- -----------------------------------------------------------------------------
-- 6. CONNECT_BY_ISCYCLE and NOCYCLE
-- -----------------------------------------------------------------------------

-- Detect cycles (if data has circular references)
-- This would fail without NOCYCLE if there's a cycle:
SELECT employee_id, first_name, manager_id,
       CONNECT_BY_ISCYCLE AS is_cycle
FROM employees
START WITH manager_id IS NULL
CONNECT BY NOCYCLE PRIOR employee_id = manager_id;

-- -----------------------------------------------------------------------------
-- 7. ORDER SIBLINGS BY
-- -----------------------------------------------------------------------------

-- Order within each level (siblings)
SELECT employee_id, first_name, salary, LEVEL,
       LPAD(' ', (LEVEL - 1) * 2) || first_name AS indented_name
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id
ORDER SIBLINGS BY salary DESC;

-- Order siblings alphabetically
SELECT employee_id, first_name, last_name, LEVEL,
       LPAD(' ', (LEVEL - 1) * 2) || first_name AS indented_name
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id
ORDER SIBLINGS BY last_name;

-- Multiple ordering criteria
SELECT employee_id, first_name, department_id, salary, LEVEL
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id
ORDER SIBLINGS BY department_id, salary DESC;

-- -----------------------------------------------------------------------------
-- 8. Bottom-Up Traversal
-- -----------------------------------------------------------------------------

-- Start from leaf and go up to root
SELECT employee_id, first_name, manager_id, LEVEL,
       SYS_CONNECT_BY_PATH(first_name, ' <- ') AS path_to_root
FROM employees
START WITH employee_id = 206  -- Start from a specific employee
CONNECT BY employee_id = PRIOR manager_id;

-- Find all ancestors of an employee
SELECT LEVEL - 1 AS levels_up,
       employee_id, first_name, job_id
FROM employees
START WITH employee_id = 206
CONNECT BY employee_id = PRIOR manager_id;

-- -----------------------------------------------------------------------------
-- 9. Generating Series with CONNECT BY
-- -----------------------------------------------------------------------------

-- Generate sequence of numbers
SELECT LEVEL AS n
FROM DUAL
CONNECT BY LEVEL <= 10;

-- Generate date series
SELECT TRUNC(SYSDATE) - LEVEL + 1 AS date_value
FROM DUAL
CONNECT BY LEVEL <= 30
ORDER BY date_value;

-- Generate months for a year
SELECT ADD_MONTHS(TRUNC(SYSDATE, 'YEAR'), LEVEL - 1) AS month_start,
       TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'YEAR'), LEVEL - 1), 'Month YYYY') AS month_name
FROM DUAL
CONNECT BY LEVEL <= 12;

-- Generate hours of a day
SELECT LEVEL - 1 AS hour_num,
       TO_CHAR(TRUNC(SYSDATE) + (LEVEL - 1) / 24, 'HH24:MI') AS time_slot
FROM DUAL
CONNECT BY LEVEL <= 24;

-- Generate alphabet
SELECT CHR(64 + LEVEL) AS letter
FROM DUAL
CONNECT BY LEVEL <= 26;

-- -----------------------------------------------------------------------------
-- 10. Splitting Strings with CONNECT BY
-- -----------------------------------------------------------------------------

-- Split comma-separated string
WITH data AS (
    SELECT 'apple,banana,cherry,date' AS csv_string FROM DUAL
)
SELECT LEVEL AS item_num,
       REGEXP_SUBSTR(csv_string, '[^,]+', 1, LEVEL) AS item
FROM data
CONNECT BY LEVEL <= REGEXP_COUNT(csv_string, ',') + 1;

-- Split with different delimiter
WITH data AS (
    SELECT 'one|two|three|four|five' AS pipe_string FROM DUAL
)
SELECT LEVEL AS item_num,
       REGEXP_SUBSTR(pipe_string, '[^|]+', 1, LEVEL) AS item
FROM data
CONNECT BY LEVEL <= LENGTH(pipe_string) - LENGTH(REPLACE(pipe_string, '|', '')) + 1;

-- Split employee phone numbers (if multiple)
SELECT employee_id, first_name,
       LEVEL AS phone_index,
       REGEXP_SUBSTR(phone_number, '[^.]+', 1, LEVEL) AS phone_part
FROM employees
WHERE phone_number IS NOT NULL
CONNECT BY LEVEL <= REGEXP_COUNT(phone_number, '\.') + 1
AND PRIOR employee_id = employee_id
AND PRIOR DBMS_RANDOM.VALUE IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 11. Generating Ranges and Combinations
-- -----------------------------------------------------------------------------

-- Generate all pairs
SELECT a.n AS num1, b.n AS num2
FROM (SELECT LEVEL AS n FROM DUAL CONNECT BY LEVEL <= 5) a
CROSS JOIN (SELECT LEVEL AS n FROM DUAL CONNECT BY LEVEL <= 5) b
WHERE a.n < b.n;

-- Generate multiplication table
SELECT a.n AS a, b.n AS b, a.n * b.n AS product
FROM (SELECT LEVEL AS n FROM DUAL CONNECT BY LEVEL <= 10) a
CROSS JOIN (SELECT LEVEL AS n FROM DUAL CONNECT BY LEVEL <= 10) b
ORDER BY a.n, b.n;

-- Generate week dates for a month
SELECT LEVEL AS week_num,
       TRUNC(SYSDATE, 'MM') + (LEVEL - 1) * 7 AS week_start,
       LEAST(TRUNC(SYSDATE, 'MM') + LEVEL * 7 - 1, LAST_DAY(SYSDATE)) AS week_end
FROM DUAL
CONNECT BY TRUNC(SYSDATE, 'MM') + (LEVEL - 1) * 7 <= LAST_DAY(SYSDATE);

-- -----------------------------------------------------------------------------
-- 12. Complex Hierarchical Queries
-- -----------------------------------------------------------------------------

-- Aggregation over hierarchy (sum of salaries in subtree)
SELECT employee_id, first_name, salary, LEVEL,
       (SELECT SUM(e2.salary) 
        FROM employees e2
        START WITH e2.employee_id = e1.employee_id
        CONNECT BY PRIOR e2.employee_id = e2.manager_id) AS subtree_salary
FROM employees e1
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- Find depth of each node
WITH emp_hierarchy AS (
    SELECT employee_id, first_name, manager_id, LEVEL AS depth
    FROM employees
    START WITH manager_id IS NULL
    CONNECT BY PRIOR employee_id = manager_id
)
SELECT e.employee_id, e.first_name, h.depth
FROM employees e
JOIN emp_hierarchy h ON e.employee_id = h.employee_id;

-- Find all descendants count
SELECT e1.employee_id, e1.first_name,
       (SELECT COUNT(*) - 1
        FROM employees e2
        START WITH e2.employee_id = e1.employee_id
        CONNECT BY PRIOR e2.employee_id = e2.manager_id) AS descendants_count
FROM employees e1
START WITH e1.manager_id IS NULL
CONNECT BY PRIOR e1.employee_id = e1.manager_id;

-- Check if one employee reports to another (anywhere in hierarchy)
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM employees
            START WITH employee_id = 206
            CONNECT BY PRIOR manager_id = employee_id
            AND employee_id = 100
        ) THEN 'Yes'
        ELSE 'No'
    END AS reports_to_100
FROM DUAL;

-- Build org chart data
SELECT employee_id, 
       first_name || ' ' || last_name AS name,
       job_id,
       manager_id,
       department_id,
       LEVEL,
       CONNECT_BY_ISLEAF AS is_leaf,
       CONNECT_BY_ROOT employee_id AS root_id,
       SYS_CONNECT_BY_PATH(employee_id, '/') AS id_path,
       RPAD('.', LEVEL * 2, '.') || first_name AS tree_display
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id
ORDER SIBLINGS BY last_name;

-- -----------------------------------------------------------------------------
-- 13. Recursive CTE Alternative (Oracle 11g R2+)
-- -----------------------------------------------------------------------------

-- Recursive CTE for hierarchy (alternative to CONNECT BY)
WITH emp_hierarchy (employee_id, first_name, manager_id, level_num, path) AS (
    -- Anchor member (root nodes)
    SELECT employee_id, first_name, manager_id, 1, 
           CAST(first_name AS VARCHAR2(4000))
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive member
    SELECT e.employee_id, e.first_name, e.manager_id, h.level_num + 1,
           h.path || ' > ' || e.first_name
    FROM employees e
    JOIN emp_hierarchy h ON e.manager_id = h.employee_id
)
SELECT employee_id, first_name, level_num, path
FROM emp_hierarchy
ORDER BY path;

-- Recursive CTE with aggregation
WITH RECURSIVE dept_totals (employee_id, first_name, salary, manager_id, level_num) AS (
    SELECT employee_id, first_name, salary, manager_id, 1
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    SELECT e.employee_id, e.first_name, e.salary, e.manager_id, d.level_num + 1
    FROM employees e
    JOIN dept_totals d ON e.manager_id = d.employee_id
)
SELECT level_num, COUNT(*) AS emp_count, SUM(salary) AS total_salary
FROM dept_totals
GROUP BY level_num
ORDER BY level_num;

-- -----------------------------------------------------------------------------
-- 14. Bill of Materials (BOM) Example
-- -----------------------------------------------------------------------------

-- Create sample BOM structure
-- Typical use case: parts containing other parts

/*
-- Sample table structure:
CREATE TABLE parts (
    part_id NUMBER PRIMARY KEY,
    part_name VARCHAR2(100),
    parent_part_id NUMBER,
    quantity NUMBER
);

-- Query BOM hierarchy
SELECT part_id, part_name, parent_part_id, quantity,
       LEVEL AS assembly_level,
       LPAD(' ', (LEVEL - 1) * 2) || part_name AS indented_name,
       SYS_CONNECT_BY_PATH(part_name, ' / ') AS assembly_path
FROM parts
START WITH parent_part_id IS NULL
CONNECT BY PRIOR part_id = parent_part_id;

-- Calculate total quantity needed (exploded BOM)
SELECT part_id, part_name,
       LEVEL,
       quantity,
       quantity * CONNECT_BY_ROOT 1 AS qty_multiplier,
       -- Total quantity = product of all quantities in path
       EXP(SUM(LN(quantity)) OVER (
           PARTITION BY CONNECT_BY_ROOT part_id
           ORDER BY LEVEL
           ROWS UNBOUNDED PRECEDING
       )) AS total_qty_needed
FROM parts
START WITH parent_part_id IS NULL
CONNECT BY PRIOR part_id = parent_part_id;
*/

-- -----------------------------------------------------------------------------
-- 15. Finding Paths and Cycles
-- -----------------------------------------------------------------------------

-- Find path between two nodes
WITH path_finder AS (
    SELECT employee_id, manager_id,
           SYS_CONNECT_BY_PATH(TO_CHAR(employee_id), ',') AS path,
           LEVEL AS depth
    FROM employees
    START WITH employee_id = 206
    CONNECT BY employee_id = PRIOR manager_id
)
SELECT path, depth
FROM path_finder
WHERE employee_id = 100;

-- All paths from any node to root
SELECT employee_id, first_name,
       SYS_CONNECT_BY_PATH(first_name, ' -> ') AS path_to_root,
       LEVEL AS distance_from_root
FROM employees
CONNECT BY PRIOR manager_id = employee_id
START WITH CONNECT_BY_ISLEAF = 1;

-- Common ancestor query
WITH ancestors AS (
    SELECT employee_id AS node_id, 
           PRIOR employee_id AS ancestor_id,
           LEVEL AS distance
    FROM employees
    START WITH employee_id IN (206, 107)  -- Two employees to compare
    CONNECT BY employee_id = PRIOR manager_id
)
SELECT a1.node_id AS emp1, a2.node_id AS emp2, a1.ancestor_id AS common_ancestor
FROM ancestors a1
JOIN ancestors a2 ON a1.ancestor_id = a2.ancestor_id
WHERE a1.node_id < a2.node_id;

