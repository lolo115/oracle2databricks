-- ============================================================================
-- File: 12_ddl_operations.sql
-- Description: CREATE, ALTER, DROP statements for tables, indexes, constraints
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. CREATE TABLE - Basic
-- -----------------------------------------------------------------------------

-- Simple table
CREATE TABLE employees_new (
    employee_id    NUMBER(6),
    first_name     VARCHAR2(20),
    last_name      VARCHAR2(25),
    email          VARCHAR2(25),
    phone_number   VARCHAR2(20),
    hire_date      DATE,
    job_id         VARCHAR2(10),
    salary         NUMBER(8,2),
    commission_pct NUMBER(2,2),
    manager_id     NUMBER(6),
    department_id  NUMBER(4)
);

-- Table with constraints inline
CREATE TABLE employees_with_constraints (
    employee_id    NUMBER(6) PRIMARY KEY,
    first_name     VARCHAR2(20) NOT NULL,
    last_name      VARCHAR2(25) NOT NULL,
    email          VARCHAR2(25) UNIQUE NOT NULL,
    hire_date      DATE DEFAULT SYSDATE NOT NULL,
    job_id         VARCHAR2(10) NOT NULL,
    salary         NUMBER(8,2) CHECK (salary > 0),
    department_id  NUMBER(4) REFERENCES departments(department_id)
);

-- Table with out-of-line constraints
CREATE TABLE employees_detailed (
    employee_id    NUMBER(6),
    first_name     VARCHAR2(20),
    last_name      VARCHAR2(25),
    email          VARCHAR2(25),
    hire_date      DATE DEFAULT SYSDATE,
    job_id         VARCHAR2(10),
    salary         NUMBER(8,2),
    manager_id     NUMBER(6),
    department_id  NUMBER(4),
    
    CONSTRAINT emp_det_pk PRIMARY KEY (employee_id),
    CONSTRAINT emp_det_email_uk UNIQUE (email),
    CONSTRAINT emp_det_salary_ck CHECK (salary > 0 AND salary < 100000),
    CONSTRAINT emp_det_job_fk FOREIGN KEY (job_id) REFERENCES jobs(job_id),
    CONSTRAINT emp_det_dept_fk FOREIGN KEY (department_id) 
        REFERENCES departments(department_id) ON DELETE SET NULL,
    CONSTRAINT emp_det_mgr_fk FOREIGN KEY (manager_id) 
        REFERENCES employees_detailed(employee_id)
);

-- -----------------------------------------------------------------------------
-- 2. CREATE TABLE AS SELECT (CTAS)
-- -----------------------------------------------------------------------------

-- Simple CTAS
CREATE TABLE emp_backup AS
SELECT * FROM employees;

-- CTAS with specific columns
CREATE TABLE emp_summary AS
SELECT employee_id, first_name || ' ' || last_name AS full_name, salary
FROM employees;

-- CTAS with transformation
CREATE TABLE dept_statistics AS
SELECT department_id,
       COUNT(*) AS emp_count,
       SUM(salary) AS total_salary,
       AVG(salary) AS avg_salary,
       MIN(salary) AS min_salary,
       MAX(salary) AS max_salary
FROM employees
GROUP BY department_id;

-- CTAS with no data (structure only)
CREATE TABLE emp_template AS
SELECT * FROM employees WHERE 1 = 0;

-- CTAS with parallel and nologging
CREATE TABLE emp_archive
NOLOGGING
PARALLEL 4
AS SELECT * FROM employees WHERE hire_date < DATE '2000-01-01';

-- -----------------------------------------------------------------------------
-- 3. Data Types
-- -----------------------------------------------------------------------------

CREATE TABLE data_types_example (
    -- Numeric types
    col_number       NUMBER,
    col_number_p     NUMBER(10),
    col_number_ps    NUMBER(10,2),
    col_integer      INTEGER,
    col_float        FLOAT,
    col_binary_float BINARY_FLOAT,
    col_binary_double BINARY_DOUBLE,
    
    -- Character types
    col_char         CHAR(10),
    col_varchar2     VARCHAR2(100),
    col_nchar        NCHAR(10),
    col_nvarchar2    NVARCHAR2(100),
    col_clob         CLOB,
    col_nclob        NCLOB,
    
    -- Date/Time types
    col_date         DATE,
    col_timestamp    TIMESTAMP,
    col_timestamp_tz TIMESTAMP WITH TIME ZONE,
    col_timestamp_ltz TIMESTAMP WITH LOCAL TIME ZONE,
    col_interval_ym  INTERVAL YEAR TO MONTH,
    col_interval_ds  INTERVAL DAY TO SECOND,
    
    -- Binary types
    col_blob         BLOB,
    col_raw          RAW(100),
    col_long_raw     LONG RAW,
    
    -- Other types
    col_rowid        ROWID,
    col_urowid       UROWID,
    col_xmltype      XMLTYPE,
    col_bfile        BFILE
);

-- -----------------------------------------------------------------------------
-- 4. CREATE TABLE with Storage and Options
-- -----------------------------------------------------------------------------

-- Table with tablespace
CREATE TABLE emp_archive (
    employee_id NUMBER(6),
    archive_date DATE,
    salary NUMBER(8,2)
)
TABLESPACE users
STORAGE (INITIAL 64K NEXT 64K);

-- Table with compression
CREATE TABLE emp_compressed (
    employee_id NUMBER(6),
    data CLOB
)
ROW STORE COMPRESS ADVANCED;

-- Table with LOB storage
CREATE TABLE documents (
    doc_id NUMBER PRIMARY KEY,
    doc_name VARCHAR2(100),
    doc_content CLOB
)
LOB (doc_content) STORE AS SECUREFILE (
    TABLESPACE users
    COMPRESS HIGH
    DEDUPLICATE
);

-- -----------------------------------------------------------------------------
-- 5. Temporary Tables
-- -----------------------------------------------------------------------------

-- Global temporary table (transaction level)
CREATE GLOBAL TEMPORARY TABLE temp_emp_trans (
    employee_id NUMBER,
    calc_value NUMBER
)
ON COMMIT DELETE ROWS;

-- Global temporary table (session level)
CREATE GLOBAL TEMPORARY TABLE temp_emp_session (
    employee_id NUMBER,
    session_data VARCHAR2(100)
)
ON COMMIT PRESERVE ROWS;

-- Private temporary table (Oracle 18c+)
CREATE PRIVATE TEMPORARY TABLE ora$ptt_temp_data (
    id NUMBER,
    value VARCHAR2(100)
)
ON COMMIT DROP DEFINITION;

-- -----------------------------------------------------------------------------
-- 6. CREATE INDEX
-- -----------------------------------------------------------------------------

-- Simple index
CREATE INDEX emp_last_name_idx ON employees(last_name);

-- Composite index
CREATE INDEX emp_name_idx ON employees(last_name, first_name);

-- Unique index
CREATE UNIQUE INDEX emp_email_uk_idx ON employees(email);

-- Function-based index
CREATE INDEX emp_upper_name_idx ON employees(UPPER(last_name));

-- Descending index
CREATE INDEX emp_salary_desc_idx ON employees(salary DESC);

-- Index with NULLS FIRST/LAST
CREATE INDEX emp_comm_idx ON employees(commission_pct NULLS FIRST);

-- Bitmap index
CREATE BITMAP INDEX emp_job_bmp_idx ON employees(job_id);

-- Compressed index
CREATE INDEX emp_dept_idx ON employees(department_id) COMPRESS;

-- Partial index (Oracle 12c+)
CREATE INDEX emp_active_idx ON employees(employee_id)
WHERE status = 'ACTIVE';

-- Invisible index
CREATE INDEX emp_hire_date_idx ON employees(hire_date) INVISIBLE;

-- Reverse key index
CREATE INDEX emp_id_rev_idx ON employees(employee_id) REVERSE;

-- Index organized table (IOT)
CREATE TABLE emp_iot (
    employee_id NUMBER PRIMARY KEY,
    first_name VARCHAR2(20),
    last_name VARCHAR2(25)
)
ORGANIZATION INDEX;

-- -----------------------------------------------------------------------------
-- 7. ALTER TABLE
-- -----------------------------------------------------------------------------

-- Add column
ALTER TABLE employees ADD (middle_name VARCHAR2(20));

-- Add multiple columns
ALTER TABLE employees ADD (
    nickname VARCHAR2(20),
    birth_date DATE,
    gender CHAR(1)
);

-- Modify column
ALTER TABLE employees MODIFY (first_name VARCHAR2(50));

-- Modify multiple columns
ALTER TABLE employees MODIFY (
    first_name VARCHAR2(50),
    last_name VARCHAR2(50)
);

-- Modify column to NOT NULL
ALTER TABLE employees MODIFY (email NOT NULL);

-- Drop column
ALTER TABLE employees DROP COLUMN middle_name;

-- Drop multiple columns
ALTER TABLE employees DROP (nickname, birth_date);

-- Set column unused (faster than drop)
ALTER TABLE employees SET UNUSED COLUMN gender;

-- Drop unused columns
ALTER TABLE employees DROP UNUSED COLUMNS;

-- Rename column
ALTER TABLE employees RENAME COLUMN phone_number TO contact_phone;

-- Add default value
ALTER TABLE employees MODIFY (hire_date DEFAULT SYSDATE);

-- Add constraint
ALTER TABLE employees ADD CONSTRAINT emp_salary_min CHECK (salary >= 1000);

-- Add foreign key
ALTER TABLE employees ADD CONSTRAINT emp_dept_fk 
    FOREIGN KEY (department_id) REFERENCES departments(department_id);

-- Drop constraint
ALTER TABLE employees DROP CONSTRAINT emp_salary_min;

-- Disable constraint
ALTER TABLE employees DISABLE CONSTRAINT emp_dept_fk;

-- Enable constraint
ALTER TABLE employees ENABLE CONSTRAINT emp_dept_fk;

-- Enable constraint with NOVALIDATE
ALTER TABLE employees ENABLE NOVALIDATE CONSTRAINT emp_dept_fk;

-- Rename table
ALTER TABLE employees RENAME TO staff;

-- Move table to different tablespace
ALTER TABLE employees MOVE TABLESPACE users;

-- Enable row movement (for partitioned tables)
ALTER TABLE employees ENABLE ROW MOVEMENT;

-- Add identity column (Oracle 12c+)
ALTER TABLE employees ADD (row_id NUMBER GENERATED ALWAYS AS IDENTITY);

-- Modify to identity column
ALTER TABLE employees MODIFY (employee_id NUMBER GENERATED BY DEFAULT AS IDENTITY);

-- -----------------------------------------------------------------------------
-- 8. DROP Statements
-- -----------------------------------------------------------------------------

-- Drop table
DROP TABLE temp_employees;

-- Drop table with cascade constraints
DROP TABLE parent_table CASCADE CONSTRAINTS;

-- Drop table and purge from recycle bin
DROP TABLE temp_employees PURGE;

-- Drop index
DROP INDEX emp_last_name_idx;

-- Drop constraint
ALTER TABLE employees DROP CONSTRAINT emp_salary_ck;

-- Drop primary key
ALTER TABLE employees DROP PRIMARY KEY;

-- Drop unique constraint
ALTER TABLE employees DROP UNIQUE (email);

-- -----------------------------------------------------------------------------
-- 9. CREATE VIEW
-- -----------------------------------------------------------------------------

-- Simple view
CREATE VIEW emp_view AS
SELECT employee_id, first_name, last_name, department_id
FROM employees;

-- View with column aliases
CREATE VIEW emp_summary_view (emp_id, full_name, dept_id) AS
SELECT employee_id, first_name || ' ' || last_name, department_id
FROM employees;

-- View with WHERE clause
CREATE VIEW emp_it_view AS
SELECT * FROM employees
WHERE department_id = 60;

-- View with CHECK OPTION
CREATE VIEW emp_dept50_view AS
SELECT * FROM employees
WHERE department_id = 50
WITH CHECK OPTION;

-- View with READ ONLY
CREATE VIEW emp_readonly_view AS
SELECT employee_id, first_name, salary
FROM employees
WITH READ ONLY;

-- Complex view with joins
CREATE VIEW emp_dept_view AS
SELECT e.employee_id, 
       e.first_name || ' ' || e.last_name AS employee_name,
       e.salary,
       d.department_name,
       l.city
FROM employees e
JOIN departments d ON e.department_id = d.department_id
JOIN locations l ON d.location_id = l.location_id;

-- Materialized view
CREATE MATERIALIZED VIEW emp_mv
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT department_id, COUNT(*) AS emp_count, AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id;

-- Materialized view with fast refresh
CREATE MATERIALIZED VIEW LOG ON employees
WITH PRIMARY KEY, ROWID, SEQUENCE (department_id, salary)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW emp_stats_mv
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS
SELECT department_id, 
       COUNT(*) AS emp_count, 
       SUM(salary) AS total_salary
FROM employees
GROUP BY department_id;

-- -----------------------------------------------------------------------------
-- 10. CREATE SEQUENCE
-- -----------------------------------------------------------------------------

-- Simple sequence
CREATE SEQUENCE emp_seq;

-- Sequence with options
CREATE SEQUENCE emp_id_seq
    START WITH 1000
    INCREMENT BY 1
    MINVALUE 1000
    MAXVALUE 999999999
    NOCYCLE
    CACHE 20;

-- Sequence for descending values
CREATE SEQUENCE desc_seq
    START WITH 1000
    INCREMENT BY -1
    MAXVALUE 1000
    MINVALUE 1
    CYCLE;

-- Alter sequence
ALTER SEQUENCE emp_id_seq INCREMENT BY 10;
ALTER SEQUENCE emp_id_seq CACHE 50;
ALTER SEQUENCE emp_id_seq RESTART START WITH 5000;

-- -----------------------------------------------------------------------------
-- 11. CREATE SYNONYM
-- -----------------------------------------------------------------------------

-- Private synonym
CREATE SYNONYM emp FOR hr.employees;

-- Public synonym
CREATE PUBLIC SYNONYM employees FOR hr.employees;

-- Synonym for remote object
CREATE SYNONYM remote_emp FOR employees@remote_db;

-- Drop synonym
DROP SYNONYM emp;
DROP PUBLIC SYNONYM employees;

-- -----------------------------------------------------------------------------
-- 12. Partitioned Tables
-- -----------------------------------------------------------------------------

-- Range partitioning
CREATE TABLE sales_range (
    sale_id NUMBER,
    sale_date DATE,
    amount NUMBER
)
PARTITION BY RANGE (sale_date) (
    PARTITION sales_q1_2024 VALUES LESS THAN (DATE '2024-04-01'),
    PARTITION sales_q2_2024 VALUES LESS THAN (DATE '2024-07-01'),
    PARTITION sales_q3_2024 VALUES LESS THAN (DATE '2024-10-01'),
    PARTITION sales_q4_2024 VALUES LESS THAN (DATE '2025-01-01'),
    PARTITION sales_future VALUES LESS THAN (MAXVALUE)
);

-- List partitioning
CREATE TABLE emp_by_region (
    employee_id NUMBER,
    region VARCHAR2(20),
    salary NUMBER
)
PARTITION BY LIST (region) (
    PARTITION p_east VALUES ('New York', 'Boston', 'Miami'),
    PARTITION p_west VALUES ('Los Angeles', 'San Francisco', 'Seattle'),
    PARTITION p_central VALUES ('Chicago', 'Dallas', 'Denver'),
    PARTITION p_other VALUES (DEFAULT)
);

-- Hash partitioning
CREATE TABLE emp_hash (
    employee_id NUMBER,
    first_name VARCHAR2(20)
)
PARTITION BY HASH (employee_id)
PARTITIONS 8;

-- Composite partitioning (Range-List)
CREATE TABLE sales_composite (
    sale_id NUMBER,
    sale_date DATE,
    region VARCHAR2(20),
    amount NUMBER
)
PARTITION BY RANGE (sale_date)
SUBPARTITION BY LIST (region)
(
    PARTITION sales_2024 VALUES LESS THAN (DATE '2025-01-01') (
        SUBPARTITION sales_2024_east VALUES ('East'),
        SUBPARTITION sales_2024_west VALUES ('West')
    ),
    PARTITION sales_2025 VALUES LESS THAN (DATE '2026-01-01') (
        SUBPARTITION sales_2025_east VALUES ('East'),
        SUBPARTITION sales_2025_west VALUES ('West')
    )
);

-- Interval partitioning
CREATE TABLE sales_interval (
    sale_id NUMBER,
    sale_date DATE,
    amount NUMBER
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
    PARTITION p_initial VALUES LESS THAN (DATE '2024-01-01')
);

-- Add partition
ALTER TABLE sales_range ADD PARTITION sales_q1_2025 
    VALUES LESS THAN (DATE '2025-04-01');

-- Drop partition
ALTER TABLE sales_range DROP PARTITION sales_q1_2024;

-- Truncate partition
ALTER TABLE sales_range TRUNCATE PARTITION sales_q2_2024;

-- Split partition
ALTER TABLE sales_range SPLIT PARTITION sales_future AT (DATE '2026-01-01')
INTO (PARTITION sales_2025, PARTITION sales_future);

-- Merge partitions
ALTER TABLE sales_range MERGE PARTITIONS sales_q1_2024, sales_q2_2024
INTO PARTITION sales_h1_2024;

-- Exchange partition
ALTER TABLE sales_range EXCHANGE PARTITION sales_q1_2024 
WITH TABLE sales_q1_staging;

-- -----------------------------------------------------------------------------
-- 13. Comments
-- -----------------------------------------------------------------------------

-- Comment on table
COMMENT ON TABLE employees IS 'Contains employee information';

-- Comment on column
COMMENT ON COLUMN employees.salary IS 'Monthly salary in USD';

-- Comment on view
COMMENT ON TABLE emp_view IS 'Simplified employee view';

-- Query comments
SELECT * FROM user_tab_comments WHERE table_name = 'EMPLOYEES';
SELECT * FROM user_col_comments WHERE table_name = 'EMPLOYEES';

