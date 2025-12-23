-- ============================================================================
-- File: 20_plsql_dynamic_sql.sql
-- Description: Dynamic SQL with EXECUTE IMMEDIATE and DBMS_SQL
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Basic EXECUTE IMMEDIATE
-- -----------------------------------------------------------------------------

DECLARE
    v_sql VARCHAR2(1000);
    v_count NUMBER;
    v_name VARCHAR2(100);
BEGIN
    -- Simple DDL
    EXECUTE IMMEDIATE 'CREATE TABLE temp_test (id NUMBER, name VARCHAR2(100))';
    
    -- DDL with variable
    v_sql := 'DROP TABLE temp_test';
    EXECUTE IMMEDIATE v_sql;
    
    -- Simple query INTO
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM employees' INTO v_count;
    DBMS_OUTPUT.PUT_LINE('Employee count: ' || v_count);
    
    -- Query with WHERE (using concatenation - NOT recommended for production)
    v_sql := 'SELECT first_name FROM employees WHERE employee_id = 100';
    EXECUTE IMMEDIATE v_sql INTO v_name;
    DBMS_OUTPUT.PUT_LINE('Name: ' || v_name);
END;
/

-- -----------------------------------------------------------------------------
-- 2. EXECUTE IMMEDIATE with Bind Variables
-- -----------------------------------------------------------------------------

DECLARE
    v_sql VARCHAR2(1000);
    v_emp_id NUMBER := 100;
    v_dept_id NUMBER := 60;
    v_min_salary NUMBER := 5000;
    v_count NUMBER;
    v_name VARCHAR2(100);
    v_salary NUMBER;
BEGIN
    -- Single bind variable
    v_sql := 'SELECT first_name FROM employees WHERE employee_id = :id';
    EXECUTE IMMEDIATE v_sql INTO v_name USING v_emp_id;
    DBMS_OUTPUT.PUT_LINE('Employee: ' || v_name);
    
    -- Multiple bind variables
    v_sql := 'SELECT COUNT(*) FROM employees WHERE department_id = :dept AND salary > :sal';
    EXECUTE IMMEDIATE v_sql INTO v_count USING v_dept_id, v_min_salary;
    DBMS_OUTPUT.PUT_LINE('Count: ' || v_count);
    
    -- Named bind variables (must be referenced in order)
    v_sql := 'SELECT first_name, salary FROM employees WHERE employee_id = :emp_id';
    EXECUTE IMMEDIATE v_sql INTO v_name, v_salary USING v_emp_id;
    DBMS_OUTPUT.PUT_LINE(v_name || ': $' || v_salary);
    
    -- Same bind variable used multiple times
    v_sql := 'SELECT COUNT(*) FROM employees WHERE salary > :val OR commission_pct > :val/10000';
    EXECUTE IMMEDIATE v_sql INTO v_count USING v_min_salary, v_min_salary;
    DBMS_OUTPUT.PUT_LINE('Count: ' || v_count);
END;
/

-- -----------------------------------------------------------------------------
-- 3. EXECUTE IMMEDIATE with DML
-- -----------------------------------------------------------------------------

DECLARE
    v_sql VARCHAR2(1000);
    v_emp_id NUMBER := 300;
    v_rows NUMBER;
BEGIN
    -- INSERT
    v_sql := 'INSERT INTO temp_employees (employee_id, first_name, last_name) 
              VALUES (:1, :2, :3)';
    EXECUTE IMMEDIATE v_sql USING v_emp_id, 'John', 'Doe';
    DBMS_OUTPUT.PUT_LINE('Inserted ' || SQL%ROWCOUNT || ' row(s)');
    
    -- UPDATE
    v_sql := 'UPDATE temp_employees SET first_name = :name WHERE employee_id = :id';
    EXECUTE IMMEDIATE v_sql USING 'Jane', v_emp_id;
    v_rows := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('Updated ' || v_rows || ' row(s)');
    
    -- DELETE
    v_sql := 'DELETE FROM temp_employees WHERE employee_id = :id';
    EXECUTE IMMEDIATE v_sql USING v_emp_id;
    DBMS_OUTPUT.PUT_LINE('Deleted ' || SQL%ROWCOUNT || ' row(s)');
    
    ROLLBACK;
END;
/

-- -----------------------------------------------------------------------------
-- 4. EXECUTE IMMEDIATE with OUT Parameters
-- -----------------------------------------------------------------------------

DECLARE
    v_sql VARCHAR2(1000);
    v_emp_id NUMBER := 100;
    v_name VARCHAR2(100);
    v_salary NUMBER;
    v_new_id NUMBER;
BEGIN
    -- Multiple OUT values
    v_sql := 'SELECT first_name, salary FROM employees WHERE employee_id = :id';
    EXECUTE IMMEDIATE v_sql INTO v_name, v_salary USING v_emp_id;
    DBMS_OUTPUT.PUT_LINE(v_name || ': $' || v_salary);
    
    -- RETURNING clause
    v_sql := 'UPDATE employees SET salary = salary * 1.1 
              WHERE employee_id = :id RETURNING salary INTO :new_sal';
    EXECUTE IMMEDIATE v_sql USING v_emp_id RETURNING INTO v_salary;
    DBMS_OUTPUT.PUT_LINE('New salary: $' || v_salary);
    
    -- INSERT with RETURNING
    v_sql := 'INSERT INTO temp_employees (employee_id, first_name) 
              VALUES (temp_seq.NEXTVAL, :name) RETURNING employee_id INTO :id';
    EXECUTE IMMEDIATE v_sql USING 'Test' RETURNING INTO v_new_id;
    DBMS_OUTPUT.PUT_LINE('New ID: ' || v_new_id);
    
    ROLLBACK;
END;
/

-- -----------------------------------------------------------------------------
-- 5. EXECUTE IMMEDIATE with BULK COLLECT
-- -----------------------------------------------------------------------------

DECLARE
    v_sql VARCHAR2(1000);
    TYPE t_names IS TABLE OF VARCHAR2(100);
    TYPE t_salaries IS TABLE OF NUMBER;
    TYPE t_emp_rec IS TABLE OF employees%ROWTYPE;
    
    v_names t_names;
    v_salaries t_salaries;
    v_employees t_emp_rec;
    v_dept_id NUMBER := 60;
BEGIN
    -- BULK COLLECT into multiple collections
    v_sql := 'SELECT first_name, salary FROM employees WHERE department_id = :dept';
    EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_names, v_salaries USING v_dept_id;
    
    DBMS_OUTPUT.PUT_LINE('Found ' || v_names.COUNT || ' employees');
    FOR i IN 1..v_names.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_names(i) || ': $' || v_salaries(i));
    END LOOP;
    
    -- BULK COLLECT into ROWTYPE collection
    v_sql := 'SELECT * FROM employees WHERE salary > :min_sal';
    EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_employees USING 10000;
    
    DBMS_OUTPUT.PUT_LINE('High earners: ' || v_employees.COUNT);
END;
/

-- -----------------------------------------------------------------------------
-- 6. Dynamic Cursor with OPEN FOR
-- -----------------------------------------------------------------------------

DECLARE
    v_sql VARCHAR2(1000);
    v_cursor SYS_REFCURSOR;
    v_emp_id NUMBER;
    v_name VARCHAR2(100);
    v_salary NUMBER;
    v_dept_id NUMBER := 60;
BEGIN
    -- Open cursor with dynamic SQL
    v_sql := 'SELECT employee_id, first_name, salary 
              FROM employees 
              WHERE department_id = :dept 
              ORDER BY salary DESC';
    
    OPEN v_cursor FOR v_sql USING v_dept_id;
    
    LOOP
        FETCH v_cursor INTO v_emp_id, v_name, v_salary;
        EXIT WHEN v_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE(v_emp_id || ': ' || v_name || ' - $' || v_salary);
    END LOOP;
    
    CLOSE v_cursor;
END;
/

-- Dynamic cursor with BULK COLLECT
DECLARE
    v_sql VARCHAR2(1000);
    v_cursor SYS_REFCURSOR;
    TYPE t_emp_rec IS RECORD (emp_id NUMBER, emp_name VARCHAR2(100), salary NUMBER);
    TYPE t_emp_tab IS TABLE OF t_emp_rec;
    v_employees t_emp_tab;
BEGIN
    v_sql := 'SELECT employee_id, first_name, salary FROM employees WHERE ROWNUM <= :n';
    
    OPEN v_cursor FOR v_sql USING 10;
    FETCH v_cursor BULK COLLECT INTO v_employees;
    CLOSE v_cursor;
    
    FOR i IN 1..v_employees.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_employees(i).emp_id || ': ' || v_employees(i).emp_name);
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- 7. Building Dynamic SQL
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE search_employees(
    p_first_name  VARCHAR2 DEFAULT NULL,
    p_last_name   VARCHAR2 DEFAULT NULL,
    p_department  NUMBER DEFAULT NULL,
    p_min_salary  NUMBER DEFAULT NULL,
    p_max_salary  NUMBER DEFAULT NULL,
    p_result      OUT SYS_REFCURSOR
)
IS
    v_sql VARCHAR2(4000);
    v_where VARCHAR2(4000) := '';
BEGIN
    v_sql := 'SELECT employee_id, first_name, last_name, department_id, salary FROM employees WHERE 1=1';
    
    -- Build WHERE clause dynamically
    IF p_first_name IS NOT NULL THEN
        v_where := v_where || ' AND UPPER(first_name) LIKE UPPER(''' || p_first_name || '%'')';
    END IF;
    
    IF p_last_name IS NOT NULL THEN
        v_where := v_where || ' AND UPPER(last_name) LIKE UPPER(''' || p_last_name || '%'')';
    END IF;
    
    IF p_department IS NOT NULL THEN
        v_where := v_where || ' AND department_id = ' || p_department;
    END IF;
    
    IF p_min_salary IS NOT NULL THEN
        v_where := v_where || ' AND salary >= ' || p_min_salary;
    END IF;
    
    IF p_max_salary IS NOT NULL THEN
        v_where := v_where || ' AND salary <= ' || p_max_salary;
    END IF;
    
    v_sql := v_sql || v_where || ' ORDER BY last_name';
    
    DBMS_OUTPUT.PUT_LINE('SQL: ' || v_sql);
    
    OPEN p_result FOR v_sql;
END search_employees;
/

-- Better version with bind variables
CREATE OR REPLACE PROCEDURE search_employees_safe(
    p_first_name  VARCHAR2 DEFAULT NULL,
    p_last_name   VARCHAR2 DEFAULT NULL,
    p_department  NUMBER DEFAULT NULL,
    p_min_salary  NUMBER DEFAULT NULL,
    p_result      OUT SYS_REFCURSOR
)
IS
    v_sql VARCHAR2(4000);
BEGIN
    v_sql := 'SELECT employee_id, first_name, last_name, department_id, salary 
              FROM employees 
              WHERE (first_name LIKE :p_first || ''%'' OR :p_first IS NULL)
              AND (last_name LIKE :p_last || ''%'' OR :p_last IS NULL)
              AND (department_id = :p_dept OR :p_dept IS NULL)
              AND (salary >= :p_sal OR :p_sal IS NULL)
              ORDER BY last_name';
    
    OPEN p_result FOR v_sql 
    USING p_first_name, p_first_name, 
          p_last_name, p_last_name,
          p_department, p_department,
          p_min_salary, p_min_salary;
END search_employees_safe;
/

-- -----------------------------------------------------------------------------
-- 8. DBMS_SQL Package
-- -----------------------------------------------------------------------------

DECLARE
    v_cursor NUMBER;
    v_sql VARCHAR2(1000);
    v_rows NUMBER;
    v_emp_id NUMBER;
    v_first_name VARCHAR2(100);
    v_salary NUMBER;
    v_dept_id NUMBER := 60;
BEGIN
    -- Open cursor
    v_cursor := DBMS_SQL.OPEN_CURSOR;
    
    -- Build query
    v_sql := 'SELECT employee_id, first_name, salary 
              FROM employees 
              WHERE department_id = :dept_id
              ORDER BY salary DESC';
    
    -- Parse
    DBMS_SQL.PARSE(v_cursor, v_sql, DBMS_SQL.NATIVE);
    
    -- Bind variables
    DBMS_SQL.BIND_VARIABLE(v_cursor, ':dept_id', v_dept_id);
    
    -- Define columns
    DBMS_SQL.DEFINE_COLUMN(v_cursor, 1, v_emp_id);
    DBMS_SQL.DEFINE_COLUMN(v_cursor, 2, v_first_name, 100);
    DBMS_SQL.DEFINE_COLUMN(v_cursor, 3, v_salary);
    
    -- Execute
    v_rows := DBMS_SQL.EXECUTE(v_cursor);
    
    -- Fetch rows
    WHILE DBMS_SQL.FETCH_ROWS(v_cursor) > 0 LOOP
        DBMS_SQL.COLUMN_VALUE(v_cursor, 1, v_emp_id);
        DBMS_SQL.COLUMN_VALUE(v_cursor, 2, v_first_name);
        DBMS_SQL.COLUMN_VALUE(v_cursor, 3, v_salary);
        
        DBMS_OUTPUT.PUT_LINE(v_emp_id || ': ' || v_first_name || ' - $' || v_salary);
    END LOOP;
    
    -- Close cursor
    DBMS_SQL.CLOSE_CURSOR(v_cursor);
    
EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_SQL.IS_OPEN(v_cursor) THEN
            DBMS_SQL.CLOSE_CURSOR(v_cursor);
        END IF;
        RAISE;
END;
/

-- DBMS_SQL for DML
DECLARE
    v_cursor NUMBER;
    v_sql VARCHAR2(1000);
    v_rows NUMBER;
BEGIN
    v_cursor := DBMS_SQL.OPEN_CURSOR;
    
    v_sql := 'UPDATE employees SET salary = salary * :raise WHERE department_id = :dept';
    
    DBMS_SQL.PARSE(v_cursor, v_sql, DBMS_SQL.NATIVE);
    DBMS_SQL.BIND_VARIABLE(v_cursor, ':raise', 1.05);
    DBMS_SQL.BIND_VARIABLE(v_cursor, ':dept', 60);
    
    v_rows := DBMS_SQL.EXECUTE(v_cursor);
    DBMS_OUTPUT.PUT_LINE('Updated ' || v_rows || ' rows');
    
    DBMS_SQL.CLOSE_CURSOR(v_cursor);
    ROLLBACK;
END;
/

-- -----------------------------------------------------------------------------
-- 9. DBMS_SQL with DESCRIBE_COLUMNS
-- -----------------------------------------------------------------------------

DECLARE
    v_cursor NUMBER;
    v_sql VARCHAR2(1000);
    v_col_cnt NUMBER;
    v_desc_tab DBMS_SQL.DESC_TAB;
    v_dummy NUMBER;
BEGIN
    v_cursor := DBMS_SQL.OPEN_CURSOR;
    v_sql := 'SELECT * FROM employees WHERE ROWNUM = 1';
    
    DBMS_SQL.PARSE(v_cursor, v_sql, DBMS_SQL.NATIVE);
    
    -- Get column descriptions
    DBMS_SQL.DESCRIBE_COLUMNS(v_cursor, v_col_cnt, v_desc_tab);
    
    DBMS_OUTPUT.PUT_LINE('Number of columns: ' || v_col_cnt);
    DBMS_OUTPUT.PUT_LINE('---');
    
    FOR i IN 1..v_col_cnt LOOP
        DBMS_OUTPUT.PUT_LINE('Column ' || i || ': ' || v_desc_tab(i).col_name ||
                            ' (' || 
                            CASE v_desc_tab(i).col_type
                                WHEN 1 THEN 'VARCHAR2'
                                WHEN 2 THEN 'NUMBER'
                                WHEN 12 THEN 'DATE'
                                WHEN 96 THEN 'CHAR'
                                WHEN 180 THEN 'TIMESTAMP'
                                ELSE 'TYPE ' || v_desc_tab(i).col_type
                            END || ')');
    END LOOP;
    
    DBMS_SQL.CLOSE_CURSOR(v_cursor);
END;
/

-- -----------------------------------------------------------------------------
-- 10. Converting Between DBMS_SQL and REF CURSOR
-- -----------------------------------------------------------------------------

DECLARE
    v_cursor NUMBER;
    v_sql VARCHAR2(1000);
    v_ref_cursor SYS_REFCURSOR;
    v_emp_id NUMBER;
    v_name VARCHAR2(100);
BEGIN
    -- Create DBMS_SQL cursor
    v_cursor := DBMS_SQL.OPEN_CURSOR;
    v_sql := 'SELECT employee_id, first_name FROM employees WHERE ROWNUM <= 5';
    DBMS_SQL.PARSE(v_cursor, v_sql, DBMS_SQL.NATIVE);
    
    -- Convert to REF CURSOR
    v_ref_cursor := DBMS_SQL.TO_REFCURSOR(v_cursor);
    
    -- Use as REF CURSOR
    LOOP
        FETCH v_ref_cursor INTO v_emp_id, v_name;
        EXIT WHEN v_ref_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE(v_emp_id || ': ' || v_name);
    END LOOP;
    
    CLOSE v_ref_cursor;
END;
/

-- Convert REF CURSOR to DBMS_SQL cursor
DECLARE
    v_ref_cursor SYS_REFCURSOR;
    v_cursor NUMBER;
    v_emp_id NUMBER;
    v_name VARCHAR2(100);
    v_dummy NUMBER;
BEGIN
    -- Open REF CURSOR
    OPEN v_ref_cursor FOR
        SELECT employee_id, first_name FROM employees WHERE ROWNUM <= 5;
    
    -- Convert to DBMS_SQL cursor
    v_cursor := DBMS_SQL.TO_CURSOR_NUMBER(v_ref_cursor);
    
    -- Define columns
    DBMS_SQL.DEFINE_COLUMN(v_cursor, 1, v_emp_id);
    DBMS_SQL.DEFINE_COLUMN(v_cursor, 2, v_name, 100);
    
    -- Fetch using DBMS_SQL
    WHILE DBMS_SQL.FETCH_ROWS(v_cursor) > 0 LOOP
        DBMS_SQL.COLUMN_VALUE(v_cursor, 1, v_emp_id);
        DBMS_SQL.COLUMN_VALUE(v_cursor, 2, v_name);
        DBMS_OUTPUT.PUT_LINE(v_emp_id || ': ' || v_name);
    END LOOP;
    
    DBMS_SQL.CLOSE_CURSOR(v_cursor);
END;
/

-- -----------------------------------------------------------------------------
-- 11. Dynamic PL/SQL
-- -----------------------------------------------------------------------------

DECLARE
    v_plsql VARCHAR2(4000);
    v_result NUMBER;
    v_proc_name VARCHAR2(100) := 'calculate_bonus';
BEGIN
    -- Execute anonymous block
    v_plsql := 'BEGIN :result := 100 + 200; END;';
    EXECUTE IMMEDIATE v_plsql USING OUT v_result;
    DBMS_OUTPUT.PUT_LINE('Result: ' || v_result);
    
    -- Call procedure dynamically
    v_plsql := 'BEGIN ' || v_proc_name || '(:1, :2, :3); END;';
    -- EXECUTE IMMEDIATE v_plsql USING IN 5000, IN 0.1, OUT v_result;
    
    -- Dynamic function call
    v_plsql := 'BEGIN :result := get_employee_salary(:emp_id); END;';
    EXECUTE IMMEDIATE v_plsql USING OUT v_result, IN 100;
    DBMS_OUTPUT.PUT_LINE('Salary: ' || v_result);
END;
/

-- Execute dynamic DDL
CREATE OR REPLACE PROCEDURE create_audit_table(p_table_name VARCHAR2)
IS
    v_sql VARCHAR2(4000);
    v_audit_table VARCHAR2(100);
BEGIN
    v_audit_table := p_table_name || '_AUDIT';
    
    -- Check if table exists
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE ' || v_audit_table;
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;
    
    -- Create audit table
    v_sql := 'CREATE TABLE ' || v_audit_table || ' (
        audit_id NUMBER GENERATED ALWAYS AS IDENTITY,
        operation VARCHAR2(10),
        old_data CLOB,
        new_data CLOB,
        changed_by VARCHAR2(100),
        changed_at TIMESTAMP DEFAULT SYSTIMESTAMP
    )';
    
    EXECUTE IMMEDIATE v_sql;
    DBMS_OUTPUT.PUT_LINE('Created table: ' || v_audit_table);
END create_audit_table;
/

-- -----------------------------------------------------------------------------
-- 12. SQL Injection Prevention
-- -----------------------------------------------------------------------------

-- BAD - vulnerable to SQL injection
CREATE OR REPLACE PROCEDURE bad_search(p_name VARCHAR2)
IS
    v_sql VARCHAR2(1000);
    v_cursor SYS_REFCURSOR;
BEGIN
    -- DANGEROUS: Direct concatenation
    v_sql := 'SELECT * FROM employees WHERE last_name = ''' || p_name || '''';
    OPEN v_cursor FOR v_sql;
    -- Attacker could pass: ' OR '1'='1
END bad_search;
/

-- GOOD - using bind variables
CREATE OR REPLACE PROCEDURE good_search(p_name VARCHAR2)
IS
    v_sql VARCHAR2(1000);
    v_cursor SYS_REFCURSOR;
BEGIN
    -- SAFE: Using bind variable
    v_sql := 'SELECT * FROM employees WHERE last_name = :name';
    OPEN v_cursor FOR v_sql USING p_name;
END good_search;
/

-- GOOD - using DBMS_ASSERT for identifiers
CREATE OR REPLACE PROCEDURE query_table(p_table_name VARCHAR2)
IS
    v_sql VARCHAR2(1000);
    v_safe_name VARCHAR2(100);
    v_count NUMBER;
BEGIN
    -- Validate table name
    v_safe_name := DBMS_ASSERT.SQL_OBJECT_NAME(p_table_name);
    
    -- Alternative validations:
    -- DBMS_ASSERT.SIMPLE_SQL_NAME - alphanumeric only
    -- DBMS_ASSERT.QUALIFIED_SQL_NAME - allows schema.object
    -- DBMS_ASSERT.SCHEMA_NAME - validates schema exists
    -- DBMS_ASSERT.ENQUOTE_NAME - safely quote identifier
    
    v_sql := 'SELECT COUNT(*) FROM ' || v_safe_name;
    EXECUTE IMMEDIATE v_sql INTO v_count;
    DBMS_OUTPUT.PUT_LINE('Count: ' || v_count);
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Invalid table name');
END query_table;
/

