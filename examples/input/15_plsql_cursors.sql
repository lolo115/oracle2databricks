-- ============================================================================
-- File: 15_plsql_cursors.sql
-- Description: Explicit cursors, cursor FOR loops, REF CURSORS
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Explicit Cursor - Basic Usage
-- -----------------------------------------------------------------------------

DECLARE
    CURSOR c_employees IS
        SELECT employee_id, first_name, last_name, salary
        FROM employees
        WHERE department_id = 60;
    
    v_emp_id employees.employee_id%TYPE;
    v_first_name employees.first_name%TYPE;
    v_last_name employees.last_name%TYPE;
    v_salary employees.salary%TYPE;
BEGIN
    -- Open cursor
    OPEN c_employees;
    
    -- Fetch loop
    LOOP
        FETCH c_employees INTO v_emp_id, v_first_name, v_last_name, v_salary;
        EXIT WHEN c_employees%NOTFOUND;
        
        DBMS_OUTPUT.PUT_LINE(v_first_name || ' ' || v_last_name || ': $' || v_salary);
    END LOOP;
    
    -- Close cursor
    CLOSE c_employees;
END;
/

-- -----------------------------------------------------------------------------
-- 2. Cursor with ROWTYPE
-- -----------------------------------------------------------------------------

DECLARE
    CURSOR c_departments IS
        SELECT * FROM departments;
    
    v_dept_rec c_departments%ROWTYPE;
BEGIN
    OPEN c_departments;
    
    LOOP
        FETCH c_departments INTO v_dept_rec;
        EXIT WHEN c_departments%NOTFOUND;
        
        DBMS_OUTPUT.PUT_LINE('Dept ' || v_dept_rec.department_id || 
                            ': ' || v_dept_rec.department_name);
    END LOOP;
    
    CLOSE c_departments;
END;
/

-- -----------------------------------------------------------------------------
-- 3. Cursor Attributes
-- -----------------------------------------------------------------------------

DECLARE
    CURSOR c_emp IS
        SELECT employee_id, first_name, salary
        FROM employees
        WHERE department_id = 50;
    
    v_emp c_emp%ROWTYPE;
    v_count NUMBER := 0;
BEGIN
    -- %ISOPEN before opening
    IF NOT c_emp%ISOPEN THEN
        DBMS_OUTPUT.PUT_LINE('Cursor is closed');
        OPEN c_emp;
    END IF;
    
    -- %ISOPEN after opening
    IF c_emp%ISOPEN THEN
        DBMS_OUTPUT.PUT_LINE('Cursor is now open');
    END IF;
    
    LOOP
        FETCH c_emp INTO v_emp;
        
        -- %FOUND and %NOTFOUND
        IF c_emp%FOUND THEN
            v_count := v_count + 1;
            DBMS_OUTPUT.PUT_LINE('Row ' || c_emp%ROWCOUNT || ': ' || v_emp.first_name);
        END IF;
        
        EXIT WHEN c_emp%NOTFOUND;
    END LOOP;
    
    -- %ROWCOUNT after all fetches
    DBMS_OUTPUT.PUT_LINE('Total rows fetched: ' || c_emp%ROWCOUNT);
    
    CLOSE c_emp;
    
    -- %ISOPEN after closing
    IF NOT c_emp%ISOPEN THEN
        DBMS_OUTPUT.PUT_LINE('Cursor is closed again');
    END IF;
END;
/

-- -----------------------------------------------------------------------------
-- 4. Cursor FOR Loop (Implicit Open/Fetch/Close)
-- -----------------------------------------------------------------------------

DECLARE
    CURSOR c_employees IS
        SELECT employee_id, first_name, last_name, salary
        FROM employees
        WHERE department_id = 60;
BEGIN
    -- Implicit OPEN, FETCH, CLOSE
    FOR emp_rec IN c_employees LOOP
        DBMS_OUTPUT.PUT_LINE(emp_rec.first_name || ' ' || emp_rec.last_name || 
                            ': $' || emp_rec.salary);
    END LOOP;
    -- Cursor automatically closed here
END;
/

-- Inline cursor definition in FOR loop
BEGIN
    FOR emp_rec IN (
        SELECT employee_id, first_name, salary
        FROM employees
        WHERE salary > 10000
        ORDER BY salary DESC
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(emp_rec.first_name || ': $' || emp_rec.salary);
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- 5. Parameterized Cursors
-- -----------------------------------------------------------------------------

DECLARE
    CURSOR c_emp_by_dept (p_dept_id NUMBER) IS
        SELECT employee_id, first_name, salary
        FROM employees
        WHERE department_id = p_dept_id;
    
    CURSOR c_emp_by_salary (p_min_sal NUMBER, p_max_sal NUMBER DEFAULT 99999) IS
        SELECT employee_id, first_name, salary
        FROM employees
        WHERE salary BETWEEN p_min_sal AND p_max_sal;
BEGIN
    -- Use with different parameter values
    DBMS_OUTPUT.PUT_LINE('IT Department (60):');
    FOR emp IN c_emp_by_dept(60) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || emp.first_name || ': $' || emp.salary);
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('Sales Department (80):');
    FOR emp IN c_emp_by_dept(80) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || emp.first_name || ': $' || emp.salary);
    END LOOP;
    
    -- Cursor with multiple parameters
    DBMS_OUTPUT.PUT_LINE('Employees earning $5000-$10000:');
    FOR emp IN c_emp_by_salary(5000, 10000) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || emp.first_name || ': $' || emp.salary);
    END LOOP;
    
    -- Using default parameter
    DBMS_OUTPUT.PUT_LINE('Employees earning >= $15000:');
    FOR emp IN c_emp_by_salary(15000) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || emp.first_name || ': $' || emp.salary);
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- 6. Cursor with FOR UPDATE
-- -----------------------------------------------------------------------------

DECLARE
    CURSOR c_low_salary IS
        SELECT employee_id, first_name, salary
        FROM employees
        WHERE salary < 3000
        FOR UPDATE OF salary;
    
    v_new_salary NUMBER;
BEGIN
    FOR emp IN c_low_salary LOOP
        v_new_salary := emp.salary * 1.10;
        
        UPDATE employees
        SET salary = v_new_salary
        WHERE CURRENT OF c_low_salary;
        
        DBMS_OUTPUT.PUT_LINE(emp.first_name || ': $' || emp.salary || ' -> $' || v_new_salary);
    END LOOP;
    
    COMMIT;
END;
/

-- FOR UPDATE with NOWAIT
DECLARE
    CURSOR c_update IS
        SELECT * FROM employees
        WHERE department_id = 60
        FOR UPDATE NOWAIT;
BEGIN
    FOR emp IN c_update LOOP
        UPDATE employees
        SET last_update = SYSDATE
        WHERE CURRENT OF c_update;
    END LOOP;
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -54 THEN
            DBMS_OUTPUT.PUT_LINE('Resource busy, could not acquire lock');
        ELSE
            RAISE;
        END IF;
END;
/

-- FOR UPDATE WAIT n
DECLARE
    CURSOR c_update IS
        SELECT * FROM employees
        WHERE department_id = 80
        FOR UPDATE WAIT 5;  -- Wait up to 5 seconds
BEGIN
    FOR emp IN c_update LOOP
        UPDATE employees
        SET commission_pct = NVL(commission_pct, 0) + 0.01
        WHERE CURRENT OF c_update;
    END LOOP;
    COMMIT;
END;
/

-- FOR UPDATE with specific columns
DECLARE
    CURSOR c_update IS
        SELECT e.employee_id, e.salary, d.department_name
        FROM employees e
        JOIN departments d ON e.department_id = d.department_id
        WHERE e.department_id = 60
        FOR UPDATE OF e.salary;  -- Only lock employees table
BEGIN
    FOR rec IN c_update LOOP
        UPDATE employees
        SET salary = rec.salary * 1.05
        WHERE CURRENT OF c_update;
    END LOOP;
    COMMIT;
END;
/

-- -----------------------------------------------------------------------------
-- 7. REF CURSOR (Cursor Variables)
-- -----------------------------------------------------------------------------

DECLARE
    -- Weak REF CURSOR (can point to any query)
    TYPE t_weak_cursor IS REF CURSOR;
    c_weak t_weak_cursor;
    
    -- Strong REF CURSOR (specific return type)
    TYPE t_emp_cursor IS REF CURSOR RETURN employees%ROWTYPE;
    c_emp t_emp_cursor;
    
    v_emp_rec employees%ROWTYPE;
    v_dept_rec departments%ROWTYPE;
BEGIN
    -- Open strong cursor
    OPEN c_emp FOR
        SELECT * FROM employees WHERE department_id = 60;
    
    LOOP
        FETCH c_emp INTO v_emp_rec;
        EXIT WHEN c_emp%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('Emp: ' || v_emp_rec.first_name);
    END LOOP;
    CLOSE c_emp;
    
    -- Weak cursor - employees
    OPEN c_weak FOR
        SELECT * FROM employees WHERE ROWNUM <= 3;
    
    LOOP
        FETCH c_weak INTO v_emp_rec;
        EXIT WHEN c_weak%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('Weak Emp: ' || v_emp_rec.first_name);
    END LOOP;
    CLOSE c_weak;
    
    -- Weak cursor - departments (different query)
    OPEN c_weak FOR
        SELECT * FROM departments WHERE ROWNUM <= 3;
    
    LOOP
        FETCH c_weak INTO v_dept_rec;
        EXIT WHEN c_weak%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('Dept: ' || v_dept_rec.department_name);
    END LOOP;
    CLOSE c_weak;
END;
/

-- -----------------------------------------------------------------------------
-- 8. SYS_REFCURSOR
-- -----------------------------------------------------------------------------

DECLARE
    c_ref SYS_REFCURSOR;
    v_emp_id NUMBER;
    v_name VARCHAR2(100);
    v_salary NUMBER;
BEGIN
    OPEN c_ref FOR
        SELECT employee_id, first_name || ' ' || last_name, salary
        FROM employees
        WHERE department_id = 60;
    
    LOOP
        FETCH c_ref INTO v_emp_id, v_name, v_salary;
        EXIT WHEN c_ref%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE(v_name || ': $' || v_salary);
    END LOOP;
    
    CLOSE c_ref;
END;
/

-- Dynamic cursor query
DECLARE
    c_ref SYS_REFCURSOR;
    v_sql VARCHAR2(1000);
    v_dept_id NUMBER := 60;
    v_min_salary NUMBER := 5000;
    
    TYPE t_emp_rec IS RECORD (
        emp_id NUMBER,
        emp_name VARCHAR2(100),
        salary NUMBER
    );
    v_emp t_emp_rec;
BEGIN
    v_sql := 'SELECT employee_id, first_name || '' '' || last_name, salary
              FROM employees
              WHERE department_id = :dept AND salary >= :sal';
    
    OPEN c_ref FOR v_sql USING v_dept_id, v_min_salary;
    
    LOOP
        FETCH c_ref INTO v_emp;
        EXIT WHEN c_ref%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE(v_emp.emp_name || ': $' || v_emp.salary);
    END LOOP;
    
    CLOSE c_ref;
END;
/

-- -----------------------------------------------------------------------------
-- 9. Cursor Expressions (Nested Cursors)
-- -----------------------------------------------------------------------------

DECLARE
    TYPE t_emp_cursor IS REF CURSOR;
    
    CURSOR c_dept IS
        SELECT department_id, 
               department_name,
               CURSOR(SELECT employee_id, first_name, salary
                      FROM employees e
                      WHERE e.department_id = d.department_id) AS emp_cursor
        FROM departments d
        WHERE d.department_id IN (60, 80);
    
    v_dept_id NUMBER;
    v_dept_name VARCHAR2(100);
    c_emp t_emp_cursor;
    v_emp_id NUMBER;
    v_emp_name VARCHAR2(100);
    v_salary NUMBER;
BEGIN
    FOR dept IN c_dept LOOP
        DBMS_OUTPUT.PUT_LINE('Department: ' || dept.department_name);
        
        c_emp := dept.emp_cursor;
        LOOP
            FETCH c_emp INTO v_emp_id, v_emp_name, v_salary;
            EXIT WHEN c_emp%NOTFOUND;
            DBMS_OUTPUT.PUT_LINE('  - ' || v_emp_name || ': $' || v_salary);
        END LOOP;
        -- Don't close nested cursor explicitly
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- 10. BULK COLLECT with Cursors
-- -----------------------------------------------------------------------------

DECLARE
    CURSOR c_employees IS
        SELECT employee_id, first_name, last_name, salary
        FROM employees
        WHERE department_id = 50;
    
    TYPE t_emp_tab IS TABLE OF c_employees%ROWTYPE;
    v_emp_tab t_emp_tab;
BEGIN
    OPEN c_employees;
    
    -- Fetch all rows at once
    FETCH c_employees BULK COLLECT INTO v_emp_tab;
    
    CLOSE c_employees;
    
    -- Process the collection
    FOR i IN 1..v_emp_tab.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_emp_tab(i).first_name || ' ' || v_emp_tab(i).last_name);
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('Total: ' || v_emp_tab.COUNT || ' employees');
END;
/

-- BULK COLLECT with LIMIT
DECLARE
    CURSOR c_employees IS
        SELECT employee_id, first_name, salary
        FROM employees;
    
    TYPE t_emp_tab IS TABLE OF c_employees%ROWTYPE;
    v_emp_tab t_emp_tab;
    v_batch_size CONSTANT NUMBER := 10;
    v_total NUMBER := 0;
BEGIN
    OPEN c_employees;
    
    LOOP
        FETCH c_employees BULK COLLECT INTO v_emp_tab LIMIT v_batch_size;
        
        -- Process batch
        FOR i IN 1..v_emp_tab.COUNT LOOP
            DBMS_OUTPUT.PUT_LINE('Processing: ' || v_emp_tab(i).first_name);
        END LOOP;
        
        v_total := v_total + v_emp_tab.COUNT;
        
        EXIT WHEN v_emp_tab.COUNT < v_batch_size;
    END LOOP;
    
    CLOSE c_employees;
    DBMS_OUTPUT.PUT_LINE('Total processed: ' || v_total);
END;
/

-- -----------------------------------------------------------------------------
-- 11. Cursor with Multiple OPEN
-- -----------------------------------------------------------------------------

DECLARE
    CURSOR c_emp(p_dept_id NUMBER) IS
        SELECT first_name, salary FROM employees WHERE department_id = p_dept_id;
    
    v_first_name VARCHAR2(100);
    v_salary NUMBER;
    v_dept_ids VARCHAR2(100) := '60,80,90';
    v_dept_id NUMBER;
BEGIN
    -- Process multiple departments with same cursor
    FOR i IN 1..3 LOOP
        v_dept_id := CASE i WHEN 1 THEN 60 WHEN 2 THEN 80 WHEN 3 THEN 90 END;
        
        DBMS_OUTPUT.PUT_LINE('--- Department ' || v_dept_id || ' ---');
        
        OPEN c_emp(v_dept_id);
        LOOP
            FETCH c_emp INTO v_first_name, v_salary;
            EXIT WHEN c_emp%NOTFOUND;
            DBMS_OUTPUT.PUT_LINE(v_first_name || ': $' || v_salary);
        END LOOP;
        CLOSE c_emp;
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- 12. Cursor in Package (Public Cursor)
-- -----------------------------------------------------------------------------

/*
CREATE OR REPLACE PACKAGE emp_cursors AS
    CURSOR c_all_employees RETURN employees%ROWTYPE;
    CURSOR c_by_department(p_dept_id NUMBER) RETURN employees%ROWTYPE;
END emp_cursors;
/

CREATE OR REPLACE PACKAGE BODY emp_cursors AS
    CURSOR c_all_employees RETURN employees%ROWTYPE IS
        SELECT * FROM employees ORDER BY last_name;
    
    CURSOR c_by_department(p_dept_id NUMBER) RETURN employees%ROWTYPE IS
        SELECT * FROM employees WHERE department_id = p_dept_id ORDER BY last_name;
END emp_cursors;
/

-- Usage
DECLARE
    v_emp employees%ROWTYPE;
BEGIN
    FOR v_emp IN emp_cursors.c_by_department(60) LOOP
        DBMS_OUTPUT.PUT_LINE(v_emp.first_name || ' ' || v_emp.last_name);
    END LOOP;
END;
/
*/

-- -----------------------------------------------------------------------------
-- 13. Advanced Cursor Patterns
-- -----------------------------------------------------------------------------

-- Cursor with Subquery
DECLARE
    CURSOR c_above_avg IS
        SELECT employee_id, first_name, salary
        FROM employees
        WHERE salary > (SELECT AVG(salary) FROM employees)
        ORDER BY salary DESC;
BEGIN
    FOR emp IN c_above_avg LOOP
        DBMS_OUTPUT.PUT_LINE(emp.first_name || ': $' || emp.salary);
    END LOOP;
END;
/

-- Cursor with Analytics
DECLARE
    CURSOR c_ranked IS
        SELECT employee_id, first_name, department_id, salary,
               RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dept_rank
        FROM employees;
BEGIN
    FOR emp IN c_ranked LOOP
        IF emp.dept_rank <= 3 THEN
            DBMS_OUTPUT.PUT_LINE('Dept ' || emp.department_id || 
                                ' #' || emp.dept_rank || ': ' || emp.first_name);
        END IF;
    END LOOP;
END;
/

-- Cursor with CTE
DECLARE
    CURSOR c_summary IS
        WITH dept_summary AS (
            SELECT department_id, COUNT(*) AS cnt, AVG(salary) AS avg_sal
            FROM employees
            GROUP BY department_id
        )
        SELECT d.department_name, s.cnt, s.avg_sal
        FROM dept_summary s
        JOIN departments d ON s.department_id = d.department_id
        ORDER BY s.avg_sal DESC;
BEGIN
    FOR rec IN c_summary LOOP
        DBMS_OUTPUT.PUT_LINE(rec.department_name || ': ' || 
                            rec.cnt || ' employees, avg $' || ROUND(rec.avg_sal, 2));
    END LOOP;
END;
/

-- Multiple cursors with coordination
DECLARE
    CURSOR c_depts IS
        SELECT department_id, department_name FROM departments WHERE department_id IN (60, 80, 90);
    
    CURSOR c_emps(p_dept_id NUMBER) IS
        SELECT first_name, salary FROM employees WHERE department_id = p_dept_id ORDER BY salary DESC;
    
    v_total_salary NUMBER;
BEGIN
    FOR dept IN c_depts LOOP
        DBMS_OUTPUT.PUT_LINE('=== ' || dept.department_name || ' ===');
        v_total_salary := 0;
        
        FOR emp IN c_emps(dept.department_id) LOOP
            DBMS_OUTPUT.PUT_LINE('  ' || emp.first_name || ': $' || emp.salary);
            v_total_salary := v_total_salary + emp.salary;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('  Total: $' || v_total_salary);
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/

