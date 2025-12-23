-- ============================================================================
-- File: 17_plsql_procedures.sql
-- Description: PL/SQL stored procedures
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Simple Procedures
-- -----------------------------------------------------------------------------

-- Basic procedure with no parameters
CREATE OR REPLACE PROCEDURE print_hello
IS
BEGIN
    DBMS_OUTPUT.PUT_LINE('Hello, World!');
END print_hello;
/

-- Procedure with IN parameter
CREATE OR REPLACE PROCEDURE print_employee(p_emp_id NUMBER)
IS
    v_name VARCHAR2(100);
    v_salary NUMBER;
BEGIN
    SELECT first_name || ' ' || last_name, salary
    INTO v_name, v_salary
    FROM employees
    WHERE employee_id = p_emp_id;
    
    DBMS_OUTPUT.PUT_LINE('Employee: ' || v_name);
    DBMS_OUTPUT.PUT_LINE('Salary: $' || v_salary);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Employee ' || p_emp_id || ' not found');
END print_employee;
/

-- Procedure with multiple IN parameters
CREATE OR REPLACE PROCEDURE print_date_range(
    p_start_date DATE,
    p_end_date DATE
)
IS
BEGIN
    DBMS_OUTPUT.PUT_LINE('From: ' || TO_CHAR(p_start_date, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('To: ' || TO_CHAR(p_end_date, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('Days: ' || (p_end_date - p_start_date));
END print_date_range;
/

-- -----------------------------------------------------------------------------
-- 2. Procedures with OUT Parameters
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE get_employee_info(
    p_emp_id    IN  NUMBER,
    p_name      OUT VARCHAR2,
    p_salary    OUT NUMBER,
    p_hire_date OUT DATE
)
IS
BEGIN
    SELECT first_name || ' ' || last_name, salary, hire_date
    INTO p_name, p_salary, p_hire_date
    FROM employees
    WHERE employee_id = p_emp_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        p_name := NULL;
        p_salary := NULL;
        p_hire_date := NULL;
END get_employee_info;
/

-- Procedure with multiple OUT parameters and record type
CREATE OR REPLACE PROCEDURE get_department_stats(
    p_dept_id     IN  NUMBER,
    p_emp_count   OUT NUMBER,
    p_total_sal   OUT NUMBER,
    p_avg_sal     OUT NUMBER,
    p_min_sal     OUT NUMBER,
    p_max_sal     OUT NUMBER
)
IS
BEGIN
    SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary)
    INTO p_emp_count, p_total_sal, p_avg_sal, p_min_sal, p_max_sal
    FROM employees
    WHERE department_id = p_dept_id;
END get_department_stats;
/

-- -----------------------------------------------------------------------------
-- 3. Procedures with IN OUT Parameters
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE apply_raise(
    p_emp_id     IN     NUMBER,
    p_raise_pct  IN OUT NUMBER  -- Pass in requested %, get actual % applied
)
IS
    v_current_salary NUMBER;
    v_max_raise CONSTANT NUMBER := 0.25;
BEGIN
    SELECT salary INTO v_current_salary
    FROM employees
    WHERE employee_id = p_emp_id;
    
    -- Cap the raise at 25%
    IF p_raise_pct > v_max_raise THEN
        p_raise_pct := v_max_raise;
    END IF;
    
    -- Apply the raise
    UPDATE employees
    SET salary = salary * (1 + p_raise_pct)
    WHERE employee_id = p_emp_id;
    
    COMMIT;
END apply_raise;
/

-- String formatting procedure
CREATE OR REPLACE PROCEDURE format_address(
    p_street  IN OUT VARCHAR2,
    p_city    IN OUT VARCHAR2,
    p_state   IN OUT VARCHAR2,
    p_zip     IN OUT VARCHAR2
)
IS
BEGIN
    p_street := INITCAP(TRIM(p_street));
    p_city := INITCAP(TRIM(p_city));
    p_state := UPPER(TRIM(p_state));
    p_zip := TRIM(p_zip);
    
    -- Validate state (2 chars)
    IF LENGTH(p_state) > 2 THEN
        p_state := SUBSTR(p_state, 1, 2);
    END IF;
END format_address;
/

-- -----------------------------------------------------------------------------
-- 4. Procedures with Default Parameters
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE create_employee(
    p_first_name    VARCHAR2,
    p_last_name     VARCHAR2,
    p_email         VARCHAR2,
    p_job_id        VARCHAR2 DEFAULT 'IT_PROG',
    p_salary        NUMBER DEFAULT 5000,
    p_department_id NUMBER DEFAULT 60,
    p_hire_date     DATE DEFAULT SYSDATE,
    p_emp_id        OUT NUMBER
)
IS
BEGIN
    SELECT employees_seq.NEXTVAL INTO p_emp_id FROM DUAL;
    
    INSERT INTO employees (
        employee_id, first_name, last_name, email, 
        job_id, salary, department_id, hire_date
    ) VALUES (
        p_emp_id, p_first_name, p_last_name, UPPER(p_email),
        p_job_id, p_salary, p_department_id, p_hire_date
    );
    
    COMMIT;
END create_employee;
/

-- -----------------------------------------------------------------------------
-- 5. Procedures with NOCOPY Hint
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE process_large_array(
    p_data IN OUT NOCOPY DBMS_SQL.VARCHAR2_TABLE  -- Pass by reference for performance
)
IS
BEGIN
    FOR i IN 1..p_data.COUNT LOOP
        p_data(i) := UPPER(p_data(i));
    END LOOP;
END process_large_array;
/

-- -----------------------------------------------------------------------------
-- 6. DML Procedures
-- -----------------------------------------------------------------------------

-- Insert procedure
CREATE OR REPLACE PROCEDURE insert_department(
    p_dept_name    VARCHAR2,
    p_manager_id   NUMBER DEFAULT NULL,
    p_location_id  NUMBER DEFAULT NULL,
    p_dept_id      OUT NUMBER
)
IS
BEGIN
    SELECT departments_seq.NEXTVAL INTO p_dept_id FROM DUAL;
    
    INSERT INTO departments (department_id, department_name, manager_id, location_id)
    VALUES (p_dept_id, p_dept_name, p_manager_id, p_location_id);
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Created department ' || p_dept_id || ': ' || p_dept_name);
END insert_department;
/

-- Update procedure
CREATE OR REPLACE PROCEDURE update_salary(
    p_emp_id     NUMBER,
    p_new_salary NUMBER,
    p_updated    OUT BOOLEAN
)
IS
BEGIN
    UPDATE employees
    SET salary = p_new_salary
    WHERE employee_id = p_emp_id;
    
    p_updated := SQL%FOUND;
    
    IF p_updated THEN
        COMMIT;
    END IF;
END update_salary;
/

-- Delete procedure
CREATE OR REPLACE PROCEDURE delete_employee(
    p_emp_id       NUMBER,
    p_rows_deleted OUT NUMBER
)
IS
BEGIN
    -- First archive the employee
    INSERT INTO employees_archive
    SELECT e.*, SYSDATE AS archive_date
    FROM employees e
    WHERE employee_id = p_emp_id;
    
    -- Then delete
    DELETE FROM employees
    WHERE employee_id = p_emp_id;
    
    p_rows_deleted := SQL%ROWCOUNT;
    
    COMMIT;
END delete_employee;
/

-- -----------------------------------------------------------------------------
-- 7. Procedures with Cursors
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE process_employees_by_dept(p_dept_id NUMBER)
IS
    CURSOR c_emp IS
        SELECT employee_id, first_name, last_name, salary
        FROM employees
        WHERE department_id = p_dept_id
        FOR UPDATE OF salary;
    
    v_raise_pct NUMBER;
    v_count NUMBER := 0;
BEGIN
    FOR emp IN c_emp LOOP
        -- Calculate raise based on salary
        v_raise_pct := CASE
            WHEN emp.salary < 5000 THEN 0.10
            WHEN emp.salary < 10000 THEN 0.07
            ELSE 0.05
        END;
        
        UPDATE employees
        SET salary = salary * (1 + v_raise_pct)
        WHERE CURRENT OF c_emp;
        
        v_count := v_count + 1;
        
        DBMS_OUTPUT.PUT_LINE(emp.first_name || ' ' || emp.last_name || 
                            ': ' || v_raise_pct * 100 || '% raise');
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('Total employees updated: ' || v_count);
    COMMIT;
END process_employees_by_dept;
/

-- Procedure returning REF CURSOR
CREATE OR REPLACE PROCEDURE get_employees_by_criteria(
    p_dept_id    NUMBER DEFAULT NULL,
    p_min_salary NUMBER DEFAULT NULL,
    p_job_id     VARCHAR2 DEFAULT NULL,
    p_result     OUT SYS_REFCURSOR
)
IS
BEGIN
    OPEN p_result FOR
        SELECT employee_id, first_name, last_name, salary, department_id, job_id
        FROM employees
        WHERE (p_dept_id IS NULL OR department_id = p_dept_id)
        AND (p_min_salary IS NULL OR salary >= p_min_salary)
        AND (p_job_id IS NULL OR job_id = p_job_id)
        ORDER BY last_name;
END get_employees_by_criteria;
/

-- -----------------------------------------------------------------------------
-- 8. Transaction Management Procedures
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE transfer_employee(
    p_emp_id        NUMBER,
    p_new_dept_id   NUMBER,
    p_new_job_id    VARCHAR2 DEFAULT NULL,
    p_new_salary    NUMBER DEFAULT NULL
)
IS
    v_old_dept_id NUMBER;
    v_old_job_id VARCHAR2(20);
    v_old_salary NUMBER;
BEGIN
    -- Get current values
    SELECT department_id, job_id, salary
    INTO v_old_dept_id, v_old_job_id, v_old_salary
    FROM employees
    WHERE employee_id = p_emp_id
    FOR UPDATE;
    
    -- Create history record
    INSERT INTO job_history (
        employee_id, start_date, end_date, job_id, department_id
    ) VALUES (
        p_emp_id, 
        (SELECT hire_date FROM employees WHERE employee_id = p_emp_id),
        SYSDATE,
        v_old_job_id,
        v_old_dept_id
    );
    
    -- Update employee
    UPDATE employees
    SET department_id = p_new_dept_id,
        job_id = NVL(p_new_job_id, job_id),
        salary = NVL(p_new_salary, salary)
    WHERE employee_id = p_emp_id;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Employee ' || p_emp_id || ' transferred successfully');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error transferring employee: ' || SQLERRM);
        RAISE;
END transfer_employee;
/

-- Procedure with savepoints
CREATE OR REPLACE PROCEDURE batch_update_salaries(
    p_dept_id     NUMBER,
    p_raise_pct   NUMBER,
    p_commit_size NUMBER DEFAULT 100
)
IS
    v_count NUMBER := 0;
    v_total NUMBER := 0;
BEGIN
    FOR emp IN (
        SELECT employee_id, salary
        FROM employees
        WHERE department_id = p_dept_id
    ) LOOP
        SAVEPOINT before_update;
        
        BEGIN
            UPDATE employees
            SET salary = salary * (1 + p_raise_pct)
            WHERE employee_id = emp.employee_id;
            
            v_count := v_count + 1;
            v_total := v_total + 1;
            
            -- Commit in batches
            IF v_count >= p_commit_size THEN
                COMMIT;
                v_count := 0;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK TO before_update;
                DBMS_OUTPUT.PUT_LINE('Error updating emp ' || emp.employee_id || ': ' || SQLERRM);
        END;
    END LOOP;
    
    -- Final commit
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Updated ' || v_total || ' employees');
END batch_update_salaries;
/

-- -----------------------------------------------------------------------------
-- 9. Autonomous Transaction Procedures
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE log_activity(
    p_action    VARCHAR2,
    p_details   VARCHAR2 DEFAULT NULL
)
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO activity_log (
        log_id, action, details, log_date, user_name
    ) VALUES (
        activity_log_seq.NEXTVAL, p_action, p_details, SYSDATE, USER
    );
    
    COMMIT;  -- Commits only the logging, not the main transaction
END log_activity;
/

-- Error logging with autonomous transaction
CREATE OR REPLACE PROCEDURE log_error(
    p_procedure_name VARCHAR2,
    p_error_code     NUMBER,
    p_error_message  VARCHAR2,
    p_error_stack    VARCHAR2 DEFAULT NULL
)
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO error_log (
        log_id, procedure_name, error_code, error_message, 
        error_stack, log_date, user_name
    ) VALUES (
        error_log_seq.NEXTVAL, p_procedure_name, p_error_code, 
        SUBSTR(p_error_message, 1, 4000),
        SUBSTR(p_error_stack, 1, 4000), SYSDATE, USER
    );
    
    COMMIT;
END log_error;
/

-- -----------------------------------------------------------------------------
-- 10. Recursive Procedures
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE print_org_chart(
    p_manager_id NUMBER DEFAULT NULL,
    p_level      NUMBER DEFAULT 0
)
IS
    v_indent VARCHAR2(100);
BEGIN
    v_indent := LPAD(' ', p_level * 4, ' ');
    
    FOR emp IN (
        SELECT employee_id, first_name, last_name, job_id
        FROM employees
        WHERE NVL(manager_id, -1) = NVL(p_manager_id, -1)
        ORDER BY last_name
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(v_indent || emp.first_name || ' ' || 
                            emp.last_name || ' (' || emp.job_id || ')');
        
        -- Recursive call for subordinates
        print_org_chart(emp.employee_id, p_level + 1);
    END LOOP;
END print_org_chart;
/

-- -----------------------------------------------------------------------------
-- 11. Overloaded Procedures
-- -----------------------------------------------------------------------------

-- In a package, you can have overloaded procedures:
/*
CREATE OR REPLACE PACKAGE employee_ops AS
    PROCEDURE find_employee(p_emp_id NUMBER);
    PROCEDURE find_employee(p_email VARCHAR2);
    PROCEDURE find_employee(p_first_name VARCHAR2, p_last_name VARCHAR2);
END employee_ops;
/

CREATE OR REPLACE PACKAGE BODY employee_ops AS
    PROCEDURE find_employee(p_emp_id NUMBER) IS
        v_name VARCHAR2(100);
    BEGIN
        SELECT first_name || ' ' || last_name INTO v_name
        FROM employees WHERE employee_id = p_emp_id;
        DBMS_OUTPUT.PUT_LINE('Found: ' || v_name);
    END;
    
    PROCEDURE find_employee(p_email VARCHAR2) IS
        v_name VARCHAR2(100);
    BEGIN
        SELECT first_name || ' ' || last_name INTO v_name
        FROM employees WHERE UPPER(email) = UPPER(p_email);
        DBMS_OUTPUT.PUT_LINE('Found: ' || v_name);
    END;
    
    PROCEDURE find_employee(p_first_name VARCHAR2, p_last_name VARCHAR2) IS
        v_emp_id NUMBER;
    BEGIN
        SELECT employee_id INTO v_emp_id
        FROM employees 
        WHERE UPPER(first_name) = UPPER(p_first_name)
        AND UPPER(last_name) = UPPER(p_last_name);
        DBMS_OUTPUT.PUT_LINE('Found ID: ' || v_emp_id);
    END;
END employee_ops;
/
*/

-- -----------------------------------------------------------------------------
-- 12. Error Handling in Procedures
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE robust_employee_update(
    p_emp_id     NUMBER,
    p_salary     NUMBER DEFAULT NULL,
    p_dept_id    NUMBER DEFAULT NULL,
    p_job_id     VARCHAR2 DEFAULT NULL,
    p_success    OUT BOOLEAN,
    p_message    OUT VARCHAR2
)
IS
    e_employee_not_found EXCEPTION;
    e_invalid_salary EXCEPTION;
    e_invalid_department EXCEPTION;
    
    v_count NUMBER;
BEGIN
    p_success := FALSE;
    p_message := NULL;
    
    -- Validate employee exists
    SELECT COUNT(*) INTO v_count FROM employees WHERE employee_id = p_emp_id;
    IF v_count = 0 THEN
        RAISE e_employee_not_found;
    END IF;
    
    -- Validate salary if provided
    IF p_salary IS NOT NULL AND p_salary < 0 THEN
        RAISE e_invalid_salary;
    END IF;
    
    -- Validate department if provided
    IF p_dept_id IS NOT NULL THEN
        SELECT COUNT(*) INTO v_count FROM departments WHERE department_id = p_dept_id;
        IF v_count = 0 THEN
            RAISE e_invalid_department;
        END IF;
    END IF;
    
    -- Perform update
    UPDATE employees
    SET salary = NVL(p_salary, salary),
        department_id = NVL(p_dept_id, department_id),
        job_id = NVL(p_job_id, job_id)
    WHERE employee_id = p_emp_id;
    
    COMMIT;
    
    p_success := TRUE;
    p_message := 'Employee updated successfully';
    
EXCEPTION
    WHEN e_employee_not_found THEN
        p_message := 'Employee ' || p_emp_id || ' not found';
        log_error('ROBUST_EMPLOYEE_UPDATE', -20001, p_message);
        
    WHEN e_invalid_salary THEN
        p_message := 'Invalid salary: ' || p_salary;
        log_error('ROBUST_EMPLOYEE_UPDATE', -20002, p_message);
        
    WHEN e_invalid_department THEN
        p_message := 'Department ' || p_dept_id || ' not found';
        log_error('ROBUST_EMPLOYEE_UPDATE', -20003, p_message);
        
    WHEN OTHERS THEN
        ROLLBACK;
        p_message := 'Unexpected error: ' || SQLERRM;
        log_error('ROBUST_EMPLOYEE_UPDATE', SQLCODE, SQLERRM, DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
END robust_employee_update;
/

-- -----------------------------------------------------------------------------
-- 13. Procedures Calling Other Procedures/Functions
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE process_new_hire(
    p_first_name    VARCHAR2,
    p_last_name     VARCHAR2,
    p_email         VARCHAR2,
    p_dept_id       NUMBER,
    p_job_id        VARCHAR2,
    p_salary        NUMBER,
    p_emp_id        OUT NUMBER
)
IS
    v_dept_name VARCHAR2(100);
    v_bonus NUMBER;
BEGIN
    -- Log the start
    log_activity('NEW_HIRE_START', p_first_name || ' ' || p_last_name);
    
    -- Create the employee
    create_employee(
        p_first_name    => p_first_name,
        p_last_name     => p_last_name,
        p_email         => p_email,
        p_job_id        => p_job_id,
        p_salary        => p_salary,
        p_department_id => p_dept_id,
        p_emp_id        => p_emp_id
    );
    
    -- Get department name using function
    v_dept_name := get_department_name(p_dept_id);
    
    -- Calculate welcome bonus using function
    v_bonus := calculate_bonus(p_salary, 0, 0);
    
    -- Log completion
    log_activity('NEW_HIRE_COMPLETE', 
                'ID: ' || p_emp_id || ', Dept: ' || v_dept_name || ', Bonus: $' || v_bonus);
    
    DBMS_OUTPUT.PUT_LINE('New employee ' || p_emp_id || ' created in ' || v_dept_name);
    
EXCEPTION
    WHEN OTHERS THEN
        log_error('PROCESS_NEW_HIRE', SQLCODE, SQLERRM);
        RAISE;
END process_new_hire;
/

-- -----------------------------------------------------------------------------
-- 14. Named and Mixed Notation Calls
-- -----------------------------------------------------------------------------

-- Demonstration procedure
CREATE OR REPLACE PROCEDURE demo_parameters(
    p_required1 VARCHAR2,
    p_required2 NUMBER,
    p_optional1 VARCHAR2 DEFAULT 'default1',
    p_optional2 NUMBER DEFAULT 100,
    p_optional3 DATE DEFAULT SYSDATE
)
IS
BEGIN
    DBMS_OUTPUT.PUT_LINE('p_required1: ' || p_required1);
    DBMS_OUTPUT.PUT_LINE('p_required2: ' || p_required2);
    DBMS_OUTPUT.PUT_LINE('p_optional1: ' || p_optional1);
    DBMS_OUTPUT.PUT_LINE('p_optional2: ' || p_optional2);
    DBMS_OUTPUT.PUT_LINE('p_optional3: ' || TO_CHAR(p_optional3, 'YYYY-MM-DD'));
END demo_parameters;
/

-- Calling examples (would be in anonymous block):
/*
BEGIN
    -- Positional notation
    demo_parameters('A', 1, 'B', 2, SYSDATE);
    
    -- Named notation
    demo_parameters(
        p_required1 => 'A',
        p_required2 => 1,
        p_optional3 => DATE '2024-01-01'
    );
    
    -- Mixed notation (positional first, then named)
    demo_parameters('A', 1, p_optional2 => 200);
END;
*/

