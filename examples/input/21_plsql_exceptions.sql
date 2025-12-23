-- ============================================================================
-- File: 21_plsql_exceptions.sql
-- Description: PL/SQL exception handling patterns
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Predefined Exceptions
-- -----------------------------------------------------------------------------

DECLARE
    v_salary NUMBER;
    v_name VARCHAR2(100);
    v_result NUMBER;
BEGIN
    -- NO_DATA_FOUND
    BEGIN
        SELECT salary INTO v_salary FROM employees WHERE employee_id = 99999;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Employee not found');
    END;
    
    -- TOO_MANY_ROWS
    BEGIN
        SELECT first_name INTO v_name FROM employees WHERE department_id = 60;
    EXCEPTION
        WHEN TOO_MANY_ROWS THEN
            DBMS_OUTPUT.PUT_LINE('Query returned multiple rows');
    END;
    
    -- ZERO_DIVIDE
    BEGIN
        v_result := 100 / 0;
    EXCEPTION
        WHEN ZERO_DIVIDE THEN
            DBMS_OUTPUT.PUT_LINE('Cannot divide by zero');
    END;
    
    -- VALUE_ERROR
    BEGIN
        v_name := 'This string is way too long for the variable declared above with only 100 characters and this text exceeds it';
    EXCEPTION
        WHEN VALUE_ERROR THEN
            DBMS_OUTPUT.PUT_LINE('Value error - string too long');
    END;
    
    -- INVALID_NUMBER
    BEGIN
        v_result := TO_NUMBER('ABC');
    EXCEPTION
        WHEN INVALID_NUMBER THEN
            DBMS_OUTPUT.PUT_LINE('Invalid number format');
    END;
    
    -- DUP_VAL_ON_INDEX
    BEGIN
        INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id)
        VALUES (100, 'Test', 'User', 'TEST', SYSDATE, 'IT_PROG');  -- ID 100 exists
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
            DBMS_OUTPUT.PUT_LINE('Duplicate value on unique index');
    END;
    
    ROLLBACK;
END;
/

-- -----------------------------------------------------------------------------
-- 2. Other Predefined Exceptions
-- -----------------------------------------------------------------------------

DECLARE
    v_cursor SYS_REFCURSOR;
    v_result VARCHAR2(100);
BEGIN
    -- CURSOR_ALREADY_OPEN
    BEGIN
        OPEN v_cursor FOR SELECT first_name FROM employees WHERE ROWNUM = 1;
        OPEN v_cursor FOR SELECT last_name FROM employees WHERE ROWNUM = 1;  -- Error
    EXCEPTION
        WHEN CURSOR_ALREADY_OPEN THEN
            DBMS_OUTPUT.PUT_LINE('Cursor already open');
            CLOSE v_cursor;
    END;
    
    -- INVALID_CURSOR
    BEGIN
        CLOSE v_cursor;  -- Already closed
    EXCEPTION
        WHEN INVALID_CURSOR THEN
            DBMS_OUTPUT.PUT_LINE('Invalid cursor operation');
    END;
    
    -- LOGIN_DENIED (cannot be caught in anonymous block)
    -- ACCESS_INTO_NULL (object not initialized)
    -- COLLECTION_IS_NULL (collection not initialized)
    -- PROGRAM_ERROR (internal PL/SQL error)
    -- STORAGE_ERROR (out of memory)
    -- TIMEOUT_ON_RESOURCE (resource wait timeout)
    -- ROWTYPE_MISMATCH (cursor variable mismatch)
    -- SUBSCRIPT_OUTSIDE_LIMIT (VARRAY index out of bounds)
    -- SUBSCRIPT_BEYOND_COUNT (collection index beyond COUNT)
END;
/

-- -----------------------------------------------------------------------------
-- 3. User-Defined Exceptions
-- -----------------------------------------------------------------------------

DECLARE
    -- Declare custom exceptions
    e_invalid_salary EXCEPTION;
    e_employee_not_found EXCEPTION;
    e_department_full EXCEPTION;
    
    v_salary NUMBER := -500;
    v_emp_count NUMBER := 0;
    v_max_employees CONSTANT NUMBER := 50;
BEGIN
    -- Raise exception based on condition
    IF v_salary < 0 THEN
        RAISE e_invalid_salary;
    END IF;
    
EXCEPTION
    WHEN e_invalid_salary THEN
        DBMS_OUTPUT.PUT_LINE('Error: Salary cannot be negative');
    WHEN e_employee_not_found THEN
        DBMS_OUTPUT.PUT_LINE('Error: Employee not found');
    WHEN e_department_full THEN
        DBMS_OUTPUT.PUT_LINE('Error: Department has reached maximum capacity');
END;
/

-- -----------------------------------------------------------------------------
-- 4. PRAGMA EXCEPTION_INIT
-- -----------------------------------------------------------------------------

DECLARE
    -- Associate exception with Oracle error code
    e_unique_violation EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_unique_violation, -1);  -- ORA-00001
    
    e_check_constraint EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_check_constraint, -2290);  -- ORA-02290
    
    e_fk_violation EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_fk_violation, -2291);  -- ORA-02291
    
    e_fk_child_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_fk_child_exists, -2292);  -- ORA-02292
    
    e_table_not_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_not_exists, -942);  -- ORA-00942
    
    e_deadlock EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_deadlock, -60);  -- ORA-00060
    
    e_resource_busy EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_resource_busy, -54);  -- ORA-00054
BEGIN
    -- Try to insert duplicate
    INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id)
    VALUES (100, 'Test', 'User', 'NEWTEST', SYSDATE, 'IT_PROG');
    
EXCEPTION
    WHEN e_unique_violation THEN
        DBMS_OUTPUT.PUT_LINE('Unique constraint violated');
    WHEN e_check_constraint THEN
        DBMS_OUTPUT.PUT_LINE('Check constraint violated');
    WHEN e_fk_violation THEN
        DBMS_OUTPUT.PUT_LINE('Foreign key constraint violated');
    WHEN e_table_not_exists THEN
        DBMS_OUTPUT.PUT_LINE('Table does not exist');
    WHEN e_deadlock THEN
        DBMS_OUTPUT.PUT_LINE('Deadlock detected');
    WHEN e_resource_busy THEN
        DBMS_OUTPUT.PUT_LINE('Resource busy, try again later');
END;
/

-- -----------------------------------------------------------------------------
-- 5. RAISE_APPLICATION_ERROR
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE validate_salary(
    p_emp_id NUMBER,
    p_new_salary NUMBER
)
IS
    v_job_id VARCHAR2(20);
    v_min_salary NUMBER;
    v_max_salary NUMBER;
BEGIN
    -- Get job info
    SELECT e.job_id, j.min_salary, j.max_salary
    INTO v_job_id, v_min_salary, v_max_salary
    FROM employees e
    JOIN jobs j ON e.job_id = j.job_id
    WHERE e.employee_id = p_emp_id;
    
    -- Validate
    IF p_new_salary < 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Salary cannot be negative');
    END IF;
    
    IF p_new_salary < v_min_salary THEN
        RAISE_APPLICATION_ERROR(-20002, 
            'Salary ' || p_new_salary || ' is below minimum ' || v_min_salary || 
            ' for job ' || v_job_id);
    END IF;
    
    IF p_new_salary > v_max_salary THEN
        RAISE_APPLICATION_ERROR(-20003,
            'Salary ' || p_new_salary || ' exceeds maximum ' || v_max_salary ||
            ' for job ' || v_job_id);
    END IF;
    
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20004, 'Employee ' || p_emp_id || ' not found');
END validate_salary;
/

-- Keep error stack with third parameter
CREATE OR REPLACE PROCEDURE process_with_stack
IS
BEGIN
    -- Simulate error
    RAISE_APPLICATION_ERROR(-20001, 'Original error');
EXCEPTION
    WHEN OTHERS THEN
        -- Re-raise with stack trace preserved
        RAISE_APPLICATION_ERROR(-20002, 'Wrapper error: ' || SQLERRM, TRUE);
END process_with_stack;
/

-- -----------------------------------------------------------------------------
-- 6. Exception Propagation
-- -----------------------------------------------------------------------------

DECLARE
    e_custom EXCEPTION;
    
    PROCEDURE level_3 IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('In level 3 - raising exception');
        RAISE e_custom;
    END;
    
    PROCEDURE level_2 IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('In level 2');
        level_3;
        DBMS_OUTPUT.PUT_LINE('After level 3 - will not print');
    END;
    
    PROCEDURE level_1 IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('In level 1');
        level_2;
        DBMS_OUTPUT.PUT_LINE('After level 2 - will not print');
    EXCEPTION
        WHEN e_custom THEN
            DBMS_OUTPUT.PUT_LINE('Caught in level 1');
            -- Optionally re-raise
            RAISE;
    END;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting');
    level_1;
    DBMS_OUTPUT.PUT_LINE('After level 1 - will not print');
EXCEPTION
    WHEN e_custom THEN
        DBMS_OUTPUT.PUT_LINE('Caught at top level');
END;
/

-- -----------------------------------------------------------------------------
-- 7. WHEN OTHERS Handler
-- -----------------------------------------------------------------------------

DECLARE
    v_result NUMBER;
BEGIN
    -- Some operation that might fail
    v_result := 100 / 0;
    
EXCEPTION
    WHEN ZERO_DIVIDE THEN
        DBMS_OUTPUT.PUT_LINE('Specific: Division by zero');
    WHEN OTHERS THEN
        -- Catch-all handler
        DBMS_OUTPUT.PUT_LINE('Error Code: ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE('Error Message: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Error Stack: ' || DBMS_UTILITY.FORMAT_ERROR_STACK);
        DBMS_OUTPUT.PUT_LINE('Call Stack: ' || DBMS_UTILITY.FORMAT_CALL_STACK);
        DBMS_OUTPUT.PUT_LINE('Backtrace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        
        -- Re-raise after logging
        RAISE;
END;
/

-- -----------------------------------------------------------------------------
-- 8. Error Logging Pattern
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE log_error(
    p_procedure_name VARCHAR2,
    p_error_code NUMBER,
    p_error_message VARCHAR2,
    p_error_stack VARCHAR2 DEFAULT NULL,
    p_parameters VARCHAR2 DEFAULT NULL
)
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO error_log (
        log_id, procedure_name, error_code, error_message,
        error_stack, parameters, log_date, session_user
    ) VALUES (
        error_log_seq.NEXTVAL, p_procedure_name, p_error_code,
        SUBSTR(p_error_message, 1, 4000),
        SUBSTR(p_error_stack, 1, 4000),
        SUBSTR(p_parameters, 1, 4000),
        SYSDATE, USER
    );
    COMMIT;
END log_error;
/

-- Using the error logger
CREATE OR REPLACE PROCEDURE process_order(
    p_order_id NUMBER,
    p_customer_id NUMBER
)
IS
    v_proc_name CONSTANT VARCHAR2(100) := 'PROCESS_ORDER';
    v_params VARCHAR2(1000);
BEGIN
    v_params := 'order_id=' || p_order_id || ', customer_id=' || p_customer_id;
    
    -- Processing logic here
    NULL;
    
EXCEPTION
    WHEN OTHERS THEN
        log_error(
            p_procedure_name => v_proc_name,
            p_error_code => SQLCODE,
            p_error_message => SQLERRM,
            p_error_stack => DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
            p_parameters => v_params
        );
        RAISE;
END process_order;
/

-- -----------------------------------------------------------------------------
-- 9. Exception Handling in Loops
-- -----------------------------------------------------------------------------

DECLARE
    v_error_count NUMBER := 0;
BEGIN
    FOR emp IN (SELECT employee_id, salary FROM employees WHERE department_id = 60) LOOP
        BEGIN
            -- Process each employee
            UPDATE employees
            SET salary = emp.salary * 1.1
            WHERE employee_id = emp.employee_id;
            
            -- Simulate occasional error
            IF MOD(emp.employee_id, 3) = 0 THEN
                RAISE_APPLICATION_ERROR(-20001, 'Simulated error');
            END IF;
            
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('Error processing employee ' || emp.employee_id || ': ' || SQLERRM);
                -- Continue to next iteration
        END;
    END LOOP;
    
    IF v_error_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Total errors: ' || v_error_count);
    END IF;
    
    ROLLBACK;
END;
/

-- Continue on error with SAVEPOINT
DECLARE
    v_success_count NUMBER := 0;
    v_error_count NUMBER := 0;
BEGIN
    FOR i IN 1..10 LOOP
        SAVEPOINT before_iteration;
        
        BEGIN
            -- Some operation
            IF MOD(i, 3) = 0 THEN
                RAISE_APPLICATION_ERROR(-20001, 'Error on iteration ' || i);
            END IF;
            
            v_success_count := v_success_count + 1;
            
        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK TO before_iteration;
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('Rolled back iteration ' || i);
        END;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('Success: ' || v_success_count || ', Errors: ' || v_error_count);
    COMMIT;
END;
/

-- -----------------------------------------------------------------------------
-- 10. Re-raising Exceptions
-- -----------------------------------------------------------------------------

DECLARE
    e_custom EXCEPTION;
BEGIN
    BEGIN
        RAISE e_custom;
    EXCEPTION
        WHEN e_custom THEN
            DBMS_OUTPUT.PUT_LINE('Handling and re-raising');
            RAISE;  -- Re-raise same exception
    END;
EXCEPTION
    WHEN e_custom THEN
        DBMS_OUTPUT.PUT_LINE('Caught at outer level');
END;
/

-- Re-raise with different exception
DECLARE
    e_validation_error EXCEPTION;
    e_processing_error EXCEPTION;
BEGIN
    BEGIN
        RAISE e_validation_error;
    EXCEPTION
        WHEN e_validation_error THEN
            DBMS_OUTPUT.PUT_LINE('Converting validation to processing error');
            RAISE e_processing_error;  -- Raise different exception
    END;
EXCEPTION
    WHEN e_processing_error THEN
        DBMS_OUTPUT.PUT_LINE('Caught processing error');
END;
/

-- -----------------------------------------------------------------------------
-- 11. Exception Variables
-- -----------------------------------------------------------------------------

DECLARE
    v_error_code NUMBER;
    v_error_msg VARCHAR2(4000);
    v_error_stack VARCHAR2(4000);
    v_backtrace VARCHAR2(4000);
BEGIN
    -- Force an error
    EXECUTE IMMEDIATE 'SELECT * FROM nonexistent_table';
    
EXCEPTION
    WHEN OTHERS THEN
        -- Capture error information
        v_error_code := SQLCODE;
        v_error_msg := SQLERRM;
        v_error_stack := DBMS_UTILITY.FORMAT_ERROR_STACK;
        v_backtrace := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        
        DBMS_OUTPUT.PUT_LINE('Code: ' || v_error_code);
        DBMS_OUTPUT.PUT_LINE('Message: ' || v_error_msg);
        DBMS_OUTPUT.PUT_LINE('Stack: ' || v_error_stack);
        DBMS_OUTPUT.PUT_LINE('Backtrace: ' || v_backtrace);
        
        -- Can also use SQLERRM with specific code
        DBMS_OUTPUT.PUT_LINE('Message for -942: ' || SQLERRM(-942));
END;
/

-- -----------------------------------------------------------------------------
-- 12. Exception Package Pattern
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE exception_pkg AS
    -- Custom exceptions
    e_validation_failed EXCEPTION;
    e_business_rule_violated EXCEPTION;
    e_data_not_found EXCEPTION;
    e_duplicate_record EXCEPTION;
    e_insufficient_privileges EXCEPTION;
    
    -- Error codes
    c_validation_failed CONSTANT NUMBER := -20001;
    c_business_rule_violated CONSTANT NUMBER := -20002;
    c_data_not_found CONSTANT NUMBER := -20003;
    c_duplicate_record CONSTANT NUMBER := -20004;
    c_insufficient_privileges CONSTANT NUMBER := -20005;
    
    PRAGMA EXCEPTION_INIT(e_validation_failed, -20001);
    PRAGMA EXCEPTION_INIT(e_business_rule_violated, -20002);
    PRAGMA EXCEPTION_INIT(e_data_not_found, -20003);
    PRAGMA EXCEPTION_INIT(e_duplicate_record, -20004);
    PRAGMA EXCEPTION_INIT(e_insufficient_privileges, -20005);
    
    -- Procedures to raise with consistent messages
    PROCEDURE raise_validation_error(p_message VARCHAR2);
    PROCEDURE raise_business_error(p_message VARCHAR2);
    PROCEDURE raise_not_found(p_entity VARCHAR2, p_id VARCHAR2);
    PROCEDURE raise_duplicate(p_entity VARCHAR2, p_id VARCHAR2);
    
END exception_pkg;
/

CREATE OR REPLACE PACKAGE BODY exception_pkg AS
    
    PROCEDURE raise_validation_error(p_message VARCHAR2) IS
    BEGIN
        RAISE_APPLICATION_ERROR(c_validation_failed, 'Validation Error: ' || p_message);
    END;
    
    PROCEDURE raise_business_error(p_message VARCHAR2) IS
    BEGIN
        RAISE_APPLICATION_ERROR(c_business_rule_violated, 'Business Rule Violation: ' || p_message);
    END;
    
    PROCEDURE raise_not_found(p_entity VARCHAR2, p_id VARCHAR2) IS
    BEGIN
        RAISE_APPLICATION_ERROR(c_data_not_found, p_entity || ' not found with ID: ' || p_id);
    END;
    
    PROCEDURE raise_duplicate(p_entity VARCHAR2, p_id VARCHAR2) IS
    BEGIN
        RAISE_APPLICATION_ERROR(c_duplicate_record, 'Duplicate ' || p_entity || ' with ID: ' || p_id);
    END;
    
END exception_pkg;
/

-- Using the exception package
DECLARE
BEGIN
    -- Use the package
    exception_pkg.raise_not_found('Employee', '999');
EXCEPTION
    WHEN exception_pkg.e_data_not_found THEN
        DBMS_OUTPUT.PUT_LINE('Caught: ' || SQLERRM);
END;
/

