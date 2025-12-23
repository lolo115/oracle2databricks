-- ============================================================================
-- File: 18_plsql_packages.sql
-- Description: PL/SQL packages - specifications and bodies
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Simple Package
-- -----------------------------------------------------------------------------

-- Package specification
CREATE OR REPLACE PACKAGE emp_util AS
    -- Public constant
    c_max_salary CONSTANT NUMBER := 100000;
    
    -- Public variable
    g_last_error VARCHAR2(4000);
    
    -- Public function
    FUNCTION get_full_name(p_emp_id NUMBER) RETURN VARCHAR2;
    
    -- Public procedure
    PROCEDURE print_employee(p_emp_id NUMBER);
    
END emp_util;
/

-- Package body
CREATE OR REPLACE PACKAGE BODY emp_util AS
    
    -- Private variable
    g_call_count NUMBER := 0;
    
    -- Private procedure (not in spec)
    PROCEDURE increment_counter IS
    BEGIN
        g_call_count := g_call_count + 1;
    END increment_counter;
    
    -- Implementation of public function
    FUNCTION get_full_name(p_emp_id NUMBER) RETURN VARCHAR2 IS
        v_name VARCHAR2(100);
    BEGIN
        increment_counter;
        
        SELECT first_name || ' ' || last_name INTO v_name
        FROM employees
        WHERE employee_id = p_emp_id;
        
        RETURN v_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            g_last_error := 'Employee ' || p_emp_id || ' not found';
            RETURN NULL;
    END get_full_name;
    
    -- Implementation of public procedure
    PROCEDURE print_employee(p_emp_id NUMBER) IS
        v_emp employees%ROWTYPE;
    BEGIN
        increment_counter;
        
        SELECT * INTO v_emp
        FROM employees
        WHERE employee_id = p_emp_id;
        
        DBMS_OUTPUT.PUT_LINE('ID: ' || v_emp.employee_id);
        DBMS_OUTPUT.PUT_LINE('Name: ' || v_emp.first_name || ' ' || v_emp.last_name);
        DBMS_OUTPUT.PUT_LINE('Salary: $' || v_emp.salary);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            g_last_error := 'Employee not found';
            DBMS_OUTPUT.PUT_LINE(g_last_error);
    END print_employee;
    
END emp_util;
/

-- -----------------------------------------------------------------------------
-- 2. Package with Initialization
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE config_pkg AS
    -- Configuration constants loaded at initialization
    g_app_name VARCHAR2(100);
    g_version VARCHAR2(20);
    g_environment VARCHAR2(20);
    g_debug_mode BOOLEAN;
    
    FUNCTION get_config(p_name VARCHAR2) RETURN VARCHAR2;
    PROCEDURE set_debug(p_enabled BOOLEAN);
    
END config_pkg;
/

CREATE OR REPLACE PACKAGE BODY config_pkg AS
    
    -- Private cache
    TYPE t_config_cache IS TABLE OF VARCHAR2(4000) INDEX BY VARCHAR2(100);
    g_cache t_config_cache;
    
    FUNCTION get_config(p_name VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        IF g_cache.EXISTS(p_name) THEN
            RETURN g_cache(p_name);
        ELSE
            RETURN NULL;
        END IF;
    END get_config;
    
    PROCEDURE set_debug(p_enabled BOOLEAN) IS
    BEGIN
        g_debug_mode := p_enabled;
    END set_debug;
    
    -- Private initialization procedure
    PROCEDURE load_configuration IS
    BEGIN
        -- Load from config table (if exists)
        FOR rec IN (SELECT config_name, config_value FROM app_config) LOOP
            g_cache(rec.config_name) := rec.config_value;
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            NULL; -- Table might not exist
    END load_configuration;
    
-- Package initialization block
BEGIN
    g_app_name := 'HR Application';
    g_version := '1.0.0';
    g_environment := NVL(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER'), 'PROD');
    g_debug_mode := FALSE;
    
    load_configuration;
END config_pkg;
/

-- -----------------------------------------------------------------------------
-- 3. Package with Types
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE employee_types AS
    
    -- Record type
    TYPE t_employee_rec IS RECORD (
        employee_id    NUMBER,
        full_name      VARCHAR2(100),
        email          VARCHAR2(100),
        salary         NUMBER,
        department_id  NUMBER,
        hire_date      DATE
    );
    
    -- Table type of records
    TYPE t_employee_tab IS TABLE OF t_employee_rec INDEX BY PLS_INTEGER;
    
    -- Simple collection types
    TYPE t_number_list IS TABLE OF NUMBER;
    TYPE t_varchar_list IS TABLE OF VARCHAR2(4000);
    TYPE t_id_table IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    
    -- Associative array
    TYPE t_salary_by_id IS TABLE OF NUMBER INDEX BY VARCHAR2(20);
    
    -- REF CURSOR type
    TYPE t_emp_cursor IS REF CURSOR RETURN employees%ROWTYPE;
    
    -- Subtype
    SUBTYPE t_emp_id IS employees.employee_id%TYPE;
    SUBTYPE t_money IS NUMBER(12,2);
    
END employee_types;
/

-- -----------------------------------------------------------------------------
-- 4. Package with Overloaded Subprograms
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE employee_mgmt AS
    
    -- Overloaded find procedures
    PROCEDURE find_employee(p_emp_id NUMBER, p_result OUT employees%ROWTYPE);
    PROCEDURE find_employee(p_email VARCHAR2, p_result OUT employees%ROWTYPE);
    PROCEDURE find_employee(p_first_name VARCHAR2, p_last_name VARCHAR2, p_result OUT employees%ROWTYPE);
    
    -- Overloaded add procedures
    FUNCTION add_employee(p_emp employees%ROWTYPE) RETURN NUMBER;
    FUNCTION add_employee(p_first_name VARCHAR2, p_last_name VARCHAR2, p_email VARCHAR2, p_job_id VARCHAR2) RETURN NUMBER;
    
    -- Overloaded print procedures
    PROCEDURE print_info(p_emp_id NUMBER);
    PROCEDURE print_info(p_dept_id NUMBER, p_include_salary BOOLEAN);
    
END employee_mgmt;
/

CREATE OR REPLACE PACKAGE BODY employee_mgmt AS
    
    PROCEDURE find_employee(p_emp_id NUMBER, p_result OUT employees%ROWTYPE) IS
    BEGIN
        SELECT * INTO p_result FROM employees WHERE employee_id = p_emp_id;
    END find_employee;
    
    PROCEDURE find_employee(p_email VARCHAR2, p_result OUT employees%ROWTYPE) IS
    BEGIN
        SELECT * INTO p_result FROM employees WHERE UPPER(email) = UPPER(p_email);
    END find_employee;
    
    PROCEDURE find_employee(p_first_name VARCHAR2, p_last_name VARCHAR2, p_result OUT employees%ROWTYPE) IS
    BEGIN
        SELECT * INTO p_result FROM employees 
        WHERE UPPER(first_name) = UPPER(p_first_name) 
        AND UPPER(last_name) = UPPER(p_last_name);
    END find_employee;
    
    FUNCTION add_employee(p_emp employees%ROWTYPE) RETURN NUMBER IS
        v_emp_id NUMBER;
    BEGIN
        SELECT employees_seq.NEXTVAL INTO v_emp_id FROM DUAL;
        INSERT INTO employees VALUES p_emp;
        RETURN v_emp_id;
    END add_employee;
    
    FUNCTION add_employee(p_first_name VARCHAR2, p_last_name VARCHAR2, p_email VARCHAR2, p_job_id VARCHAR2) RETURN NUMBER IS
        v_emp_id NUMBER;
    BEGIN
        SELECT employees_seq.NEXTVAL INTO v_emp_id FROM DUAL;
        INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id, salary)
        VALUES (v_emp_id, p_first_name, p_last_name, p_email, SYSDATE, p_job_id, 5000);
        RETURN v_emp_id;
    END add_employee;
    
    PROCEDURE print_info(p_emp_id NUMBER) IS
        v_emp employees%ROWTYPE;
    BEGIN
        SELECT * INTO v_emp FROM employees WHERE employee_id = p_emp_id;
        DBMS_OUTPUT.PUT_LINE('Employee: ' || v_emp.first_name || ' ' || v_emp.last_name);
    END print_info;
    
    PROCEDURE print_info(p_dept_id NUMBER, p_include_salary BOOLEAN) IS
    BEGIN
        FOR emp IN (SELECT * FROM employees WHERE department_id = p_dept_id) LOOP
            IF p_include_salary THEN
                DBMS_OUTPUT.PUT_LINE(emp.first_name || ': $' || emp.salary);
            ELSE
                DBMS_OUTPUT.PUT_LINE(emp.first_name || ' ' || emp.last_name);
            END IF;
        END LOOP;
    END print_info;
    
END employee_mgmt;
/

-- -----------------------------------------------------------------------------
-- 5. Package with SERIALLY_REUSABLE
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE session_data 
AUTHID CURRENT_USER
AS
    PRAGMA SERIALLY_REUSABLE;  -- Package state reset after each call
    
    g_counter NUMBER := 0;
    
    PROCEDURE increment;
    FUNCTION get_counter RETURN NUMBER;
    
END session_data;
/

CREATE OR REPLACE PACKAGE BODY session_data AS
    PRAGMA SERIALLY_REUSABLE;
    
    PROCEDURE increment IS
    BEGIN
        g_counter := g_counter + 1;
    END increment;
    
    FUNCTION get_counter RETURN NUMBER IS
    BEGIN
        RETURN g_counter;
    END get_counter;
    
END session_data;
/

-- -----------------------------------------------------------------------------
-- 6. Package for Data Validation
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE validation_pkg AS
    
    -- Validation result type
    TYPE t_validation_result IS RECORD (
        is_valid BOOLEAN,
        error_code VARCHAR2(20),
        error_message VARCHAR2(4000)
    );
    
    -- Validation functions
    FUNCTION validate_email(p_email VARCHAR2) RETURN t_validation_result;
    FUNCTION validate_phone(p_phone VARCHAR2) RETURN t_validation_result;
    FUNCTION validate_salary(p_salary NUMBER, p_job_id VARCHAR2) RETURN t_validation_result;
    FUNCTION validate_date_range(p_start DATE, p_end DATE) RETURN t_validation_result;
    
    -- Convenience function
    FUNCTION is_valid_email(p_email VARCHAR2) RETURN BOOLEAN;
    
END validation_pkg;
/

CREATE OR REPLACE PACKAGE BODY validation_pkg AS
    
    -- Email validation
    FUNCTION validate_email(p_email VARCHAR2) RETURN t_validation_result IS
        v_result t_validation_result;
    BEGIN
        v_result.is_valid := TRUE;
        v_result.error_code := NULL;
        v_result.error_message := NULL;
        
        IF p_email IS NULL THEN
            v_result.is_valid := FALSE;
            v_result.error_code := 'EMAIL_NULL';
            v_result.error_message := 'Email cannot be null';
        ELSIF NOT REGEXP_LIKE(p_email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN
            v_result.is_valid := FALSE;
            v_result.error_code := 'EMAIL_INVALID';
            v_result.error_message := 'Email format is invalid';
        END IF;
        
        RETURN v_result;
    END validate_email;
    
    -- Phone validation
    FUNCTION validate_phone(p_phone VARCHAR2) RETURN t_validation_result IS
        v_result t_validation_result;
        v_digits VARCHAR2(20);
    BEGIN
        v_result.is_valid := TRUE;
        
        IF p_phone IS NOT NULL THEN
            v_digits := REGEXP_REPLACE(p_phone, '[^0-9]', '');
            IF LENGTH(v_digits) < 10 OR LENGTH(v_digits) > 15 THEN
                v_result.is_valid := FALSE;
                v_result.error_code := 'PHONE_INVALID';
                v_result.error_message := 'Phone must have 10-15 digits';
            END IF;
        END IF;
        
        RETURN v_result;
    END validate_phone;
    
    -- Salary validation
    FUNCTION validate_salary(p_salary NUMBER, p_job_id VARCHAR2) RETURN t_validation_result IS
        v_result t_validation_result;
        v_min_sal NUMBER;
        v_max_sal NUMBER;
    BEGIN
        v_result.is_valid := TRUE;
        
        IF p_salary IS NULL OR p_salary < 0 THEN
            v_result.is_valid := FALSE;
            v_result.error_code := 'SALARY_INVALID';
            v_result.error_message := 'Salary must be a positive number';
            RETURN v_result;
        END IF;
        
        BEGIN
            SELECT min_salary, max_salary INTO v_min_sal, v_max_sal
            FROM jobs WHERE job_id = p_job_id;
            
            IF p_salary < v_min_sal OR p_salary > v_max_sal THEN
                v_result.is_valid := FALSE;
                v_result.error_code := 'SALARY_RANGE';
                v_result.error_message := 'Salary must be between ' || v_min_sal || ' and ' || v_max_sal;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_result.is_valid := FALSE;
                v_result.error_code := 'JOB_INVALID';
                v_result.error_message := 'Invalid job ID';
        END;
        
        RETURN v_result;
    END validate_salary;
    
    -- Date range validation
    FUNCTION validate_date_range(p_start DATE, p_end DATE) RETURN t_validation_result IS
        v_result t_validation_result;
    BEGIN
        v_result.is_valid := TRUE;
        
        IF p_start IS NULL OR p_end IS NULL THEN
            v_result.is_valid := FALSE;
            v_result.error_code := 'DATE_NULL';
            v_result.error_message := 'Start and end dates are required';
        ELSIF p_start > p_end THEN
            v_result.is_valid := FALSE;
            v_result.error_code := 'DATE_RANGE';
            v_result.error_message := 'Start date must be before end date';
        END IF;
        
        RETURN v_result;
    END validate_date_range;
    
    -- Convenience function
    FUNCTION is_valid_email(p_email VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN validate_email(p_email).is_valid;
    END is_valid_email;
    
END validation_pkg;
/

-- -----------------------------------------------------------------------------
-- 7. Package for Logging
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE log_pkg AS
    
    -- Log levels
    c_debug   CONSTANT NUMBER := 1;
    c_info    CONSTANT NUMBER := 2;
    c_warning CONSTANT NUMBER := 3;
    c_error   CONSTANT NUMBER := 4;
    c_fatal   CONSTANT NUMBER := 5;
    
    -- Current log level (can be changed)
    g_log_level NUMBER := c_info;
    
    -- Logging procedures
    PROCEDURE log(p_level NUMBER, p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL);
    PROCEDURE debug(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL);
    PROCEDURE info(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL);
    PROCEDURE warning(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL);
    PROCEDURE error(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL);
    PROCEDURE fatal(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL);
    
    -- Configuration
    PROCEDURE set_log_level(p_level NUMBER);
    FUNCTION get_level_name(p_level NUMBER) RETURN VARCHAR2;
    
END log_pkg;
/

CREATE OR REPLACE PACKAGE BODY log_pkg AS
    
    PROCEDURE write_log(p_level NUMBER, p_message VARCHAR2, p_context VARCHAR2) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO application_log (
            log_id, log_level, log_level_name, message, context,
            log_timestamp, session_user, client_info
        ) VALUES (
            app_log_seq.NEXTVAL, p_level, get_level_name(p_level), 
            SUBSTR(p_message, 1, 4000), p_context,
            SYSTIMESTAMP, USER, SYS_CONTEXT('USERENV', 'CLIENT_INFO')
        );
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            -- Silently fail logging errors
            NULL;
    END write_log;
    
    PROCEDURE log(p_level NUMBER, p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL) IS
    BEGIN
        IF p_level >= g_log_level THEN
            write_log(p_level, p_message, p_context);
            
            -- Also output to DBMS_OUTPUT for development
            DBMS_OUTPUT.PUT_LINE('[' || get_level_name(p_level) || '] ' || p_message);
        END IF;
    END log;
    
    PROCEDURE debug(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL) IS
    BEGIN
        log(c_debug, p_message, p_context);
    END debug;
    
    PROCEDURE info(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL) IS
    BEGIN
        log(c_info, p_message, p_context);
    END info;
    
    PROCEDURE warning(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL) IS
    BEGIN
        log(c_warning, p_message, p_context);
    END warning;
    
    PROCEDURE error(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL) IS
    BEGIN
        log(c_error, p_message, p_context);
    END error;
    
    PROCEDURE fatal(p_message VARCHAR2, p_context VARCHAR2 DEFAULT NULL) IS
    BEGIN
        log(c_fatal, p_message, p_context);
    END fatal;
    
    PROCEDURE set_log_level(p_level NUMBER) IS
    BEGIN
        IF p_level BETWEEN c_debug AND c_fatal THEN
            g_log_level := p_level;
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Invalid log level: ' || p_level);
        END IF;
    END set_log_level;
    
    FUNCTION get_level_name(p_level NUMBER) RETURN VARCHAR2 IS
    BEGIN
        RETURN CASE p_level
            WHEN c_debug THEN 'DEBUG'
            WHEN c_info THEN 'INFO'
            WHEN c_warning THEN 'WARNING'
            WHEN c_error THEN 'ERROR'
            WHEN c_fatal THEN 'FATAL'
            ELSE 'UNKNOWN'
        END;
    END get_level_name;
    
END log_pkg;
/

-- -----------------------------------------------------------------------------
-- 8. Package with Forward Declarations
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE calc_pkg AS
    FUNCTION calculate(p_operation VARCHAR2, p_value1 NUMBER, p_value2 NUMBER) RETURN NUMBER;
END calc_pkg;
/

CREATE OR REPLACE PACKAGE BODY calc_pkg AS
    
    -- Forward declarations (for mutual recursion or ordering)
    FUNCTION add_values(p_a NUMBER, p_b NUMBER) RETURN NUMBER;
    FUNCTION subtract_values(p_a NUMBER, p_b NUMBER) RETURN NUMBER;
    FUNCTION multiply_values(p_a NUMBER, p_b NUMBER) RETURN NUMBER;
    FUNCTION divide_values(p_a NUMBER, p_b NUMBER) RETURN NUMBER;
    
    -- Implementation
    FUNCTION add_values(p_a NUMBER, p_b NUMBER) RETURN NUMBER IS
    BEGIN
        RETURN p_a + p_b;
    END;
    
    FUNCTION subtract_values(p_a NUMBER, p_b NUMBER) RETURN NUMBER IS
    BEGIN
        RETURN p_a - p_b;
    END;
    
    FUNCTION multiply_values(p_a NUMBER, p_b NUMBER) RETURN NUMBER IS
    BEGIN
        RETURN p_a * p_b;
    END;
    
    FUNCTION divide_values(p_a NUMBER, p_b NUMBER) RETURN NUMBER IS
    BEGIN
        IF p_b = 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Division by zero');
        END IF;
        RETURN p_a / p_b;
    END;
    
    -- Main function using forward-declared functions
    FUNCTION calculate(p_operation VARCHAR2, p_value1 NUMBER, p_value2 NUMBER) RETURN NUMBER IS
    BEGIN
        RETURN CASE UPPER(p_operation)
            WHEN 'ADD' THEN add_values(p_value1, p_value2)
            WHEN 'SUBTRACT' THEN subtract_values(p_value1, p_value2)
            WHEN 'MULTIPLY' THEN multiply_values(p_value1, p_value2)
            WHEN 'DIVIDE' THEN divide_values(p_value1, p_value2)
            ELSE NULL
        END;
    END calculate;
    
END calc_pkg;
/

-- -----------------------------------------------------------------------------
-- 9. Package State Management
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE session_context AS
    
    -- Set context values
    PROCEDURE set_value(p_name VARCHAR2, p_value VARCHAR2);
    PROCEDURE set_user_id(p_user_id NUMBER);
    PROCEDURE set_org_id(p_org_id NUMBER);
    
    -- Get context values
    FUNCTION get_value(p_name VARCHAR2) RETURN VARCHAR2;
    FUNCTION get_user_id RETURN NUMBER;
    FUNCTION get_org_id RETURN NUMBER;
    
    -- Clear context
    PROCEDURE clear_all;
    PROCEDURE clear(p_name VARCHAR2);
    
END session_context;
/

CREATE OR REPLACE PACKAGE BODY session_context AS
    
    -- Private state
    TYPE t_context IS TABLE OF VARCHAR2(4000) INDEX BY VARCHAR2(100);
    g_context t_context;
    
    g_user_id NUMBER;
    g_org_id NUMBER;
    
    PROCEDURE set_value(p_name VARCHAR2, p_value VARCHAR2) IS
    BEGIN
        g_context(UPPER(p_name)) := p_value;
    END set_value;
    
    PROCEDURE set_user_id(p_user_id NUMBER) IS
    BEGIN
        g_user_id := p_user_id;
    END set_user_id;
    
    PROCEDURE set_org_id(p_org_id NUMBER) IS
    BEGIN
        g_org_id := p_org_id;
    END set_org_id;
    
    FUNCTION get_value(p_name VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        IF g_context.EXISTS(UPPER(p_name)) THEN
            RETURN g_context(UPPER(p_name));
        END IF;
        RETURN NULL;
    END get_value;
    
    FUNCTION get_user_id RETURN NUMBER IS
    BEGIN
        RETURN g_user_id;
    END get_user_id;
    
    FUNCTION get_org_id RETURN NUMBER IS
    BEGIN
        RETURN g_org_id;
    END get_org_id;
    
    PROCEDURE clear_all IS
    BEGIN
        g_context.DELETE;
        g_user_id := NULL;
        g_org_id := NULL;
    END clear_all;
    
    PROCEDURE clear(p_name VARCHAR2) IS
    BEGIN
        IF g_context.EXISTS(UPPER(p_name)) THEN
            g_context.DELETE(UPPER(p_name));
        END IF;
    END clear;
    
END session_context;
/

-- -----------------------------------------------------------------------------
-- 10. Complete Business Package Example
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE order_processing AS
    
    -- Custom exceptions
    e_order_not_found EXCEPTION;
    e_insufficient_inventory EXCEPTION;
    e_invalid_status EXCEPTION;
    
    PRAGMA EXCEPTION_INIT(e_order_not_found, -20101);
    PRAGMA EXCEPTION_INIT(e_insufficient_inventory, -20102);
    PRAGMA EXCEPTION_INIT(e_invalid_status, -20103);
    
    -- Order status constants
    c_status_pending   CONSTANT VARCHAR2(20) := 'PENDING';
    c_status_confirmed CONSTANT VARCHAR2(20) := 'CONFIRMED';
    c_status_shipped   CONSTANT VARCHAR2(20) := 'SHIPPED';
    c_status_delivered CONSTANT VARCHAR2(20) := 'DELIVERED';
    c_status_cancelled CONSTANT VARCHAR2(20) := 'CANCELLED';
    
    -- Types
    TYPE t_order_line IS RECORD (
        product_id NUMBER,
        quantity NUMBER,
        unit_price NUMBER
    );
    TYPE t_order_lines IS TABLE OF t_order_line INDEX BY PLS_INTEGER;
    
    -- Order management
    FUNCTION create_order(p_customer_id NUMBER, p_lines t_order_lines) RETURN NUMBER;
    PROCEDURE update_order_status(p_order_id NUMBER, p_new_status VARCHAR2);
    PROCEDURE cancel_order(p_order_id NUMBER);
    FUNCTION get_order_total(p_order_id NUMBER) RETURN NUMBER;
    
    -- Reporting
    PROCEDURE get_order_details(p_order_id NUMBER, p_cursor OUT SYS_REFCURSOR);
    FUNCTION get_customer_orders(p_customer_id NUMBER) RETURN SYS_REFCURSOR;
    
END order_processing;
/

CREATE OR REPLACE PACKAGE BODY order_processing AS
    
    -- Private: Validate order exists
    FUNCTION order_exists(p_order_id NUMBER) RETURN BOOLEAN IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count FROM orders WHERE order_id = p_order_id;
        RETURN v_count > 0;
    END order_exists;
    
    -- Private: Check inventory
    PROCEDURE check_inventory(p_lines t_order_lines) IS
        v_available NUMBER;
    BEGIN
        FOR i IN 1..p_lines.COUNT LOOP
            SELECT quantity_available INTO v_available
            FROM inventory
            WHERE product_id = p_lines(i).product_id;
            
            IF v_available < p_lines(i).quantity THEN
                RAISE e_insufficient_inventory;
            END IF;
        END LOOP;
    END check_inventory;
    
    -- Private: Reserve inventory
    PROCEDURE reserve_inventory(p_lines t_order_lines) IS
    BEGIN
        FOR i IN 1..p_lines.COUNT LOOP
            UPDATE inventory
            SET quantity_available = quantity_available - p_lines(i).quantity,
                quantity_reserved = quantity_reserved + p_lines(i).quantity
            WHERE product_id = p_lines(i).product_id;
        END LOOP;
    END reserve_inventory;
    
    -- Create order
    FUNCTION create_order(p_customer_id NUMBER, p_lines t_order_lines) RETURN NUMBER IS
        v_order_id NUMBER;
    BEGIN
        log_pkg.info('Creating order for customer ' || p_customer_id);
        
        -- Check inventory
        check_inventory(p_lines);
        
        -- Create order header
        SELECT orders_seq.NEXTVAL INTO v_order_id FROM DUAL;
        
        INSERT INTO orders (order_id, customer_id, order_date, status, total_amount)
        VALUES (v_order_id, p_customer_id, SYSDATE, c_status_pending, 0);
        
        -- Create order lines
        FOR i IN 1..p_lines.COUNT LOOP
            INSERT INTO order_lines (order_id, line_number, product_id, quantity, unit_price)
            VALUES (v_order_id, i, p_lines(i).product_id, p_lines(i).quantity, p_lines(i).unit_price);
        END LOOP;
        
        -- Update total
        UPDATE orders
        SET total_amount = get_order_total(v_order_id)
        WHERE order_id = v_order_id;
        
        -- Reserve inventory
        reserve_inventory(p_lines);
        
        COMMIT;
        
        log_pkg.info('Order ' || v_order_id || ' created successfully');
        RETURN v_order_id;
        
    EXCEPTION
        WHEN e_insufficient_inventory THEN
            ROLLBACK;
            log_pkg.error('Insufficient inventory for order');
            RAISE;
        WHEN OTHERS THEN
            ROLLBACK;
            log_pkg.error('Error creating order: ' || SQLERRM);
            RAISE;
    END create_order;
    
    -- Update order status
    PROCEDURE update_order_status(p_order_id NUMBER, p_new_status VARCHAR2) IS
        v_current_status VARCHAR2(20);
    BEGIN
        IF NOT order_exists(p_order_id) THEN
            RAISE e_order_not_found;
        END IF;
        
        SELECT status INTO v_current_status FROM orders WHERE order_id = p_order_id;
        
        -- Validate status transition
        IF v_current_status = c_status_cancelled THEN
            RAISE e_invalid_status;
        END IF;
        
        UPDATE orders
        SET status = p_new_status,
            last_updated = SYSDATE
        WHERE order_id = p_order_id;
        
        -- Insert status history
        INSERT INTO order_status_history (order_id, old_status, new_status, change_date)
        VALUES (p_order_id, v_current_status, p_new_status, SYSDATE);
        
        COMMIT;
        
        log_pkg.info('Order ' || p_order_id || ' status changed from ' || 
                     v_current_status || ' to ' || p_new_status);
    END update_order_status;
    
    -- Cancel order
    PROCEDURE cancel_order(p_order_id NUMBER) IS
    BEGIN
        update_order_status(p_order_id, c_status_cancelled);
        
        -- Release reserved inventory
        FOR line IN (SELECT product_id, quantity FROM order_lines WHERE order_id = p_order_id) LOOP
            UPDATE inventory
            SET quantity_available = quantity_available + line.quantity,
                quantity_reserved = quantity_reserved - line.quantity
            WHERE product_id = line.product_id;
        END LOOP;
        
        COMMIT;
    END cancel_order;
    
    -- Get order total
    FUNCTION get_order_total(p_order_id NUMBER) RETURN NUMBER IS
        v_total NUMBER;
    BEGIN
        SELECT NVL(SUM(quantity * unit_price), 0) INTO v_total
        FROM order_lines
        WHERE order_id = p_order_id;
        
        RETURN v_total;
    END get_order_total;
    
    -- Get order details
    PROCEDURE get_order_details(p_order_id NUMBER, p_cursor OUT SYS_REFCURSOR) IS
    BEGIN
        OPEN p_cursor FOR
            SELECT o.order_id, o.customer_id, o.order_date, o.status, o.total_amount,
                   ol.line_number, ol.product_id, p.product_name, 
                   ol.quantity, ol.unit_price, ol.quantity * ol.unit_price AS line_total
            FROM orders o
            JOIN order_lines ol ON o.order_id = ol.order_id
            JOIN products p ON ol.product_id = p.product_id
            WHERE o.order_id = p_order_id
            ORDER BY ol.line_number;
    END get_order_details;
    
    -- Get customer orders
    FUNCTION get_customer_orders(p_customer_id NUMBER) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        OPEN v_cursor FOR
            SELECT order_id, order_date, status, total_amount
            FROM orders
            WHERE customer_id = p_customer_id
            ORDER BY order_date DESC;
        
        RETURN v_cursor;
    END get_customer_orders;
    
END order_processing;
/

