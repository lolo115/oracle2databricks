-- ============================================================================
-- File: 14_plsql_basics.sql
-- Description: PL/SQL anonymous blocks, variables, control structures
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Anonymous Block Structure
-- -----------------------------------------------------------------------------

-- Simplest block
BEGIN
    DBMS_OUTPUT.PUT_LINE('Hello, World!');
END;
/

-- Block with declaration section
DECLARE
    v_message VARCHAR2(100) := 'Hello from PL/SQL';
BEGIN
    DBMS_OUTPUT.PUT_LINE(v_message);
END;
/

-- Block with all sections
DECLARE
    v_result NUMBER;
BEGIN
    v_result := 10 + 20;
    DBMS_OUTPUT.PUT_LINE('Result: ' || v_result);
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

-- Nested blocks
DECLARE
    v_outer VARCHAR2(20) := 'Outer';
BEGIN
    DBMS_OUTPUT.PUT_LINE('Outer block: ' || v_outer);
    
    DECLARE
        v_inner VARCHAR2(20) := 'Inner';
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Inner block: ' || v_inner);
        DBMS_OUTPUT.PUT_LINE('Can see outer: ' || v_outer);
    END;
    
    -- v_inner not visible here
    DBMS_OUTPUT.PUT_LINE('Back to outer');
END;
/

-- -----------------------------------------------------------------------------
-- 2. Variable Declarations
-- -----------------------------------------------------------------------------

DECLARE
    -- Scalar types
    v_number NUMBER := 100;
    v_integer INTEGER := 42;
    v_decimal NUMBER(10,2) := 123.45;
    v_string VARCHAR2(100) := 'Hello';
    v_char CHAR(10) := 'Fixed';
    v_date DATE := SYSDATE;
    v_timestamp TIMESTAMP := SYSTIMESTAMP;
    v_boolean BOOLEAN := TRUE;
    
    -- Anchored types
    v_emp_salary employees.salary%TYPE;
    v_emp_name employees.first_name%TYPE;
    
    -- Constants
    c_tax_rate CONSTANT NUMBER := 0.25;
    c_company_name CONSTANT VARCHAR2(50) := 'Acme Corp';
    
    -- NOT NULL constraint
    v_count NUMBER NOT NULL := 0;
    
    -- Default values
    v_status VARCHAR2(20) DEFAULT 'ACTIVE';
    
BEGIN
    -- Variable assignment
    v_number := v_number + 50;
    v_string := v_string || ' World';
    v_date := v_date + 7;
    v_boolean := NOT v_boolean;
    
    -- Query into variables
    SELECT salary, first_name
    INTO v_emp_salary, v_emp_name
    FROM employees
    WHERE employee_id = 100;
    
    DBMS_OUTPUT.PUT_LINE('Employee: ' || v_emp_name || ', Salary: ' || v_emp_salary);
    DBMS_OUTPUT.PUT_LINE('Tax rate: ' || c_tax_rate);
END;
/

-- -----------------------------------------------------------------------------
-- 3. Record Types
-- -----------------------------------------------------------------------------

DECLARE
    -- %ROWTYPE for table structure
    v_emp_rec employees%ROWTYPE;
    
    -- User-defined record type
    TYPE t_customer_rec IS RECORD (
        customer_id    NUMBER,
        customer_name  VARCHAR2(100),
        email          VARCHAR2(100),
        balance        NUMBER(10,2) := 0
    );
    
    v_customer t_customer_rec;
    
    -- Record from cursor
    CURSOR c_emp IS 
        SELECT employee_id, first_name, last_name, salary
        FROM employees;
    v_emp_cursor c_emp%ROWTYPE;
    
BEGIN
    -- Populate ROWTYPE record
    SELECT *
    INTO v_emp_rec
    FROM employees
    WHERE employee_id = 100;
    
    DBMS_OUTPUT.PUT_LINE('Name: ' || v_emp_rec.first_name || ' ' || v_emp_rec.last_name);
    DBMS_OUTPUT.PUT_LINE('Salary: ' || v_emp_rec.salary);
    
    -- Populate user-defined record
    v_customer.customer_id := 1001;
    v_customer.customer_name := 'John Doe';
    v_customer.email := 'john.doe@email.com';
    v_customer.balance := 1500.00;
    
    DBMS_OUTPUT.PUT_LINE('Customer: ' || v_customer.customer_name);
END;
/

-- -----------------------------------------------------------------------------
-- 4. IF-THEN-ELSE Statements
-- -----------------------------------------------------------------------------

DECLARE
    v_salary NUMBER := 7500;
    v_bonus NUMBER;
    v_grade VARCHAR2(10);
BEGIN
    -- Simple IF
    IF v_salary > 10000 THEN
        DBMS_OUTPUT.PUT_LINE('High salary');
    END IF;
    
    -- IF-ELSE
    IF v_salary > 10000 THEN
        v_bonus := v_salary * 0.15;
    ELSE
        v_bonus := v_salary * 0.10;
    END IF;
    
    -- IF-ELSIF-ELSE
    IF v_salary >= 15000 THEN
        v_grade := 'A';
    ELSIF v_salary >= 10000 THEN
        v_grade := 'B';
    ELSIF v_salary >= 5000 THEN
        v_grade := 'C';
    ELSE
        v_grade := 'D';
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('Grade: ' || v_grade || ', Bonus: ' || v_bonus);
    
    -- Nested IF
    IF v_salary > 5000 THEN
        IF v_bonus > 500 THEN
            DBMS_OUTPUT.PUT_LINE('Good compensation package');
        END IF;
    END IF;
    
    -- Boolean conditions
    DECLARE
        v_is_manager BOOLEAN := TRUE;
        v_is_senior BOOLEAN := FALSE;
    BEGIN
        IF v_is_manager AND NOT v_is_senior THEN
            DBMS_OUTPUT.PUT_LINE('New manager');
        ELSIF v_is_manager AND v_is_senior THEN
            DBMS_OUTPUT.PUT_LINE('Senior manager');
        END IF;
    END;
END;
/

-- -----------------------------------------------------------------------------
-- 5. CASE Statements
-- -----------------------------------------------------------------------------

DECLARE
    v_department_id NUMBER := 60;
    v_department_name VARCHAR2(50);
    v_salary NUMBER := 8500;
    v_rating VARCHAR2(20);
BEGIN
    -- Simple CASE statement
    CASE v_department_id
        WHEN 10 THEN v_department_name := 'Administration';
        WHEN 20 THEN v_department_name := 'Marketing';
        WHEN 30 THEN v_department_name := 'Purchasing';
        WHEN 50 THEN v_department_name := 'Shipping';
        WHEN 60 THEN v_department_name := 'IT';
        WHEN 80 THEN v_department_name := 'Sales';
        WHEN 90 THEN v_department_name := 'Executive';
        ELSE v_department_name := 'Other';
    END CASE;
    
    DBMS_OUTPUT.PUT_LINE('Department: ' || v_department_name);
    
    -- Searched CASE statement
    CASE
        WHEN v_salary < 5000 THEN v_rating := 'Low';
        WHEN v_salary < 10000 THEN v_rating := 'Medium';
        WHEN v_salary < 15000 THEN v_rating := 'High';
        ELSE v_rating := 'Executive';
    END CASE;
    
    DBMS_OUTPUT.PUT_LINE('Salary Rating: ' || v_rating);
    
    -- CASE expression (returns value)
    v_rating := CASE v_department_id
        WHEN 90 THEN 'Executive'
        WHEN 80 THEN 'Revenue Generator'
        WHEN 60 THEN 'Cost Center'
        ELSE 'Support'
    END;
    
    DBMS_OUTPUT.PUT_LINE('Dept Type: ' || v_rating);
END;
/

-- -----------------------------------------------------------------------------
-- 6. Basic Loops
-- -----------------------------------------------------------------------------

DECLARE
    v_counter NUMBER := 1;
    v_sum NUMBER := 0;
BEGIN
    -- Simple LOOP (infinite until EXIT)
    LOOP
        v_sum := v_sum + v_counter;
        v_counter := v_counter + 1;
        EXIT WHEN v_counter > 10;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('Sum 1-10: ' || v_sum);
    
    -- LOOP with EXIT in middle
    v_counter := 1;
    LOOP
        IF v_counter > 5 THEN
            EXIT;
        END IF;
        DBMS_OUTPUT.PUT_LINE('Counter: ' || v_counter);
        v_counter := v_counter + 1;
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- 7. WHILE Loops
-- -----------------------------------------------------------------------------

DECLARE
    v_counter NUMBER := 1;
    v_factorial NUMBER := 1;
    v_n NUMBER := 5;
BEGIN
    -- Basic WHILE
    WHILE v_counter <= 10 LOOP
        DBMS_OUTPUT.PUT_LINE('Iteration: ' || v_counter);
        v_counter := v_counter + 1;
    END LOOP;
    
    -- Calculate factorial
    v_counter := 1;
    WHILE v_counter <= v_n LOOP
        v_factorial := v_factorial * v_counter;
        v_counter := v_counter + 1;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE(v_n || '! = ' || v_factorial);
END;
/

-- -----------------------------------------------------------------------------
-- 8. FOR Loops
-- -----------------------------------------------------------------------------

DECLARE
    v_sum NUMBER := 0;
BEGIN
    -- Basic FOR loop (ascending)
    FOR i IN 1..10 LOOP
        DBMS_OUTPUT.PUT_LINE('i = ' || i);
        v_sum := v_sum + i;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('Sum: ' || v_sum);
    
    -- FOR loop descending
    FOR i IN REVERSE 1..5 LOOP
        DBMS_OUTPUT.PUT_LINE('Countdown: ' || i);
    END LOOP;
    
    -- FOR loop with dynamic bounds
    DECLARE
        v_start NUMBER := 5;
        v_end NUMBER := 8;
    BEGIN
        FOR i IN v_start..v_end LOOP
            DBMS_OUTPUT.PUT_LINE('Dynamic: ' || i);
        END LOOP;
    END;
    
    -- Nested FOR loops
    FOR i IN 1..3 LOOP
        FOR j IN 1..3 LOOP
            DBMS_OUTPUT.PUT_LINE('(' || i || ',' || j || ')');
        END LOOP;
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- 9. Loop Labels and CONTINUE
-- -----------------------------------------------------------------------------

DECLARE
    v_total NUMBER := 0;
BEGIN
    -- Labeled loop for EXIT
    <<outer_loop>>
    FOR i IN 1..5 LOOP
        <<inner_loop>>
        FOR j IN 1..5 LOOP
            IF i * j > 12 THEN
                EXIT outer_loop;
            END IF;
            v_total := v_total + 1;
            DBMS_OUTPUT.PUT_LINE('i=' || i || ', j=' || j);
        END LOOP inner_loop;
    END LOOP outer_loop;
    
    DBMS_OUTPUT.PUT_LINE('Total iterations: ' || v_total);
    
    -- CONTINUE statement
    FOR i IN 1..10 LOOP
        IF MOD(i, 2) = 0 THEN
            CONTINUE;  -- Skip even numbers
        END IF;
        DBMS_OUTPUT.PUT_LINE('Odd: ' || i);
    END LOOP;
    
    -- CONTINUE WHEN
    FOR i IN 1..20 LOOP
        CONTINUE WHEN MOD(i, 3) <> 0;  -- Only process multiples of 3
        DBMS_OUTPUT.PUT_LINE('Multiple of 3: ' || i);
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- 10. GOTO Statement
-- -----------------------------------------------------------------------------

DECLARE
    v_value NUMBER := 1;
BEGIN
    <<start_label>>
    DBMS_OUTPUT.PUT_LINE('Value: ' || v_value);
    v_value := v_value + 1;
    
    IF v_value <= 5 THEN
        GOTO start_label;
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('Done');
    GOTO end_label;  -- Skip the next line
    
    DBMS_OUTPUT.PUT_LINE('This will not print');
    
    <<end_label>>
    DBMS_OUTPUT.PUT_LINE('At end label');
END;
/

-- -----------------------------------------------------------------------------
-- 11. NULL Statement
-- -----------------------------------------------------------------------------

DECLARE
    v_status VARCHAR2(20) := 'PENDING';
BEGIN
    -- NULL as placeholder
    IF v_status = 'ACTIVE' THEN
        DBMS_OUTPUT.PUT_LINE('Processing active');
    ELSIF v_status = 'PENDING' THEN
        NULL;  -- TODO: implement pending logic
    ELSE
        DBMS_OUTPUT.PUT_LINE('Unknown status');
    END IF;
    
    -- NULL in CASE
    CASE v_status
        WHEN 'ACTIVE' THEN DBMS_OUTPUT.PUT_LINE('Active');
        WHEN 'PENDING' THEN NULL;
        ELSE DBMS_OUTPUT.PUT_LINE('Other');
    END CASE;
END;
/

-- -----------------------------------------------------------------------------
-- 12. SELECT INTO Statements
-- -----------------------------------------------------------------------------

DECLARE
    v_emp_name VARCHAR2(100);
    v_salary NUMBER;
    v_hire_date DATE;
    v_count NUMBER;
    v_emp_rec employees%ROWTYPE;
BEGIN
    -- Single row, single column
    SELECT first_name INTO v_emp_name
    FROM employees
    WHERE employee_id = 100;
    
    -- Single row, multiple columns
    SELECT first_name || ' ' || last_name, salary, hire_date
    INTO v_emp_name, v_salary, v_hire_date
    FROM employees
    WHERE employee_id = 100;
    
    -- Into record
    SELECT * INTO v_emp_rec
    FROM employees
    WHERE employee_id = 100;
    
    -- Aggregate function (always returns one row)
    SELECT COUNT(*) INTO v_count
    FROM employees
    WHERE department_id = 60;
    
    DBMS_OUTPUT.PUT_LINE('Employee: ' || v_emp_name);
    DBMS_OUTPUT.PUT_LINE('Salary: ' || v_salary);
    DBMS_OUTPUT.PUT_LINE('Hired: ' || v_hire_date);
    DBMS_OUTPUT.PUT_LINE('IT Employees: ' || v_count);
END;
/

-- -----------------------------------------------------------------------------
-- 13. DML in PL/SQL
-- -----------------------------------------------------------------------------

DECLARE
    v_rows_affected NUMBER;
    v_new_id NUMBER;
BEGIN
    -- INSERT
    INSERT INTO emp_audit (employee_id, action, action_date)
    VALUES (100, 'LOGIN', SYSDATE);
    
    -- INSERT with RETURNING
    INSERT INTO temp_employees (employee_id, first_name, last_name)
    VALUES (emp_seq.NEXTVAL, 'Test', 'User')
    RETURNING employee_id INTO v_new_id;
    
    DBMS_OUTPUT.PUT_LINE('New ID: ' || v_new_id);
    
    -- UPDATE
    UPDATE employees
    SET salary = salary * 1.05
    WHERE department_id = 60;
    
    v_rows_affected := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('Updated ' || v_rows_affected || ' rows');
    
    -- DELETE
    DELETE FROM temp_employees
    WHERE employee_id = v_new_id;
    
    -- MERGE
    MERGE INTO emp_stats t
    USING (SELECT department_id, AVG(salary) avg_sal FROM employees GROUP BY department_id) s
    ON (t.department_id = s.department_id)
    WHEN MATCHED THEN UPDATE SET t.avg_salary = s.avg_sal
    WHEN NOT MATCHED THEN INSERT (department_id, avg_salary) VALUES (s.department_id, s.avg_sal);
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/

-- -----------------------------------------------------------------------------
-- 14. SQL%Attributes (Implicit Cursor Attributes)
-- -----------------------------------------------------------------------------

DECLARE
    v_name VARCHAR2(100);
BEGIN
    -- SQL%FOUND - TRUE if DML affected rows or SELECT returned rows
    UPDATE employees SET salary = salary WHERE employee_id = 99999;
    IF SQL%FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Row was updated');
    ELSE
        DBMS_OUTPUT.PUT_LINE('No row found to update');
    END IF;
    
    -- SQL%NOTFOUND - opposite of SQL%FOUND
    IF SQL%NOTFOUND THEN
        DBMS_OUTPUT.PUT_LINE('No matching row');
    END IF;
    
    -- SQL%ROWCOUNT - number of rows affected
    UPDATE employees SET commission_pct = NVL(commission_pct, 0) + 0.01
    WHERE department_id = 80;
    DBMS_OUTPUT.PUT_LINE('Updated ' || SQL%ROWCOUNT || ' sales employees');
    
    -- SQL%ISOPEN - always FALSE for implicit cursors
    IF SQL%ISOPEN THEN
        DBMS_OUTPUT.PUT_LINE('Cursor is open');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Implicit cursor is closed');
    END IF;
    
    ROLLBACK;
END;
/

-- -----------------------------------------------------------------------------
-- 15. Transaction Control
-- -----------------------------------------------------------------------------

DECLARE
    v_error_flag BOOLEAN := FALSE;
BEGIN
    -- Start transaction (implicit)
    INSERT INTO audit_log (action, action_date) VALUES ('START', SYSDATE);
    
    -- Savepoint
    SAVEPOINT before_updates;
    
    UPDATE employees SET salary = salary * 1.10 WHERE department_id = 60;
    
    -- Check condition
    IF v_error_flag THEN
        ROLLBACK TO before_updates;
        DBMS_OUTPUT.PUT_LINE('Rolled back to savepoint');
    ELSE
        SAVEPOINT after_salary_update;
        
        UPDATE employees SET commission_pct = 0.05 WHERE department_id = 60 AND commission_pct IS NULL;
    END IF;
    
    -- Commit all changes
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Transaction committed');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Transaction rolled back due to error: ' || SQLERRM);
        RAISE;
END;
/

