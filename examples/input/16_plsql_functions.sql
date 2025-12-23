-- ============================================================================
-- File: 16_plsql_functions.sql
-- Description: PL/SQL user-defined functions
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Simple Functions
-- -----------------------------------------------------------------------------

-- Function returning scalar value
CREATE OR REPLACE FUNCTION get_employee_salary(p_emp_id NUMBER)
RETURN NUMBER
IS
    v_salary NUMBER;
BEGIN
    SELECT salary INTO v_salary
    FROM employees
    WHERE employee_id = p_emp_id;
    
    RETURN v_salary;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END get_employee_salary;
/

-- Function with VARCHAR2 return
CREATE OR REPLACE FUNCTION get_employee_name(p_emp_id NUMBER)
RETURN VARCHAR2
IS
    v_name VARCHAR2(100);
BEGIN
    SELECT first_name || ' ' || last_name INTO v_name
    FROM employees
    WHERE employee_id = p_emp_id;
    
    RETURN v_name;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 'Unknown Employee';
END get_employee_name;
/

-- Function returning DATE
CREATE OR REPLACE FUNCTION get_hire_date(p_emp_id NUMBER)
RETURN DATE
IS
    v_hire_date DATE;
BEGIN
    SELECT hire_date INTO v_hire_date
    FROM employees
    WHERE employee_id = p_emp_id;
    
    RETURN v_hire_date;
END get_hire_date;
/

-- Function returning BOOLEAN (can't be used in SQL)
CREATE OR REPLACE FUNCTION is_manager(p_emp_id NUMBER)
RETURN BOOLEAN
IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM employees
    WHERE manager_id = p_emp_id;
    
    RETURN v_count > 0;
END is_manager;
/

-- -----------------------------------------------------------------------------
-- 2. Functions with Multiple Parameters
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION calculate_bonus(
    p_salary      NUMBER,
    p_commission  NUMBER DEFAULT 0,
    p_years       NUMBER DEFAULT 1
)
RETURN NUMBER
IS
    v_bonus NUMBER;
    c_base_rate CONSTANT NUMBER := 0.1;
    c_commission_rate CONSTANT NUMBER := 0.5;
    c_seniority_rate CONSTANT NUMBER := 0.02;
BEGIN
    -- Base bonus
    v_bonus := p_salary * c_base_rate;
    
    -- Commission component
    v_bonus := v_bonus + (p_salary * NVL(p_commission, 0) * c_commission_rate);
    
    -- Seniority bonus
    v_bonus := v_bonus * (1 + (p_years * c_seniority_rate));
    
    RETURN ROUND(v_bonus, 2);
END calculate_bonus;
/

-- Function with OUT parameter (less common for functions)
CREATE OR REPLACE FUNCTION get_employee_details(
    p_emp_id     IN  NUMBER,
    p_dept_name  OUT VARCHAR2
)
RETURN VARCHAR2
IS
    v_emp_name VARCHAR2(100);
BEGIN
    SELECT e.first_name || ' ' || e.last_name, d.department_name
    INTO v_emp_name, p_dept_name
    FROM employees e
    JOIN departments d ON e.department_id = d.department_id
    WHERE e.employee_id = p_emp_id;
    
    RETURN v_emp_name;
END get_employee_details;
/

-- -----------------------------------------------------------------------------
-- 3. Functions with Different Parameter Modes
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION format_name(
    p_first_name  IN VARCHAR2,
    p_last_name   IN VARCHAR2,
    p_format      IN VARCHAR2 DEFAULT 'FL'  -- FL=FirstLast, LF=LastFirst, FI=FirstInitial
)
RETURN VARCHAR2
IS
BEGIN
    RETURN CASE p_format
        WHEN 'FL' THEN INITCAP(p_first_name) || ' ' || INITCAP(p_last_name)
        WHEN 'LF' THEN INITCAP(p_last_name) || ', ' || INITCAP(p_first_name)
        WHEN 'FI' THEN INITCAP(p_first_name) || ' ' || UPPER(SUBSTR(p_last_name, 1, 1)) || '.'
        ELSE p_first_name || ' ' || p_last_name
    END;
END format_name;
/

-- -----------------------------------------------------------------------------
-- 4. Deterministic Functions
-- -----------------------------------------------------------------------------

-- Deterministic function - always returns same result for same input
CREATE OR REPLACE FUNCTION calculate_tax(p_amount NUMBER)
RETURN NUMBER
DETERMINISTIC
IS
BEGIN
    RETURN ROUND(p_amount * 0.25, 2);
END calculate_tax;
/

-- Deterministic for indexing
CREATE OR REPLACE FUNCTION standardize_phone(p_phone VARCHAR2)
RETURN VARCHAR2
DETERMINISTIC
IS
    v_clean VARCHAR2(20);
BEGIN
    -- Remove all non-numeric characters
    v_clean := REGEXP_REPLACE(p_phone, '[^0-9]', '');
    
    -- Format as (XXX) XXX-XXXX
    IF LENGTH(v_clean) = 10 THEN
        RETURN '(' || SUBSTR(v_clean, 1, 3) || ') ' || 
               SUBSTR(v_clean, 4, 3) || '-' || SUBSTR(v_clean, 7, 4);
    ELSE
        RETURN p_phone;
    END IF;
END standardize_phone;
/

-- Can create function-based index
-- CREATE INDEX emp_phone_idx ON employees(standardize_phone(phone_number));

-- -----------------------------------------------------------------------------
-- 5. RESULT_CACHE Functions
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_department_name(p_dept_id NUMBER)
RETURN VARCHAR2
RESULT_CACHE RELIES_ON (departments)
IS
    v_name VARCHAR2(100);
BEGIN
    SELECT department_name INTO v_name
    FROM departments
    WHERE department_id = p_dept_id;
    
    RETURN v_name;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 'Unknown';
END get_department_name;
/

CREATE OR REPLACE FUNCTION get_tax_rate(p_country_code VARCHAR2)
RETURN NUMBER
RESULT_CACHE
IS
    v_rate NUMBER;
BEGIN
    SELECT tax_rate INTO v_rate
    FROM country_tax_rates
    WHERE country_code = p_country_code;
    
    RETURN v_rate;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
END get_tax_rate;
/

-- -----------------------------------------------------------------------------
-- 6. Functions Returning Complex Types
-- -----------------------------------------------------------------------------

-- Function returning RECORD type
CREATE OR REPLACE TYPE t_emp_info AS OBJECT (
    emp_id NUMBER,
    emp_name VARCHAR2(100),
    salary NUMBER,
    dept_name VARCHAR2(100)
);
/

CREATE OR REPLACE FUNCTION get_emp_info(p_emp_id NUMBER)
RETURN t_emp_info
IS
    v_info t_emp_info;
BEGIN
    SELECT t_emp_info(
        e.employee_id,
        e.first_name || ' ' || e.last_name,
        e.salary,
        d.department_name
    )
    INTO v_info
    FROM employees e
    LEFT JOIN departments d ON e.department_id = d.department_id
    WHERE e.employee_id = p_emp_id;
    
    RETURN v_info;
END get_emp_info;
/

-- -----------------------------------------------------------------------------
-- 7. Table Functions (Pipelined)
-- -----------------------------------------------------------------------------

-- Create table type
CREATE OR REPLACE TYPE t_emp_row AS OBJECT (
    employee_id NUMBER,
    full_name VARCHAR2(100),
    salary NUMBER
);
/

CREATE OR REPLACE TYPE t_emp_table AS TABLE OF t_emp_row;
/

-- Non-pipelined table function
CREATE OR REPLACE FUNCTION get_employees_by_dept(p_dept_id NUMBER)
RETURN t_emp_table
IS
    v_result t_emp_table := t_emp_table();
BEGIN
    FOR emp IN (
        SELECT employee_id, first_name || ' ' || last_name AS full_name, salary
        FROM employees
        WHERE department_id = p_dept_id
    ) LOOP
        v_result.EXTEND;
        v_result(v_result.COUNT) := t_emp_row(emp.employee_id, emp.full_name, emp.salary);
    END LOOP;
    
    RETURN v_result;
END get_employees_by_dept;
/

-- Pipelined table function
CREATE OR REPLACE FUNCTION get_employees_pipe(p_dept_id NUMBER)
RETURN t_emp_table PIPELINED
IS
BEGIN
    FOR emp IN (
        SELECT employee_id, first_name || ' ' || last_name AS full_name, salary
        FROM employees
        WHERE department_id = NVL(p_dept_id, department_id)
    ) LOOP
        PIPE ROW(t_emp_row(emp.employee_id, emp.full_name, emp.salary));
    END LOOP;
    
    RETURN;
END get_employees_pipe;
/

-- Usage:
-- SELECT * FROM TABLE(get_employees_pipe(60));

-- Pipelined function with parallel
CREATE OR REPLACE FUNCTION get_all_employees_parallel
RETURN t_emp_table
PIPELINED
PARALLEL_ENABLE (PARTITION employees BY HASH(department_id))
IS
BEGIN
    FOR emp IN (
        SELECT employee_id, first_name || ' ' || last_name AS full_name, salary
        FROM employees
    ) LOOP
        PIPE ROW(t_emp_row(emp.employee_id, emp.full_name, emp.salary));
    END LOOP;
    
    RETURN;
END get_all_employees_parallel;
/

-- -----------------------------------------------------------------------------
-- 8. Functions with REF CURSOR Return
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_employees_cursor(p_dept_id NUMBER)
RETURN SYS_REFCURSOR
IS
    v_cursor SYS_REFCURSOR;
BEGIN
    OPEN v_cursor FOR
        SELECT employee_id, first_name, last_name, salary
        FROM employees
        WHERE department_id = NVL(p_dept_id, department_id)
        ORDER BY last_name;
    
    RETURN v_cursor;
END get_employees_cursor;
/

-- -----------------------------------------------------------------------------
-- 9. Utility Functions
-- -----------------------------------------------------------------------------

-- String utility
CREATE OR REPLACE FUNCTION proper_case(p_string VARCHAR2)
RETURN VARCHAR2
IS
    v_result VARCHAR2(4000);
    v_prev_char CHAR(1) := ' ';
    v_curr_char CHAR(1);
BEGIN
    FOR i IN 1..LENGTH(p_string) LOOP
        v_curr_char := SUBSTR(p_string, i, 1);
        
        IF v_prev_char IN (' ', '-', '''', '(', '.') THEN
            v_result := v_result || UPPER(v_curr_char);
        ELSE
            v_result := v_result || LOWER(v_curr_char);
        END IF;
        
        v_prev_char := v_curr_char;
    END LOOP;
    
    RETURN v_result;
END proper_case;
/

-- Date utility
CREATE OR REPLACE FUNCTION business_days_between(
    p_start_date DATE,
    p_end_date DATE
)
RETURN NUMBER
IS
    v_count NUMBER := 0;
    v_date DATE;
BEGIN
    v_date := TRUNC(p_start_date);
    
    WHILE v_date <= TRUNC(p_end_date) LOOP
        IF TO_CHAR(v_date, 'DY', 'NLS_DATE_LANGUAGE=ENGLISH') NOT IN ('SAT', 'SUN') THEN
            v_count := v_count + 1;
        END IF;
        v_date := v_date + 1;
    END LOOP;
    
    RETURN v_count;
END business_days_between;
/

-- Number utility
CREATE OR REPLACE FUNCTION number_to_words(p_number NUMBER)
RETURN VARCHAR2
IS
    v_number NUMBER := ABS(TRUNC(p_number));
    v_result VARCHAR2(4000);
    
    TYPE t_words IS TABLE OF VARCHAR2(20) INDEX BY PLS_INTEGER;
    v_ones t_words;
    v_teens t_words;
    v_tens t_words;
    
    FUNCTION convert_hundreds(p_num NUMBER) RETURN VARCHAR2 IS
        v_str VARCHAR2(200);
        v_n NUMBER := p_num;
    BEGIN
        IF v_n >= 100 THEN
            v_str := v_ones(TRUNC(v_n/100)) || ' Hundred ';
            v_n := MOD(v_n, 100);
        END IF;
        
        IF v_n >= 20 THEN
            v_str := v_str || v_tens(TRUNC(v_n/10));
            v_n := MOD(v_n, 10);
            IF v_n > 0 THEN
                v_str := v_str || '-' || v_ones(v_n);
            END IF;
        ELSIF v_n >= 10 THEN
            v_str := v_str || v_teens(v_n - 10);
        ELSIF v_n > 0 THEN
            v_str := v_str || v_ones(v_n);
        END IF;
        
        RETURN TRIM(v_str);
    END;
BEGIN
    -- Initialize arrays
    v_ones(0) := 'Zero'; v_ones(1) := 'One'; v_ones(2) := 'Two';
    v_ones(3) := 'Three'; v_ones(4) := 'Four'; v_ones(5) := 'Five';
    v_ones(6) := 'Six'; v_ones(7) := 'Seven'; v_ones(8) := 'Eight';
    v_ones(9) := 'Nine';
    
    v_teens(0) := 'Ten'; v_teens(1) := 'Eleven'; v_teens(2) := 'Twelve';
    v_teens(3) := 'Thirteen'; v_teens(4) := 'Fourteen'; v_teens(5) := 'Fifteen';
    v_teens(6) := 'Sixteen'; v_teens(7) := 'Seventeen'; v_teens(8) := 'Eighteen';
    v_teens(9) := 'Nineteen';
    
    v_tens(2) := 'Twenty'; v_tens(3) := 'Thirty'; v_tens(4) := 'Forty';
    v_tens(5) := 'Fifty'; v_tens(6) := 'Sixty'; v_tens(7) := 'Seventy';
    v_tens(8) := 'Eighty'; v_tens(9) := 'Ninety';
    
    IF v_number = 0 THEN
        RETURN 'Zero';
    END IF;
    
    IF v_number >= 1000000 THEN
        v_result := convert_hundreds(TRUNC(v_number/1000000)) || ' Million ';
        v_number := MOD(v_number, 1000000);
    END IF;
    
    IF v_number >= 1000 THEN
        v_result := v_result || convert_hundreds(TRUNC(v_number/1000)) || ' Thousand ';
        v_number := MOD(v_number, 1000);
    END IF;
    
    IF v_number > 0 THEN
        v_result := v_result || convert_hundreds(v_number);
    END IF;
    
    IF p_number < 0 THEN
        v_result := 'Negative ' || v_result;
    END IF;
    
    RETURN TRIM(v_result);
END number_to_words;
/

-- -----------------------------------------------------------------------------
-- 10. Functions for SQL Expressions
-- -----------------------------------------------------------------------------

-- Function usable in SQL
CREATE OR REPLACE FUNCTION years_of_service(p_hire_date DATE)
RETURN NUMBER
IS
BEGIN
    RETURN TRUNC(MONTHS_BETWEEN(SYSDATE, p_hire_date) / 12);
END years_of_service;
/

-- Usage in SQL:
-- SELECT first_name, hire_date, years_of_service(hire_date) as tenure FROM employees;

-- Function with PRAGMA RESTRICT_REFERENCES (deprecated but still seen)
CREATE OR REPLACE FUNCTION salary_grade(p_salary NUMBER)
RETURN VARCHAR2
IS
BEGIN
    RETURN CASE
        WHEN p_salary < 5000 THEN 'G1'
        WHEN p_salary < 10000 THEN 'G2'
        WHEN p_salary < 15000 THEN 'G3'
        WHEN p_salary < 20000 THEN 'G4'
        ELSE 'G5'
    END;
END salary_grade;
/

-- -----------------------------------------------------------------------------
-- 11. Recursive Functions
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION factorial(p_n NUMBER)
RETURN NUMBER
IS
BEGIN
    IF p_n <= 1 THEN
        RETURN 1;
    ELSE
        RETURN p_n * factorial(p_n - 1);
    END IF;
END factorial;
/

CREATE OR REPLACE FUNCTION fibonacci(p_n NUMBER)
RETURN NUMBER
IS
BEGIN
    IF p_n <= 0 THEN
        RETURN 0;
    ELSIF p_n = 1 THEN
        RETURN 1;
    ELSE
        RETURN fibonacci(p_n - 1) + fibonacci(p_n - 2);
    END IF;
END fibonacci;
/

-- -----------------------------------------------------------------------------
-- 12. Error Handling in Functions
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION safe_divide(
    p_numerator   NUMBER,
    p_denominator NUMBER,
    p_default     NUMBER DEFAULT 0
)
RETURN NUMBER
IS
BEGIN
    IF p_denominator = 0 THEN
        RETURN p_default;
    END IF;
    
    RETURN p_numerator / p_denominator;
EXCEPTION
    WHEN ZERO_DIVIDE THEN
        RETURN p_default;
    WHEN OTHERS THEN
        RETURN NULL;
END safe_divide;
/

CREATE OR REPLACE FUNCTION get_employee_salary_safe(p_emp_id NUMBER)
RETURN NUMBER
IS
    v_salary NUMBER;
BEGIN
    SELECT salary INTO v_salary
    FROM employees
    WHERE employee_id = p_emp_id;
    
    RETURN v_salary;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Employee ' || p_emp_id || ' not found');
        RETURN -1;
    WHEN TOO_MANY_ROWS THEN
        DBMS_OUTPUT.PUT_LINE('Multiple employees found for ' || p_emp_id);
        RETURN -2;
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RETURN NULL;
END get_employee_salary_safe;
/

-- -----------------------------------------------------------------------------
-- 13. Function Calling Another Function
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION calculate_total_compensation(p_emp_id NUMBER)
RETURN NUMBER
IS
    v_salary NUMBER;
    v_bonus NUMBER;
    v_years NUMBER;
    v_hire_date DATE;
BEGIN
    -- Call other functions
    v_salary := get_employee_salary(p_emp_id);
    v_hire_date := get_hire_date(p_emp_id);
    v_years := years_of_service(v_hire_date);
    
    -- Call function with named parameters
    v_bonus := calculate_bonus(
        p_salary => v_salary,
        p_years => v_years
    );
    
    RETURN v_salary + v_bonus;
END calculate_total_compensation;
/

-- -----------------------------------------------------------------------------
-- 14. Functions in SQL vs PL/SQL Context
-- -----------------------------------------------------------------------------

-- This function can be called from SQL
CREATE OR REPLACE FUNCTION sql_callable_func(p_value NUMBER)
RETURN NUMBER
IS
BEGIN
    RETURN p_value * 2;
END sql_callable_func;
/

-- This function CANNOT be called from SQL (due to BOOLEAN return)
CREATE OR REPLACE FUNCTION plsql_only_func(p_value NUMBER)
RETURN BOOLEAN
IS
BEGIN
    RETURN p_value > 0;
END plsql_only_func;
/

-- Wrapper to make it SQL-callable
CREATE OR REPLACE FUNCTION is_positive(p_value NUMBER)
RETURN VARCHAR2
IS
BEGIN
    RETURN CASE WHEN p_value > 0 THEN 'Y' ELSE 'N' END;
END is_positive;
/

