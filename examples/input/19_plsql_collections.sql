-- ============================================================================
-- File: 19_plsql_collections.sql
-- Description: PL/SQL collections - Associative arrays, Nested tables, VARRAYs
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Associative Arrays (Index-By Tables)
-- -----------------------------------------------------------------------------

DECLARE
    -- Index by integer
    TYPE t_number_array IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    TYPE t_varchar_array IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
    
    -- Index by string
    TYPE t_salary_by_name IS TABLE OF NUMBER INDEX BY VARCHAR2(100);
    TYPE t_dept_by_code IS TABLE OF VARCHAR2(100) INDEX BY VARCHAR2(10);
    
    v_numbers t_number_array;
    v_names t_varchar_array;
    v_salaries t_salary_by_name;
    v_depts t_dept_by_code;
    
    v_key VARCHAR2(100);
BEGIN
    -- Populate integer-indexed array
    v_numbers(1) := 100;
    v_numbers(2) := 200;
    v_numbers(5) := 500;  -- Sparse - index 3 and 4 don't exist
    
    -- Populate string-indexed array (like a hash map)
    v_salaries('John') := 5000;
    v_salaries('Jane') := 6000;
    v_salaries('Bob') := 5500;
    
    v_depts('IT') := 'Information Technology';
    v_depts('HR') := 'Human Resources';
    v_depts('FIN') := 'Finance';
    
    -- Iterate with FIRST/NEXT (sparse-safe)
    DBMS_OUTPUT.PUT_LINE('Numbers:');
    DECLARE
        v_idx PLS_INTEGER := v_numbers.FIRST;
    BEGIN
        WHILE v_idx IS NOT NULL LOOP
            DBMS_OUTPUT.PUT_LINE('  Index ' || v_idx || ': ' || v_numbers(v_idx));
            v_idx := v_numbers.NEXT(v_idx);
        END LOOP;
    END;
    
    -- Iterate string-indexed with FIRST/NEXT
    DBMS_OUTPUT.PUT_LINE('Salaries:');
    v_key := v_salaries.FIRST;
    WHILE v_key IS NOT NULL LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || v_key || ': $' || v_salaries(v_key));
        v_key := v_salaries.NEXT(v_key);
    END LOOP;
    
    -- EXISTS method
    IF v_numbers.EXISTS(1) THEN
        DBMS_OUTPUT.PUT_LINE('Index 1 exists');
    END IF;
    
    IF NOT v_numbers.EXISTS(3) THEN
        DBMS_OUTPUT.PUT_LINE('Index 3 does not exist');
    END IF;
    
    -- COUNT method
    DBMS_OUTPUT.PUT_LINE('Number of elements: ' || v_salaries.COUNT);
    
    -- DELETE methods
    v_salaries.DELETE('Bob');        -- Delete specific element
    v_numbers.DELETE(1, 2);          -- Delete range
    v_depts.DELETE;                  -- Delete all
END;
/

-- -----------------------------------------------------------------------------
-- 2. Nested Tables
-- -----------------------------------------------------------------------------

-- Define nested table type in schema
CREATE OR REPLACE TYPE t_number_nt AS TABLE OF NUMBER;
/

CREATE OR REPLACE TYPE t_varchar_nt AS TABLE OF VARCHAR2(100);
/

DECLARE
    -- Local nested table type
    TYPE t_emp_names IS TABLE OF VARCHAR2(100);
    TYPE t_emp_salaries IS TABLE OF NUMBER;
    
    v_numbers t_number_nt;
    v_names t_emp_names;
    v_salaries t_emp_salaries := t_emp_salaries();  -- Initialize empty
BEGIN
    -- Initialize with constructor
    v_numbers := t_number_nt(10, 20, 30, 40, 50);
    v_names := t_emp_names('Alice', 'Bob', 'Charlie');
    
    -- Access elements (1-based)
    DBMS_OUTPUT.PUT_LINE('First number: ' || v_numbers(1));
    DBMS_OUTPUT.PUT_LINE('Second name: ' || v_names(2));
    
    -- EXTEND to add elements
    v_salaries.EXTEND;
    v_salaries(1) := 5000;
    
    v_salaries.EXTEND(3);  -- Add 3 more elements
    v_salaries(2) := 6000;
    v_salaries(3) := 7000;
    v_salaries(4) := 8000;
    
    -- EXTEND with copy
    v_numbers.EXTEND(2, 1);  -- Add 2 copies of element 1
    
    -- Iterate
    FOR i IN 1..v_numbers.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('v_numbers(' || i || ') = ' || v_numbers(i));
    END LOOP;
    
    -- DELETE makes elements null but keeps index
    v_numbers.DELETE(2);
    
    -- TRIM removes from end
    v_names.TRIM;      -- Remove last element
    v_names.TRIM(1);   -- Remove 1 element from end
    
    -- Collection methods
    DBMS_OUTPUT.PUT_LINE('Count: ' || v_salaries.COUNT);
    DBMS_OUTPUT.PUT_LINE('First: ' || v_salaries.FIRST);
    DBMS_OUTPUT.PUT_LINE('Last: ' || v_salaries.LAST);
    DBMS_OUTPUT.PUT_LINE('Limit: ' || NVL(TO_CHAR(v_salaries.LIMIT), 'No limit'));
    
    -- PRIOR and NEXT
    FOR i IN 1..v_numbers.COUNT LOOP
        IF v_numbers.EXISTS(i) THEN
            DBMS_OUTPUT.PUT_LINE('Element ' || i || 
                                ', Prior: ' || NVL(TO_CHAR(v_numbers.PRIOR(i)), 'NULL') ||
                                ', Next: ' || NVL(TO_CHAR(v_numbers.NEXT(i)), 'NULL'));
        END IF;
    END LOOP;
END;
/

-- -----------------------------------------------------------------------------
-- 3. VARRAYs (Variable-Size Arrays)
-- -----------------------------------------------------------------------------

-- Define VARRAY type in schema
CREATE OR REPLACE TYPE t_phone_varray AS VARRAY(5) OF VARCHAR2(20);
/

CREATE OR REPLACE TYPE t_score_varray AS VARRAY(10) OF NUMBER;
/

DECLARE
    -- Local VARRAY type
    TYPE t_color_array IS VARRAY(7) OF VARCHAR2(20);
    
    v_phones t_phone_varray;
    v_scores t_score_varray;
    v_colors t_color_array;
BEGIN
    -- Initialize with constructor
    v_phones := t_phone_varray('555-1234', '555-5678');
    v_colors := t_color_array('Red', 'Green', 'Blue');
    
    -- Empty initialization
    v_scores := t_score_varray();
    
    -- Access elements
    DBMS_OUTPUT.PUT_LINE('First phone: ' || v_phones(1));
    
    -- EXTEND within limit
    v_phones.EXTEND;
    v_phones(3) := '555-9999';
    
    v_scores.EXTEND(5);
    FOR i IN 1..5 LOOP
        v_scores(i) := i * 10;
    END LOOP;
    
    -- LIMIT method - returns maximum size
    DBMS_OUTPUT.PUT_LINE('Max phones: ' || v_phones.LIMIT);
    DBMS_OUTPUT.PUT_LINE('Current count: ' || v_phones.COUNT);
    
    -- Cannot EXTEND beyond limit
    -- v_phones.EXTEND(10);  -- This would cause error
    
    -- Iterate
    FOR i IN 1..v_colors.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Color ' || i || ': ' || v_colors(i));
    END LOOP;
    
    -- TRIM removes from end
    v_colors.TRIM;
    DBMS_OUTPUT.PUT_LINE('After trim: ' || v_colors.COUNT);
    
    -- Note: VARRAYs cannot delete individual elements
    -- v_colors.DELETE(1);  -- This would cause error
END;
/

-- -----------------------------------------------------------------------------
-- 4. Collection of Records
-- -----------------------------------------------------------------------------

DECLARE
    TYPE t_emp_rec IS RECORD (
        emp_id NUMBER,
        emp_name VARCHAR2(100),
        salary NUMBER
    );
    
    TYPE t_emp_table IS TABLE OF t_emp_rec;
    TYPE t_emp_array IS VARRAY(100) OF t_emp_rec;
    TYPE t_emp_assoc IS TABLE OF t_emp_rec INDEX BY PLS_INTEGER;
    
    v_employees t_emp_table := t_emp_table();
    v_emp_array t_emp_array := t_emp_array();
    v_emp_assoc t_emp_assoc;
    v_emp_rec t_emp_rec;
BEGIN
    -- Populate nested table of records
    FOR emp IN (SELECT employee_id, first_name || ' ' || last_name AS name, salary 
                FROM employees WHERE ROWNUM <= 5) LOOP
        v_employees.EXTEND;
        v_employees(v_employees.LAST).emp_id := emp.employee_id;
        v_employees(v_employees.LAST).emp_name := emp.name;
        v_employees(v_employees.LAST).salary := emp.salary;
    END LOOP;
    
    -- Access record fields
    FOR i IN 1..v_employees.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_employees(i).emp_id || ': ' || 
                            v_employees(i).emp_name || ' - $' || 
                            v_employees(i).salary);
    END LOOP;
    
    -- Associative array with record
    v_emp_rec.emp_id := 100;
    v_emp_rec.emp_name := 'John Doe';
    v_emp_rec.salary := 5000;
    v_emp_assoc(100) := v_emp_rec;
    
    v_emp_rec.emp_id := 101;
    v_emp_rec.emp_name := 'Jane Smith';
    v_emp_rec.salary := 6000;
    v_emp_assoc(101) := v_emp_rec;
    
    DBMS_OUTPUT.PUT_LINE('From assoc: ' || v_emp_assoc(100).emp_name);
END;
/

-- -----------------------------------------------------------------------------
-- 5. Collections with BULK COLLECT
-- -----------------------------------------------------------------------------

DECLARE
    TYPE t_emp_ids IS TABLE OF employees.employee_id%TYPE;
    TYPE t_emp_names IS TABLE OF VARCHAR2(100);
    TYPE t_emp_rec IS TABLE OF employees%ROWTYPE;
    
    v_ids t_emp_ids;
    v_names t_emp_names;
    v_emps t_emp_rec;
BEGIN
    -- Bulk collect into multiple collections
    SELECT employee_id, first_name || ' ' || last_name
    BULK COLLECT INTO v_ids, v_names
    FROM employees
    WHERE department_id = 60;
    
    DBMS_OUTPUT.PUT_LINE('Loaded ' || v_ids.COUNT || ' employees');
    
    FOR i IN 1..v_ids.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_ids(i) || ': ' || v_names(i));
    END LOOP;
    
    -- Bulk collect entire rows
    SELECT *
    BULK COLLECT INTO v_emps
    FROM employees
    WHERE salary > 10000;
    
    DBMS_OUTPUT.PUT_LINE('High earners: ' || v_emps.COUNT);
    
    FOR i IN 1..v_emps.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(v_emps(i).first_name || ' ' || v_emps(i).last_name);
    END LOOP;
END;
/

-- BULK COLLECT with LIMIT
DECLARE
    CURSOR c_emp IS SELECT * FROM employees;
    TYPE t_emp_tab IS TABLE OF employees%ROWTYPE;
    v_employees t_emp_tab;
    c_batch_size CONSTANT NUMBER := 10;
    v_total NUMBER := 0;
BEGIN
    OPEN c_emp;
    
    LOOP
        FETCH c_emp BULK COLLECT INTO v_employees LIMIT c_batch_size;
        
        EXIT WHEN v_employees.COUNT = 0;
        
        v_total := v_total + v_employees.COUNT;
        DBMS_OUTPUT.PUT_LINE('Processing batch of ' || v_employees.COUNT);
        
        -- Process batch
        FOR i IN 1..v_employees.COUNT LOOP
            NULL; -- Process each employee
        END LOOP;
        
        EXIT WHEN v_employees.COUNT < c_batch_size;
    END LOOP;
    
    CLOSE c_emp;
    DBMS_OUTPUT.PUT_LINE('Total processed: ' || v_total);
END;
/

-- -----------------------------------------------------------------------------
-- 6. FORALL Bulk DML
-- -----------------------------------------------------------------------------

DECLARE
    TYPE t_emp_ids IS TABLE OF NUMBER;
    TYPE t_salaries IS TABLE OF NUMBER;
    
    v_emp_ids t_emp_ids;
    v_new_salaries t_salaries;
BEGIN
    -- Prepare data
    v_emp_ids := t_emp_ids(100, 101, 102, 103, 104);
    v_new_salaries := t_salaries(5000, 5500, 6000, 6500, 7000);
    
    -- FORALL for bulk update
    FORALL i IN 1..v_emp_ids.COUNT
        UPDATE employees
        SET salary = v_new_salaries(i)
        WHERE employee_id = v_emp_ids(i);
    
    DBMS_OUTPUT.PUT_LINE('Updated ' || SQL%ROWCOUNT || ' rows');
    
    ROLLBACK;
END;
/

-- FORALL with INSERT
DECLARE
    TYPE t_emp_rec IS RECORD (
        emp_id NUMBER,
        emp_name VARCHAR2(100),
        hire_date DATE
    );
    TYPE t_emp_tab IS TABLE OF t_emp_rec;
    v_new_emps t_emp_tab := t_emp_tab();
BEGIN
    -- Prepare new employees
    FOR i IN 1..5 LOOP
        v_new_emps.EXTEND;
        v_new_emps(i).emp_id := 900 + i;
        v_new_emps(i).emp_name := 'Employee ' || i;
        v_new_emps(i).hire_date := SYSDATE;
    END LOOP;
    
    -- Bulk insert
    FORALL i IN 1..v_new_emps.COUNT
        INSERT INTO temp_employees (employee_id, employee_name, hire_date)
        VALUES (v_new_emps(i).emp_id, v_new_emps(i).emp_name, v_new_emps(i).hire_date);
    
    DBMS_OUTPUT.PUT_LINE('Inserted ' || SQL%ROWCOUNT || ' rows');
    
    ROLLBACK;
END;
/

-- FORALL with sparse collection (INDICES OF)
DECLARE
    TYPE t_emp_ids IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    v_emp_ids t_emp_ids;
BEGIN
    -- Sparse array
    v_emp_ids(1) := 100;
    v_emp_ids(5) := 101;
    v_emp_ids(10) := 102;
    -- Indices 2,3,4,6,7,8,9 don't exist
    
    -- INDICES OF handles sparse collections
    FORALL i IN INDICES OF v_emp_ids
        UPDATE employees
        SET last_update = SYSDATE
        WHERE employee_id = v_emp_ids(i);
    
    DBMS_OUTPUT.PUT_LINE('Updated ' || SQL%ROWCOUNT || ' rows');
    ROLLBACK;
END;
/

-- FORALL with VALUES OF
DECLARE
    TYPE t_index_tab IS TABLE OF PLS_INTEGER;
    TYPE t_emp_ids IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    
    v_indices t_index_tab;
    v_emp_ids t_emp_ids;
BEGIN
    -- Data
    v_emp_ids(1) := 100;
    v_emp_ids(2) := 101;
    v_emp_ids(3) := 102;
    v_emp_ids(4) := 103;
    v_emp_ids(5) := 104;
    
    -- Only process certain indices
    v_indices := t_index_tab(1, 3, 5);
    
    -- VALUES OF uses indices from another collection
    FORALL i IN VALUES OF v_indices
        UPDATE employees
        SET last_update = SYSDATE
        WHERE employee_id = v_emp_ids(i);
    
    DBMS_OUTPUT.PUT_LINE('Updated ' || SQL%ROWCOUNT || ' rows');
    ROLLBACK;
END;
/

-- FORALL with SAVE EXCEPTIONS
DECLARE
    TYPE t_emp_ids IS TABLE OF NUMBER;
    TYPE t_salaries IS TABLE OF NUMBER;
    
    v_emp_ids t_emp_ids := t_emp_ids(100, 101, 999, 103, 998);  -- 999, 998 don't exist
    v_salaries t_salaries := t_salaries(5000, 5500, 6000, 6500, 7000);
    
    e_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_dml_errors, -24381);
BEGIN
    FORALL i IN 1..v_emp_ids.COUNT SAVE EXCEPTIONS
        UPDATE employees
        SET salary = v_salaries(i)
        WHERE employee_id = v_emp_ids(i);
    
EXCEPTION
    WHEN e_dml_errors THEN
        DBMS_OUTPUT.PUT_LINE('Number of errors: ' || SQL%BULK_EXCEPTIONS.COUNT);
        
        FOR i IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
            DBMS_OUTPUT.PUT_LINE('Error ' || i || 
                                ': Index ' || SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ||
                                ', Code ' || SQL%BULK_EXCEPTIONS(i).ERROR_CODE);
        END LOOP;
        
        ROLLBACK;
END;
/

-- -----------------------------------------------------------------------------
-- 7. Collection Methods Summary
-- -----------------------------------------------------------------------------

DECLARE
    TYPE t_demo IS TABLE OF NUMBER;
    v_coll t_demo := t_demo(10, 20, 30, 40, 50);
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Collection Methods Demo ===');
    
    -- COUNT - number of elements
    DBMS_OUTPUT.PUT_LINE('COUNT: ' || v_coll.COUNT);
    
    -- FIRST - first index
    DBMS_OUTPUT.PUT_LINE('FIRST: ' || v_coll.FIRST);
    
    -- LAST - last index
    DBMS_OUTPUT.PUT_LINE('LAST: ' || v_coll.LAST);
    
    -- EXISTS - check if index exists
    DBMS_OUTPUT.PUT_LINE('EXISTS(3): ' || CASE WHEN v_coll.EXISTS(3) THEN 'TRUE' ELSE 'FALSE' END);
    
    -- PRIOR - previous index
    DBMS_OUTPUT.PUT_LINE('PRIOR(3): ' || v_coll.PRIOR(3));
    
    -- NEXT - next index
    DBMS_OUTPUT.PUT_LINE('NEXT(3): ' || v_coll.NEXT(3));
    
    -- LIMIT - max size (NULL for nested tables)
    DBMS_OUTPUT.PUT_LINE('LIMIT: ' || NVL(TO_CHAR(v_coll.LIMIT), 'NULL (no limit)'));
    
    -- EXTEND - add elements
    v_coll.EXTEND(2);  -- Add 2 null elements
    DBMS_OUTPUT.PUT_LINE('After EXTEND(2), COUNT: ' || v_coll.COUNT);
    
    -- TRIM - remove from end
    v_coll.TRIM(1);
    DBMS_OUTPUT.PUT_LINE('After TRIM(1), COUNT: ' || v_coll.COUNT);
    
    -- DELETE - remove elements
    v_coll.DELETE(3);  -- Delete element at index 3
    DBMS_OUTPUT.PUT_LINE('After DELETE(3), EXISTS(3): ' || 
                        CASE WHEN v_coll.EXISTS(3) THEN 'TRUE' ELSE 'FALSE' END);
END;
/

-- -----------------------------------------------------------------------------
-- 8. Multiset Operations (Nested Tables)
-- -----------------------------------------------------------------------------

DECLARE
    TYPE t_numbers IS TABLE OF NUMBER;
    v_set1 t_numbers := t_numbers(1, 2, 3, 4, 5);
    v_set2 t_numbers := t_numbers(4, 5, 6, 7, 8);
    v_result t_numbers;
BEGIN
    -- MULTISET UNION
    v_result := v_set1 MULTISET UNION v_set2;
    DBMS_OUTPUT.PUT_LINE('UNION: ' || v_result.COUNT || ' elements');  -- Includes duplicates
    
    -- MULTISET UNION DISTINCT
    v_result := v_set1 MULTISET UNION DISTINCT v_set2;
    DBMS_OUTPUT.PUT_LINE('UNION DISTINCT: ' || v_result.COUNT || ' elements');
    
    -- MULTISET INTERSECT
    v_result := v_set1 MULTISET INTERSECT v_set2;
    DBMS_OUTPUT.PUT_LINE('INTERSECT: ' || v_result.COUNT || ' elements');  -- 4, 5
    
    -- MULTISET EXCEPT
    v_result := v_set1 MULTISET EXCEPT v_set2;
    DBMS_OUTPUT.PUT_LINE('EXCEPT: ' || v_result.COUNT || ' elements');  -- 1, 2, 3
    
    -- CARDINALITY - count of elements
    DBMS_OUTPUT.PUT_LINE('CARDINALITY(v_set1): ' || CARDINALITY(v_set1));
    
    -- SET - remove duplicates
    v_set1 := t_numbers(1, 2, 2, 3, 3, 3);
    v_result := SET(v_set1);
    DBMS_OUTPUT.PUT_LINE('SET (distinct): ' || v_result.COUNT);
    
    -- IS A SET - check if all elements unique
    IF v_result IS A SET THEN
        DBMS_OUTPUT.PUT_LINE('v_result is a set (no duplicates)');
    END IF;
    
    -- IS EMPTY
    v_result := t_numbers();
    IF v_result IS EMPTY THEN
        DBMS_OUTPUT.PUT_LINE('v_result is empty');
    END IF;
    
    -- MEMBER OF
    v_set1 := t_numbers(1, 2, 3, 4, 5);
    IF 3 MEMBER OF v_set1 THEN
        DBMS_OUTPUT.PUT_LINE('3 is a member of v_set1');
    END IF;
    
    -- SUBMULTISET
    v_set2 := t_numbers(2, 3);
    IF v_set2 SUBMULTISET OF v_set1 THEN
        DBMS_OUTPUT.PUT_LINE('v_set2 is a submultiset of v_set1');
    END IF;
END;
/

-- -----------------------------------------------------------------------------
-- 9. Collections in SQL
-- -----------------------------------------------------------------------------

-- Using TABLE() to query collection
SELECT * FROM TABLE(t_number_nt(10, 20, 30, 40, 50));

-- Collection in subquery
SELECT e.employee_id, e.first_name
FROM employees e
WHERE e.department_id IN (
    SELECT COLUMN_VALUE FROM TABLE(t_number_nt(60, 80, 90))
);

-- COLLECT aggregate function
SELECT COLLECT(first_name) AS employee_names
FROM employees
WHERE department_id = 60;

-- CAST and MULTISET to create collection from query
SELECT CAST(MULTISET(
    SELECT salary FROM employees WHERE department_id = 60
) AS t_number_nt) AS salaries
FROM DUAL;

-- Using collection with MEMBER OF in SQL
SELECT *
FROM employees
WHERE department_id MEMBER OF t_number_nt(60, 80, 90);

