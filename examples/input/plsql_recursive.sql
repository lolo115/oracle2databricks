DECLARE
    CURSOR c_dept_tree IS
        SELECT LPAD(' ', 2*(LEVEL-1)) || dept_name AS indented_name,
               dept_id,
               parent_dept_id
        FROM departments
        START WITH parent_dept_id IS NULL
        CONNECT BY PRIOR dept_id = parent_dept_id;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Department Hierarchy:');
    FOR r IN c_dept_tree LOOP
        DBMS_OUTPUT.PUT_LINE(r.indented_name);
    END LOOP;
END;
/
