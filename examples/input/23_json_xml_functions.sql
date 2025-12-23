-- ============================================================================
-- File: 23_json_xml_functions.sql
-- Description: JSON and XML functions in Oracle
-- ============================================================================

-- =============================================================================
-- PART 1: JSON FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. JSON Generation Functions
-- -----------------------------------------------------------------------------

-- JSON_OBJECT - Create JSON object from key-value pairs
SELECT JSON_OBJECT(
    'employee_id' VALUE employee_id,
    'name' VALUE first_name || ' ' || last_name,
    'salary' VALUE salary,
    'hire_date' VALUE hire_date
) AS emp_json
FROM employees
WHERE ROWNUM <= 3;

-- JSON_OBJECT with NULL handling
SELECT JSON_OBJECT(
    'employee_id' VALUE employee_id,
    'commission' VALUE commission_pct NULL ON NULL,
    'manager' VALUE manager_id ABSENT ON NULL
) AS emp_json
FROM employees
WHERE ROWNUM <= 3;

-- JSON_ARRAY - Create JSON array
SELECT JSON_ARRAY(first_name, last_name, salary) AS name_array
FROM employees
WHERE ROWNUM <= 3;

-- JSON_ARRAY from query
SELECT JSON_ARRAY(
    SELECT first_name FROM employees WHERE department_id = 60
) AS names_array
FROM DUAL;

-- JSON_OBJECTAGG - Aggregate into JSON object
SELECT JSON_OBJECTAGG(
    employee_id VALUE first_name
) AS emp_map
FROM employees
WHERE department_id = 60;

-- JSON_ARRAYAGG - Aggregate into JSON array
SELECT department_id,
       JSON_ARRAYAGG(first_name ORDER BY first_name) AS employees
FROM employees
GROUP BY department_id;

-- Nested JSON structures
SELECT JSON_OBJECT(
    'department' VALUE d.department_name,
    'employees' VALUE (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'name' VALUE e.first_name || ' ' || e.last_name,
                'salary' VALUE e.salary
            )
        )
        FROM employees e
        WHERE e.department_id = d.department_id
    )
) AS dept_json
FROM departments d
WHERE d.department_id IN (60, 80);

-- -----------------------------------------------------------------------------
-- 2. JSON Query Functions
-- -----------------------------------------------------------------------------

-- JSON_VALUE - Extract scalar value
SELECT JSON_VALUE('{"name":"John","age":30}', '$.name') AS name FROM DUAL;
SELECT JSON_VALUE('{"name":"John","age":30}', '$.age' RETURNING NUMBER) AS age FROM DUAL;

-- JSON_VALUE with DEFAULT
SELECT JSON_VALUE('{"name":"John"}', '$.age' DEFAULT 0 ON EMPTY) AS age FROM DUAL;
SELECT JSON_VALUE('{"name":"John"}', '$.age' NULL ON ERROR) AS age FROM DUAL;

-- JSON_QUERY - Extract JSON fragment
SELECT JSON_QUERY('{"person":{"name":"John","address":{"city":"NYC"}}}', '$.person') AS person FROM DUAL;
SELECT JSON_QUERY('{"items":[1,2,3]}', '$.items') AS items FROM DUAL;
SELECT JSON_QUERY('{"items":[1,2,3]}', '$.items[0]' WITH WRAPPER) AS first_item FROM DUAL;

-- JSON_EXISTS - Check if path exists
SELECT * FROM employees
WHERE JSON_EXISTS('{"dept":60}', '$.dept');

-- JSON_TABLE - Convert JSON to relational
SELECT jt.*
FROM (SELECT '{"employees":[{"name":"John","age":30},{"name":"Jane","age":25}]}' AS json_doc FROM DUAL),
     JSON_TABLE(json_doc, '$.employees[*]'
         COLUMNS (
             row_num FOR ORDINALITY,
             name VARCHAR2(100) PATH '$.name',
             age NUMBER PATH '$.age'
         )
     ) jt;

-- Complex JSON_TABLE with nested paths
SELECT jt.*
FROM (
    SELECT '{
        "order_id": 1001,
        "customer": {"name": "John Doe", "email": "john@example.com"},
        "items": [
            {"product": "Widget", "qty": 5, "price": 10.00},
            {"product": "Gadget", "qty": 2, "price": 25.00}
        ]
    }' AS json_doc FROM DUAL
),
JSON_TABLE(json_doc, '$'
    COLUMNS (
        order_id NUMBER PATH '$.order_id',
        customer_name VARCHAR2(100) PATH '$.customer.name',
        customer_email VARCHAR2(100) PATH '$.customer.email',
        NESTED PATH '$.items[*]'
            COLUMNS (
                product VARCHAR2(100) PATH '$.product',
                quantity NUMBER PATH '$.qty',
                price NUMBER PATH '$.price'
            )
    )
) jt;

-- -----------------------------------------------------------------------------
-- 3. JSON Conditions and Operators
-- -----------------------------------------------------------------------------

-- IS JSON - Check if value is valid JSON
SELECT '{"name":"John"}' AS json_text,
       CASE WHEN '{"name":"John"}' IS JSON THEN 'Valid' ELSE 'Invalid' END AS is_valid
FROM DUAL;

-- IS JSON with options
SELECT json_text,
       CASE WHEN json_text IS JSON STRICT THEN 'Strict JSON' ELSE 'Not strict' END AS strict_check
FROM (
    SELECT '{"name":"John"}' AS json_text FROM DUAL UNION ALL
    SELECT '{name:"John"}' FROM DUAL  -- Not strict JSON
);

-- JSON_EQUAL - Compare JSON values
SELECT CASE WHEN JSON_EQUAL('{"a":1,"b":2}', '{"b":2,"a":1}') THEN 'Equal' ELSE 'Not Equal' END AS result
FROM DUAL;

-- DOT notation for JSON columns (requires JSON column)
-- SELECT emp_data.name, emp_data.salary FROM employees_json;

-- -----------------------------------------------------------------------------
-- 4. JSON Updates
-- -----------------------------------------------------------------------------

-- JSON_MERGEPATCH - Update JSON
SELECT JSON_MERGEPATCH(
    '{"name":"John","age":30,"city":"NYC"}',
    '{"age":31,"country":"USA"}'
) AS updated_json
FROM DUAL;

-- JSON_TRANSFORM (Oracle 21c+)
/*
SELECT JSON_TRANSFORM(
    '{"name":"John","age":30}',
    SET '$.age' = 31,
    INSERT '$.city' = 'NYC'
) AS transformed
FROM DUAL;
*/

-- -----------------------------------------------------------------------------
-- 5. JSON Data Guide
-- -----------------------------------------------------------------------------

-- Get JSON schema information
SELECT JSON_DATAGUIDE('{"name":"John","age":30,"address":{"city":"NYC"}}') AS data_guide
FROM DUAL;

-- =============================================================================
-- PART 2: XML FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 6. XML Generation Functions
-- -----------------------------------------------------------------------------

-- XMLELEMENT - Create XML element
SELECT XMLELEMENT("employee",
    XMLELEMENT("name", first_name || ' ' || last_name),
    XMLELEMENT("salary", salary)
) AS emp_xml
FROM employees
WHERE ROWNUM <= 3;

-- XMLELEMENT with attributes
SELECT XMLELEMENT("employee",
    XMLATTRIBUTES(employee_id AS "id", department_id AS "dept"),
    XMLELEMENT("name", first_name),
    XMLELEMENT("salary", salary)
) AS emp_xml
FROM employees
WHERE ROWNUM <= 3;

-- XMLFOREST - Create multiple elements
SELECT XMLELEMENT("employee",
    XMLFOREST(
        employee_id AS "id",
        first_name AS "firstName",
        last_name AS "lastName",
        salary AS "salary"
    )
) AS emp_xml
FROM employees
WHERE ROWNUM <= 3;

-- XMLAGG - Aggregate XML elements
SELECT XMLELEMENT("department",
    XMLATTRIBUTES(department_id AS "id"),
    XMLAGG(
        XMLELEMENT("employee",
            XMLFOREST(first_name AS "name", salary AS "salary")
        ) ORDER BY salary DESC
    )
) AS dept_xml
FROM employees
WHERE department_id = 60
GROUP BY department_id;

-- XMLROOT - Add XML declaration
SELECT XMLROOT(
    XMLELEMENT("employees",
        XMLAGG(XMLELEMENT("emp", first_name))
    ),
    VERSION '1.0'
) AS xml_doc
FROM employees
WHERE ROWNUM <= 3;

-- XMLCONCAT - Concatenate XML
SELECT XMLCONCAT(
    XMLELEMENT("first", first_name),
    XMLELEMENT("last", last_name)
) AS name_xml
FROM employees
WHERE ROWNUM <= 3;

-- XMLCOMMENT and XMLPI
SELECT XMLELEMENT("root",
    XMLCOMMENT('This is a comment'),
    XMLPI(NAME "processor", 'instruction'),
    XMLELEMENT("data", 'content')
) AS xml_with_extras
FROM DUAL;

-- XMLCDATA - Create CDATA section
SELECT XMLELEMENT("script",
    XMLCDATA('if (a < b) { return true; }')
) AS cdata_xml
FROM DUAL;

-- -----------------------------------------------------------------------------
-- 7. XML Query Functions
-- -----------------------------------------------------------------------------

-- EXTRACT (deprecated, use XMLQUERY)
SELECT EXTRACT(
    XMLTYPE('<employee><name>John</name><salary>5000</salary></employee>'),
    '/employee/name/text()'
) AS name_node
FROM DUAL;

-- EXTRACTVALUE (deprecated, use XMLQUERY)
SELECT EXTRACTVALUE(
    XMLTYPE('<employee><name>John</name><salary>5000</salary></employee>'),
    '/employee/name'
) AS name_value
FROM DUAL;

-- XMLQUERY - XQuery expression
SELECT XMLQUERY(
    '/employee/name/text()'
    PASSING XMLTYPE('<employee><name>John</name><salary>5000</salary></employee>')
    RETURNING CONTENT
) AS name_value
FROM DUAL;

-- XMLQUERY with FLWOR expression
SELECT XMLQUERY(
    'for $emp in /employees/employee
     where $emp/salary > 5000
     return $emp/name'
    PASSING XMLTYPE('<employees>
        <employee><name>John</name><salary>6000</salary></employee>
        <employee><name>Jane</name><salary>4000</salary></employee>
    </employees>')
    RETURNING CONTENT
) AS high_earners
FROM DUAL;

-- XMLEXISTS - Check if XPath exists
SELECT * FROM (
    SELECT XMLTYPE('<employee><name>John</name></employee>') AS xml_data FROM DUAL
)
WHERE XMLEXISTS('/employee/name' PASSING xml_data);

-- XMLTABLE - Convert XML to relational
SELECT x.*
FROM XMLTABLE(
    '/employees/employee'
    PASSING XMLTYPE('<employees>
        <employee id="1"><name>John</name><salary>5000</salary></employee>
        <employee id="2"><name>Jane</name><salary>6000</salary></employee>
    </employees>')
    COLUMNS
        emp_id NUMBER PATH '@id',
        emp_name VARCHAR2(100) PATH 'name',
        salary NUMBER PATH 'salary'
) x;

-- Complex XMLTABLE with namespace
SELECT x.*
FROM XMLTABLE(
    XMLNAMESPACES('http://example.com' AS "ns"),
    '/ns:root/ns:item'
    PASSING XMLTYPE('<root xmlns="http://example.com">
        <item><name>Item1</name><price>100</price></item>
        <item><name>Item2</name><price>200</price></item>
    </root>')
    COLUMNS
        item_name VARCHAR2(100) PATH 'ns:name',
        price NUMBER PATH 'ns:price'
) x;

-- -----------------------------------------------------------------------------
-- 8. XMLTYPE Methods
-- -----------------------------------------------------------------------------

DECLARE
    v_xml XMLTYPE;
    v_xml2 XMLTYPE;
    v_clob CLOB;
    v_string VARCHAR2(4000);
BEGIN
    v_xml := XMLTYPE('<employee><name>John</name><salary>5000</salary></employee>');
    
    -- Get string representation
    v_string := v_xml.getStringVal();
    DBMS_OUTPUT.PUT_LINE('String: ' || v_string);
    
    -- Get CLOB representation
    v_clob := v_xml.getClobVal();
    
    -- Check if valid
    IF v_xml.isFragment() = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Is a complete document');
    END IF;
    
    -- Extract value
    DBMS_OUTPUT.PUT_LINE('Name: ' || v_xml.extract('/employee/name/text()').getStringVal());
    
    -- Existsnode
    IF v_xml.existsNode('/employee/name') = 1 THEN
        DBMS_OUTPUT.PUT_LINE('Name element exists');
    END IF;
    
    -- Transform with XSLT
    v_xml2 := XMLTYPE('<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:template match="/">
            <result><xsl:value-of select="/employee/name"/></result>
        </xsl:template>
    </xsl:stylesheet>');
    
    -- DBMS_OUTPUT.PUT_LINE('Transformed: ' || v_xml.transform(v_xml2).getStringVal());
END;
/

-- -----------------------------------------------------------------------------
-- 9. XML Namespace Handling
-- -----------------------------------------------------------------------------

-- XMLNAMESPACES in queries
SELECT XMLQUERY(
    'declare namespace ns="http://example.com"; /ns:root/ns:item'
    PASSING XMLTYPE('<root xmlns="http://example.com"><item>value</item></root>')
    RETURNING CONTENT
) AS result
FROM DUAL;

-- XMLSERIALIZE - Convert XMLTYPE to string
SELECT XMLSERIALIZE(
    DOCUMENT XMLTYPE('<root><item>value</item></root>')
    AS CLOB INDENT SIZE = 2
) AS formatted_xml
FROM DUAL;

-- XMLPARSE - Parse string to XMLTYPE
SELECT XMLPARSE(CONTENT '<item>value</item>' WELLFORMED) AS parsed_xml FROM DUAL;
SELECT XMLPARSE(DOCUMENT '<?xml version="1.0"?><root/>' WELLFORMED) AS parsed_doc FROM DUAL;

-- -----------------------------------------------------------------------------
-- 10. JSON to XML / XML to JSON Conversion
-- -----------------------------------------------------------------------------

-- JSON to XML (using XMLTYPE constructor)
SELECT XMLTYPE('{"name":"John","age":30}').transform(
    XMLTYPE('<?xml version="1.0"?>
        <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
            <xsl:output method="xml"/>
            <xsl:template match="/">
                <person>
                    <name><xsl:value-of select="/json/name"/></name>
                </person>
            </xsl:template>
        </xsl:stylesheet>')
) AS xml_from_json
FROM DUAL;

-- Manual JSON to XML conversion
SELECT XMLELEMENT("person",
    XMLELEMENT("name", JSON_VALUE('{"name":"John","age":30}', '$.name')),
    XMLELEMENT("age", JSON_VALUE('{"name":"John","age":30}', '$.age'))
) AS xml_output
FROM DUAL;

-- XML to JSON (manual conversion)
SELECT JSON_OBJECT(
    'name' VALUE EXTRACTVALUE(XMLTYPE('<person><name>John</name><age>30</age></person>'), '/person/name'),
    'age' VALUE EXTRACTVALUE(XMLTYPE('<person><name>John</name><age>30</age></person>'), '/person/age')
) AS json_output
FROM DUAL;

-- -----------------------------------------------------------------------------
-- 11. Practical Examples
-- -----------------------------------------------------------------------------

-- Generate JSON API response
SELECT JSON_OBJECT(
    'status' VALUE 'success',
    'data' VALUE JSON_OBJECT(
        'employees' VALUE (
            SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'id' VALUE employee_id,
                    'name' VALUE first_name || ' ' || last_name,
                    'email' VALUE email,
                    'salary' VALUE salary
                ) ORDER BY employee_id
            )
            FROM employees
            WHERE department_id = 60
        ),
        'count' VALUE (SELECT COUNT(*) FROM employees WHERE department_id = 60)
    ),
    'timestamp' VALUE TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
) AS api_response
FROM DUAL;

-- Generate XML report
SELECT XMLROOT(
    XMLELEMENT("report",
        XMLATTRIBUTES(SYSDATE AS "generated"),
        XMLELEMENT("summary",
            XMLFOREST(
                (SELECT COUNT(*) FROM employees) AS "totalEmployees",
                (SELECT SUM(salary) FROM employees) AS "totalSalary",
                (SELECT AVG(salary) FROM employees) AS "avgSalary"
            )
        ),
        XMLELEMENT("departments",
            (SELECT XMLAGG(
                XMLELEMENT("department",
                    XMLATTRIBUTES(d.department_id AS "id"),
                    XMLFOREST(
                        d.department_name AS "name",
                        (SELECT COUNT(*) FROM employees e WHERE e.department_id = d.department_id) AS "empCount"
                    )
                )
            )
            FROM departments d)
        )
    ),
    VERSION '1.0'
) AS xml_report
FROM DUAL;

-- Parse and process JSON in PL/SQL
DECLARE
    v_json CLOB := '{"employees":[{"name":"John","salary":5000},{"name":"Jane","salary":6000}]}';
    v_name VARCHAR2(100);
    v_salary NUMBER;
BEGIN
    FOR rec IN (
        SELECT jt.*
        FROM JSON_TABLE(v_json, '$.employees[*]'
            COLUMNS (
                name VARCHAR2(100) PATH '$.name',
                salary NUMBER PATH '$.salary'
            )
        ) jt
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(rec.name || ': $' || rec.salary);
    END LOOP;
END;
/

