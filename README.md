# Oracle to Databricks SQL Translator

A comprehensive Python tool for translating Oracle SQL and PL/SQL code to Databricks SQL using the [sqlglot](https://github.com/tobymao/sqlglot) framework.

## Features

- **SQL Translation**: Convert Oracle SQL queries to Databricks SQL
- **PL/SQL Conversion**: Convert stored procedures, functions, and packages
- **Function Mapping**: Automatic mapping of Oracle functions to Databricks equivalents
- **Data Type Conversion**: Oracle data types to Databricks data types
- **Custom Rules**: Define your own regex-based transformations for in-house functions ([see documentation](extra_config/README.md))
- **CLI Interface**: Easy-to-use command line interface
- **Interactive Mode**: Quick translations without file operations

## Installation

```bash
# Clone or navigate to the project directory
cd oracle2databricks

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

### Command Line Usage

```bash
# Convert any Oracle file (auto-detects SQL vs PL/SQL)
python ora2databricks.py convert input.sql --output output.sql

# Convert with custom transformation rules (for in-house functions)
python ora2databricks.py convert input.sql -o output.sql --config extra_config/my_rules.json

# Convert with verbose output
python ora2databricks.py convert input.sql -o output.sql --verbose

# Convert with detailed conversion report
python ora2databricks.py convert input.sql -o output.sql --report

# Convert with JSON report saved to file
python ora2databricks.py convert input.sql -o out.sql --report --report-format json --report-output report.json

# Batch convert a directory (auto-detects each file type)
python ora2databricks.py batch ./oracle_scripts ./databricks_scripts --recursive

# Batch convert with custom rules
python ora2databricks.py batch ./oracle_scripts ./databricks_scripts -r --config extra_config/my_rules.json

# Batch convert with report
python ora2databricks.py batch ./oracle_scripts ./databricks_scripts -r --report

# Interactive mode
python ora2databricks.py interactive

# Quick inline translation
python ora2databricks.py inline "SELECT SYSDATE FROM DUAL"

# Generate a custom rules configuration file
python ora2databricks.py init-config --output extra_config/my_rules.json

# Validate a custom rules configuration
python ora2databricks.py validate-config extra_config/my_rules.json
```

### Single File Conversion

The `convert` command auto-detects whether the file contains SQL, PL/SQL, or mixed content:

```bash
python ora2databricks.py convert <input_file> [options]
```

**Options:**
- `--output, -o`: Output file path (prints to stdout if not specified)
- `--config, -c`: Path to custom rules JSON file ([see custom rules documentation](extra_config/README.md))
- `--format, -f`: Format output SQL with indentation (default: True)
- `--verbose, -v`: Show detailed conversion notes and suggestions
- `--report, -R`: Generate a detailed conversion report after conversion
- `--report-format`: Format for the conversion report (`text` or `json`, default: text)
- `--report-output`: Write report to file instead of stdout

### Batch Processing

The `batch` command processes all SQL files in a directory and writes converted files to an output directory:

```bash
python ora2databricks.py batch <input_directory> <output_directory> [options]
```

**Options:**
- `--config, -c`: Path to custom rules JSON file ([see custom rules documentation](extra_config/README.md))
- `--recursive, -r`: Process subdirectories recursively
- `--report, -R`: Generate a detailed conversion report after batch processing
- `--report-format`: Format for the conversion report (`text` or `json`, default: text)
- `--report-output`: Write report to file instead of stdout

**Supported file extensions:** `.sql`, `.pls`, `.pks`, `.pkb`, `.plb`, `.prc`, `.fnc`, `.trg`

The batch command automatically detects whether each file contains SQL, PL/SQL, or mixed content and applies the appropriate conversion.

### Inline Translation

Translate a single SQL statement directly from the command line:

```bash
python ora2databricks.py inline "SELECT SYSDATE FROM DUAL"
```

**Options:**
- `--config, -c`: Path to custom rules JSON file
- `--format, -f`: Format output SQL (default: True)
- `--verbose, -v`: Show detailed conversion notes and suggestions

### Python API

```python
from oracle2databricks import OracleToDatabricksTranslator, PLSQLConverter

# Translate SQL
translator = OracleToDatabricksTranslator()
result = translator.translate("SELECT SYSDATE FROM DUAL")
print(result.translated_sql)
# Output: SELECT CURRENT_TIMESTAMP()

# Convert PL/SQL
converter = PLSQLConverter()
result = converter.convert("""
CREATE OR REPLACE PROCEDURE hello_world IS
BEGIN
    DBMS_OUTPUT.PUT_LINE('Hello!');
END;
""")
print(result.converted_code)
```

## Supported Conversions

### SQL Functions

#### String Functions

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `SUBSTR(s, p, l)` | `SUBSTRING(s, p, l)` | |
| `INSTR(s, sub)` | `INSTR(s, sub)` | Native support |
| `LENGTH(s)` | `LENGTH(s)` | Native support |
| `UPPER/LOWER/INITCAP` | `UPPER/LOWER/INITCAP` | Native support |
| `TRIM/LTRIM/RTRIM` | `TRIM/LTRIM/RTRIM` | Native support |
| `LPAD/RPAD` | `LPAD/RPAD` | Native support |
| `REPLACE` | `REPLACE` | Native support |
| `TRANSLATE` | `TRANSLATE` | Native support |
| `CONCAT` / `\|\|` | `CONCAT` | String concatenation |
| `REVERSE` | `REVERSE` | Native support |
| `SOUNDEX` | `SOUNDEX` | Native support |
| `REGEXP_REPLACE` | `REGEXP_REPLACE` | Native support |
| `REGEXP_SUBSTR` | `REGEXP_EXTRACT` | Different syntax |
| `REGEXP_LIKE` | `RLIKE` | Different syntax |
| `REGEXP_INSTR` | `REGEXP_INSTR` | Native support |
| `REGEXP_COUNT` | Custom | Uses `REGEXP_EXTRACT_ALL` |

#### Numeric Functions

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `ABS/CEIL/FLOOR` | `ABS/CEIL/FLOOR` | Native support |
| `ROUND/TRUNC` | `ROUND/TRUNC` | Native support |
| `MOD` | `MOD` | Native support |
| `POWER/SQRT` | `POWER/SQRT` | Native support |
| `SIGN` | `SIGN` | Native support |
| `EXP/LN/LOG` | `EXP/LN/LOG` | Native support |
| `SIN/COS/TAN/ASIN/ACOS/ATAN` | Same | Trigonometric functions |
| `SINH/COSH/TANH` | Same | Hyperbolic functions |
| `BITAND` | `BITAND` | Native support |
| `WIDTH_BUCKET` | `WIDTH_BUCKET` | Native support |

#### Date/Time Functions

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `SYSDATE` | `CURRENT_DATE()` / `CURRENT_TIMESTAMP()` | |
| `SYSTIMESTAMP` | `CURRENT_TIMESTAMP()` | |
| `ADD_MONTHS` | `ADD_MONTHS` | Native support |
| `MONTHS_BETWEEN` | `MONTHS_BETWEEN` | Native support |
| `LAST_DAY` | `LAST_DAY` | Native support |
| `NEXT_DAY` | `NEXT_DAY` | Native support |
| `EXTRACT` | `EXTRACT` | Native support |
| `TO_DATE` | `TO_DATE` | Format conversion applied |
| `TO_CHAR` | `TO_CHAR` / `DATE_FORMAT` | Format conversion applied |
| `TO_TIMESTAMP` | `TO_TIMESTAMP` | Format conversion applied |
| `TRUNC(date)` | `TRUNC` | For date truncation |

#### Conversion & Conditional Functions

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `NVL(a, b)` | `NVL(a, b)` | Native support |
| `NVL2(a, b, c)` | `NVL2(a, b, c)` | Native support |
| `DECODE(...)` | `CASE WHEN ... END` | Converted to CASE |
| `COALESCE` | `COALESCE` | Native support |
| `NULLIF` | `NULLIF` | Native support |
| `GREATEST/LEAST` | `GREATEST/LEAST` | Native support |
| `TO_NUMBER(x)` | `CAST(x AS DECIMAL)` | |
| `CAST` | `CAST` | Native support |
| `RAWTOHEX` | `HEX` | |
| `HEXTORAW` | `UNHEX` | |

#### Aggregate Functions

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `COUNT/SUM/AVG/MIN/MAX` | Same | Native support |
| `LISTAGG(...)` | `ARRAY_JOIN(COLLECT_LIST(...))` | String aggregation |
| `WM_CONCAT` | `ARRAY_JOIN(COLLECT_LIST(...))` | Deprecated Oracle function |
| `MEDIAN` | `PERCENTILE(col, 0.5)` | |
| `STDDEV/VARIANCE` | `STDDEV/VARIANCE` | Native support |
| `STDDEV_POP/STDDEV_SAMP` | Same | Native support |
| `VAR_POP/VAR_SAMP` | Same | Native support |
| `CORR/COVAR_POP/COVAR_SAMP` | Same | Native support |
| `COLLECT` | `COLLECT_LIST` | |
| `APPROX_COUNT_DISTINCT` | `APPROX_COUNT_DISTINCT` | Native support |

#### Analytic/Window Functions

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `ROW_NUMBER` | `ROW_NUMBER` | Native support |
| `RANK/DENSE_RANK` | `RANK/DENSE_RANK` | Native support |
| `NTILE` | `NTILE` | Native support |
| `LEAD/LAG` | `LEAD/LAG` | Native support |
| `FIRST_VALUE/LAST_VALUE` | `FIRST_VALUE/LAST_VALUE` | Native support |
| `NTH_VALUE` | `NTH_VALUE` | Native support |
| `CUME_DIST/PERCENT_RANK` | Same | Native support |
| `PERCENTILE_CONT/DISC` | Same | Native support |
| `RATIO_TO_REPORT` | `expr / SUM(expr) OVER()` | Converted |
| `ROWNUM` | `ROW_NUMBER() OVER()` | Converted |

#### JSON Functions (Oracle 12c+)

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `JSON_VALUE` | `GET_JSON_OBJECT` | Different syntax |
| `JSON_QUERY` | `GET_JSON_OBJECT` | Returns JSON fragment |
| `JSON_OBJECT` | `TO_JSON` | |
| `JSON_ARRAY` | `TO_JSON` | |
| `JSON_EXISTS` | `GET_JSON_OBJECT IS NOT NULL` | |
| `JSON_ARRAYAGG` | `TO_JSON(COLLECT_LIST())` | |

#### Other Functions

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `SYS_GUID` | `UUID()` | |
| `ORA_HASH` | `HASH()` | |
| `STANDARD_HASH` | `SHA2` | Default SHA256 |
| `USER` | `CURRENT_USER()` | |
| `USERENV(param)` | Various | Mapped to Databricks equivalents |
| `SYS_CONTEXT` | Various | Mapped to Databricks equivalents |

### SQL Syntax Conversions

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `a.col = b.col(+)` | `LEFT OUTER JOIN` | Oracle outer join syntax |
| `a.col(+) = b.col` | `RIGHT OUTER JOIN` | Oracle outer join syntax |
| `FROM DUAL` | (removed) | Not needed in Databricks |
| `/*+ hints */` | (removed) | Oracle hints not applicable |
| `CONNECT BY` | `WITH RECURSIVE` | Hierarchical queries |
| `START WITH` | CTE anchor | Part of recursive CTE |
| `LEVEL` | Computed column | Tracked in recursive CTE |
| `SYS_CONNECT_BY_PATH` | Path concatenation | In recursive CTE |
| `CONNECT_BY_ROOT` | Root value tracking | In recursive CTE |
| `ROWNUM <= N` | `LIMIT N` | Row limiting |

**Outer Join Example:**

```sql
-- Oracle (implicit join with (+) notation)
SELECT e.employee_id, d.department_name
FROM employees e, departments d
WHERE e.department_id = d.department_id(+)

-- Databricks (explicit JOIN syntax)
SELECT e.employee_id, d.department_name
FROM employees e
LEFT OUTER JOIN departments d ON e.department_id = d.department_id
```

### Data Types

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `VARCHAR2(n)` | `STRING` | |
| `NVARCHAR2(n)` | `STRING` | |
| `CHAR(n)` | `STRING` | |
| `CLOB` / `NCLOB` | `STRING` | |
| `LONG` | `STRING` | |
| `NUMBER` | `DECIMAL(38,10)` | Default precision |
| `NUMBER(p)` | `DECIMAL(p,0)` or `INT/BIGINT` | Based on precision |
| `NUMBER(p,s)` | `DECIMAL(p,s)` | |
| `BINARY_FLOAT` | `FLOAT` | |
| `BINARY_DOUBLE` | `DOUBLE` | |
| `INTEGER` / `INT` | `INT` | |
| `DATE` | `TIMESTAMP` | Oracle DATE includes time |
| `TIMESTAMP` | `TIMESTAMP` | |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` | TZ info may be lost |
| `RAW` / `LONG RAW` | `BINARY` | |
| `BLOB` | `BINARY` | |
| `BOOLEAN` | `BOOLEAN` | |
| `XMLTYPE` | `STRING` | |
| `ROWID` / `UROWID` | `STRING` | |

### PL/SQL Constructs

#### Stored Procedures

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `CREATE PROCEDURE` | `CREATE PROCEDURE ... LANGUAGE SQL` | |
| `CREATE OR REPLACE PROCEDURE` | `CREATE OR REPLACE PROCEDURE` | |
| `IN` parameters | Parameters | Direct mapping |
| `OUT` parameters | ⚠️ Manual review | Use STRUCT returns or temp tables |
| `IN OUT` parameters | ⚠️ Manual review | Use STRUCT returns or temp tables |
| `DEFAULT` values | `DEFAULT` | Native support |

**Procedure Example:**

```sql
-- Oracle
CREATE OR REPLACE PROCEDURE update_salary(
    p_emp_id IN NUMBER,
    p_amount IN NUMBER
) IS
BEGIN
    UPDATE employees SET salary = p_amount WHERE employee_id = p_emp_id;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Updated employee ' || p_emp_id);
END;

-- Databricks
CREATE OR REPLACE PROCEDURE update_salary(p_emp_id DECIMAL, p_amount DECIMAL)
LANGUAGE SQL
AS $$
BEGIN
  UPDATE employees SET salary = p_amount WHERE employee_id = p_emp_id;
  -- Note: Databricks uses auto-commit
  SELECT 'Updated employee ' || p_emp_id;
END;
$$;
```

#### Stored Functions

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `CREATE FUNCTION` | `CREATE FUNCTION ... LANGUAGE SQL` | |
| `RETURN` | `RETURN` | Native support |
| `DETERMINISTIC` | `DETERMINISTIC` | Native support |
| `PIPELINED` | ⚠️ Manual review | No direct equivalent |

**Function Example:**

```sql
-- Oracle
CREATE OR REPLACE FUNCTION get_full_name(p_emp_id NUMBER)
RETURN VARCHAR2
IS
    v_name VARCHAR2(100);
BEGIN
    SELECT first_name || ' ' || last_name INTO v_name
    FROM employees WHERE employee_id = p_emp_id;
    RETURN v_name;
END;

-- Databricks
CREATE OR REPLACE FUNCTION get_full_name(p_emp_id DECIMAL)
RETURNS STRING
LANGUAGE SQL
AS $$
  SELECT first_name || ' ' || last_name
  FROM employees WHERE employee_id = p_emp_id
$$;
```

#### Control Structures

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `IF...THEN...ELSIF...ELSE...END IF` | `IF...THEN...ELSEIF...ELSE...END IF` | `ELSIF` → `ELSEIF` |
| `CASE...WHEN...END CASE` | `CASE...WHEN...END CASE` | Native support |
| `LOOP...END LOOP` | `LOOP...END LOOP` | Native support |
| `WHILE...LOOP` | `WHILE...DO...END WHILE` | Different syntax |
| `FOR...IN...LOOP` | `FOR...IN...DO...END FOR` | Different syntax |
| `EXIT` / `EXIT WHEN` | `LEAVE` / `IF...LEAVE` | |
| `CONTINUE` | `ITERATE` | |
| `GOTO` | ⚠️ Not supported | Restructure logic |

#### Variable Handling

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `DECLARE` | `DECLARE` | Native support |
| `:=` assignment | `SET var = value` or `=` | |
| `%TYPE` | ⚠️ Manual review | Specify explicit type |
| `%ROWTYPE` | ⚠️ Manual review | Use STRUCT or explicit columns |
| `CONSTANT` | Not supported | Use regular variable |
| `NOT NULL` | Not enforced | Validation in code |

#### Exception Handling

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `EXCEPTION` | `EXCEPTION` | Native support |
| `WHEN NO_DATA_FOUND` | `WHEN NO_DATA_FOUND` | Native support |
| `WHEN TOO_MANY_ROWS` | ⚠️ Manual review | Handle with LIMIT |
| `WHEN OTHERS` | `WHEN OTHER` | Different keyword |
| `RAISE_APPLICATION_ERROR` | `RAISE USING MESSAGE` | Different syntax |
| `SQLERRM` | Error message | In exception handler |
| `SQLCODE` | Error code | In exception handler |

#### Cursor Operations

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `CURSOR...IS SELECT` | `DECLARE cursor CURSOR FOR` | Different syntax |
| `OPEN cursor` | `OPEN cursor` | Native support |
| `FETCH cursor INTO` | `FETCH cursor INTO` | Native support |
| `CLOSE cursor` | `CLOSE cursor` | Native support |
| `cursor%FOUND` | ⚠️ Manual review | Check fetch result |
| `cursor%NOTFOUND` | ⚠️ Manual review | Check fetch result |
| `cursor%ROWCOUNT` | ⚠️ Manual review | Use counter variable |
| `FOR rec IN cursor LOOP` | `FOR rec IN cursor DO` | Different syntax |

#### Other PL/SQL Features

| Oracle | Databricks | Notes |
|--------|------------|-------|
| `DBMS_OUTPUT.PUT_LINE` | `SELECT` statement | For debugging |
| `COMMIT` / `ROLLBACK` | (removed) | Auto-commit in Databricks |
| `AUTONOMOUS_TRANSACTION` | ⚠️ Not supported | Restructure logic |
| `BULK COLLECT` | ⚠️ Manual review | Use set-based operations |
| `FORALL` | ⚠️ Manual review | Use set-based operations |
| Anonymous blocks | ⚠️ Manual review | Convert to procedures |

#### Packages

Oracle packages are converted to individual procedures and functions:

```sql
-- Oracle Package
CREATE OR REPLACE PACKAGE emp_pkg AS
    PROCEDURE hire_employee(p_name VARCHAR2);
    FUNCTION get_salary(p_emp_id NUMBER) RETURN NUMBER;
END emp_pkg;

-- Databricks (separate objects)
CREATE OR REPLACE PROCEDURE emp_pkg_hire_employee(p_name STRING) ...
CREATE OR REPLACE FUNCTION emp_pkg_get_salary(p_emp_id DECIMAL) ...
```

## Limitations & Manual Review Items

Some Oracle features don't have direct equivalents in Databricks and require manual review:

1. **Packages**: Databricks doesn't support packages. They're converted to individual procedures/functions.

2. **Triggers**: Oracle database triggers aren't supported. Consider:
   - Delta Lake Change Data Feed
   - Structured Streaming
   - Databricks Workflows

3. **OUT Parameters**: Databricks doesn't support OUT parameters the same way. Consider:
   - Returning STRUCT types
   - Using temporary tables

4. **Sequences**: Use Databricks IDENTITY columns or custom implementations.

5. **CONNECT BY**: Hierarchical queries are **automatically converted** to recursive CTEs. Supported features:
   - `START WITH` and `CONNECT BY PRIOR` → Recursive CTE
   - `LEVEL` pseudo-column → Tracked `level` column
   - `SYS_CONNECT_BY_PATH` → Path concatenation
   - `CONNECT_BY_ROOT` → Root value tracking
   - `CONNECT BY LEVEL <= N` → `RANGE()` function
   
   Complex cases (CONNECT_BY_ISLEAF, ORDER SIBLINGS BY) may need review.

6. **UTL_FILE**: Use Databricks file APIs or cloud storage.

7. **BULK COLLECT/FORALL**: Convert to set-based operations.

## Project Structure

```
oracle2databricks/
├── oracle2databricks/
│   ├── __init__.py
│   ├── translator.py           # Main SQL translator
│   ├── plsql_converter.py      # PL/SQL conversion
│   ├── transformations.py      # Custom transformations
│   ├── function_mappings.py    # Function & type mappings
│   ├── connect_by_converter.py # CONNECT BY → recursive CTE
│   ├── function_detector.py    # Oracle function detection
│   ├── custom_rules.py         # Custom regex-based rules engine
│   └── report_generator.py     # Conversion report generation
├── extra_config/
│   ├── README.md               # Custom rules documentation
│   └── custom_rules.sample.json # Sample configuration template
├── examples/
│   ├── input/                  # Example Oracle SQL files
│   └── output/                 # Converted output files
├── ora2databricks.py           # Command line interface
├── requirements.txt
└── README.md
```

## Conversion Examples

### Complex Query with Multiple Conversions

**Oracle:**
```sql
SELECT e.employee_id,
       NVL(e.commission_pct, 0) as commission,
       DECODE(e.department_id, 10, 'Admin', 20, 'IT', 'Other') as dept,
       TO_CHAR(e.hire_date, 'YYYY-MM-DD') as hire_date
FROM employees e, departments d
WHERE e.department_id = d.department_id(+)
  AND ROWNUM <= 100;
```

**Databricks:**
```sql
SELECT
  e.employee_id,
  NVL(e.commission_pct, 0) AS commission,
  CASE
    WHEN e.department_id = 10 THEN 'Admin'
    WHEN e.department_id = 20 THEN 'IT'
    ELSE 'Other'
  END AS dept,
  TO_CHAR(e.hire_date, 'yyyy-MM-dd') AS hire_date
FROM employees AS e
LEFT OUTER JOIN departments AS d
  ON e.department_id = d.department_id
LIMIT 100
```

### Hierarchical Query (CONNECT BY → Recursive CTE)

**Oracle:**
```sql
SELECT employee_id, first_name, manager_id, LEVEL,
       SYS_CONNECT_BY_PATH(first_name, '/') as path
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;
```

**Databricks:**
```sql
WITH RECURSIVE hierarchy_cte AS (
    -- Anchor member: root nodes (START WITH condition)
    SELECT employee_id, first_name, manager_id, 1 AS level,
           CONCAT('/', first_name) AS path
    FROM employees AS e
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive member: child nodes (CONNECT BY condition)
    SELECT e.employee_id, e.first_name, e.manager_id, 
           hierarchy_cte.level + 1 AS level,
           CONCAT(hierarchy_cte.path, '/', e.first_name) AS path
    FROM employees AS e
    INNER JOIN hierarchy_cte ON e.manager_id = hierarchy_cte.employee_id
)
SELECT employee_id, first_name, manager_id, level, path
FROM hierarchy_cte;
```

### Analytic Functions with LISTAGG

**Oracle:**
```sql
SELECT department_id,
       LISTAGG(first_name, ', ') WITHIN GROUP (ORDER BY hire_date) as employees,
       MEDIAN(salary) as median_sal,
       RATIO_TO_REPORT(SUM(salary)) OVER () as sal_ratio
FROM employees
GROUP BY department_id;
```

**Databricks:**
```sql
SELECT department_id,
       ARRAY_JOIN(COLLECT_LIST(first_name), ', ') AS employees,
       PERCENTILE(salary, 0.5) AS median_sal,
       SUM(salary) / SUM(SUM(salary)) OVER () AS sal_ratio
FROM employees
GROUP BY department_id;
```

### Date Format Conversions

**Oracle:**
```sql
SELECT TO_CHAR(hire_date, 'DD-MON-YYYY HH24:MI:SS') as formatted_date,
       TO_DATE('2024-01-15', 'YYYY-MM-DD') as parsed_date,
       ADD_MONTHS(SYSDATE, 3) as future_date,
       MONTHS_BETWEEN(SYSDATE, hire_date) as months_employed
FROM employees;
```

**Databricks:**
```sql
SELECT TO_CHAR(hire_date, 'dd-MMM-yyyy HH:mm:ss') AS formatted_date,
       TO_DATE('2024-01-15', 'yyyy-MM-dd') AS parsed_date,
       ADD_MONTHS(CURRENT_DATE(), 3) AS future_date,
       MONTHS_BETWEEN(CURRENT_DATE(), hire_date) AS months_employed
FROM employees;
```

### PL/SQL with Exception Handling

**Oracle:**
```sql
CREATE OR REPLACE PROCEDURE transfer_employee(
    p_emp_id IN NUMBER,
    p_new_dept_id IN NUMBER
) IS
    v_old_dept NUMBER;
    e_invalid_dept EXCEPTION;
BEGIN
    -- Validate department
    SELECT COUNT(*) INTO v_old_dept FROM departments WHERE department_id = p_new_dept_id;
    IF v_old_dept = 0 THEN
        RAISE e_invalid_dept;
    END IF;
    
    -- Perform transfer
    UPDATE employees SET department_id = p_new_dept_id WHERE employee_id = p_emp_id;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Transfer successful');
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Employee not found');
        ROLLBACK;
    WHEN e_invalid_dept THEN
        DBMS_OUTPUT.PUT_LINE('Invalid department');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        ROLLBACK;
END;
```

**Databricks:**
```sql
CREATE OR REPLACE PROCEDURE transfer_employee(p_emp_id DECIMAL, p_new_dept_id DECIMAL)
LANGUAGE SQL
AS $$
DECLARE
  v_old_dept DECIMAL;
BEGIN
  -- Validate department
  SELECT COUNT(*) INTO v_old_dept FROM departments WHERE department_id = p_new_dept_id;
  IF v_old_dept = 0 THEN
    RAISE USING MESSAGE = 'Invalid department';
  END IF;
  
  -- Perform transfer
  UPDATE employees SET department_id = p_new_dept_id WHERE employee_id = p_emp_id;
  SELECT 'Transfer successful';
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    SELECT 'Employee not found';
  WHEN OTHER THEN
    SELECT 'Error occurred';
END;
$$;
```

## Custom Rules for In-House Functions

If your Oracle codebase contains in-house functions or packages that are not covered by the default conversion, you can define custom transformation rules using regex patterns.

See the **[Custom Rules Documentation](extra_config/README.md)** for detailed instructions.

### Quick Example

```bash
# Generate a configuration file
python ora2databricks.py init-config --output extra_config/my_rules.json

# Edit the file to add your custom rules
# Then use it during conversion
python ora2databricks.py convert input.sql -o output.sql --config extra_config/my_rules.json
```

### Sample Rule

```json
{
  "name": "Convert MY_CORP_FUNC",
  "pattern": "MY_CORP_FUNC\\s*\\(\\s*([^)]+)\\s*\\)",
  "replacement": "DATABRICKS_EQUIVALENT(\\1)",
  "flags": ["IGNORECASE"],
  "enabled": true
}
```

## Contributing

Contributions are welcome! Areas that could use enhancement:

- Additional Oracle function mappings
- Improved CONNECT BY to recursive CTE conversion
- Better package body parsing
- Support for more PL/SQL constructs
- Additional custom rule examples

## License

MIT License

