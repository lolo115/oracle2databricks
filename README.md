# Oracle to Databricks SQL Translator

A comprehensive Python tool for translating Oracle SQL and PL/SQL code to Databricks SQL using the [sqlglot](https://github.com/tobymao/sqlglot) framework.

## Features

- **SQL Translation**: Convert Oracle SQL queries to Databricks SQL
- **PL/SQL Conversion**: Convert stored procedures, functions, and packages
- **Function Mapping**: Automatic mapping of Oracle functions to Databricks equivalents
- **Data Type Conversion**: Oracle data types to Databricks data types
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
# Translate a SQL file
python cli.py translate examples/oracle_queries.sql --output output.sql

# Convert PL/SQL procedures
python cli.py convert-plsql examples/oracle_procedures.sql --output databricks_procs.sql

# Batch translate a directory (supports subdirectories with --recursive)
python cli.py batch ./oracle_scripts ./databricks_output --recursive

# Interactive mode
python cli.py interactive

# Quick inline translation
python cli.py inline "SELECT SYSDATE FROM DUAL"
```

### Batch Processing

The `batch` command processes all SQL files in a directory and writes converted files to an output directory:

```bash
python cli.py batch <input_directory> <output_directory> [options]
```

**Options:**
- `--recursive, -r`: Process subdirectories recursively
- `--report, -R`: Generate a detailed conversion report

**Supported file extensions:** `.sql`, `.pls`, `.pks`, `.pkb`, `.plb`, `.prc`, `.fnc`, `.trg`

The batch command automatically detects whether each file contains SQL or PL/SQL and applies the appropriate conversion.

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

| Oracle | Databricks |
|--------|------------|
| `SYSDATE` | `CURRENT_TIMESTAMP()` |
| `NVL(a, b)` | `COALESCE(a, b)` |
| `NVL2(a, b, c)` | `IF(a IS NOT NULL, b, c)` |
| `DECODE(...)` | `CASE WHEN ... END` |
| `TO_NUMBER(x)` | `CAST(x AS DECIMAL)` |
| `SUBSTR(s, p, l)` | `SUBSTRING(s, p, l)` |
| `LISTAGG(...)` | `ARRAY_JOIN(COLLECT_LIST(...))` |
| `ROWNUM` | `ROW_NUMBER() OVER()` |

### SQL Syntax

| Oracle | Databricks |
|--------|------------|
| `a.col = b.col(+)` | `LEFT OUTER JOIN` |
| `a.col(+) = b.col` | `RIGHT OUTER JOIN` |
| `FROM DUAL` | (removed - not needed) |
| `/*+ hints */` | (removed - not applicable) |

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

| Oracle | Databricks |
|--------|------------|
| `VARCHAR2(n)` | `STRING` |
| `NUMBER(p,s)` | `DECIMAL(p,s)` |
| `DATE` | `TIMESTAMP` |
| `CLOB` | `STRING` |
| `BLOB` | `BINARY` |
| `RAW` | `BINARY` |

### PL/SQL Constructs

| Oracle | Databricks |
|--------|------------|
| `CREATE PROCEDURE` | `CREATE PROCEDURE ... LANGUAGE SQL` |
| `CREATE FUNCTION` | `CREATE FUNCTION ... LANGUAGE SQL` |
| `DBMS_OUTPUT.PUT_LINE` | `SELECT` (for debugging) |
| `RAISE_APPLICATION_ERROR` | `RAISE USING MESSAGE` |
| `:=` assignment | `SET var = value` |
| `EXCEPTION WHEN` | `EXCEPTION WHEN` |

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
│   └── report_generator.py     # Conversion report generation
├── examples/
│   ├── input/                  # Example Oracle SQL files
│   └── output/                 # Converted output files
├── cli.py                      # Command line interface
├── requirements.txt
└── README.md
```

## Examples

### Translating a Complex Query

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
  COALESCE(e.commission_pct, 0) AS commission,
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

### Converting a Hierarchical Query (CONNECT BY)

**Oracle:**
```sql
SELECT employee_id, first_name, manager_id, LEVEL
FROM employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;
```

**Databricks:**
```sql
WITH RECURSIVE hierarchy_cte AS (
    -- Anchor member: root nodes (START WITH condition)
    SELECT employee_id, first_name, manager_id, 1 AS level
    FROM employees AS e
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive member: child nodes (CONNECT BY condition)
    SELECT e.employee_id, e.first_name, e.manager_id, hierarchy_cte.level + 1 AS level
    FROM employees AS e
    INNER JOIN hierarchy_cte ON e.manager_id = hierarchy_cte.employee_id
)
SELECT employee_id, first_name, manager_id, level
FROM hierarchy_cte;
```

### Converting a Stored Procedure

**Oracle:**
```sql
CREATE OR REPLACE PROCEDURE update_salary(
    p_emp_id IN NUMBER,
    p_amount IN NUMBER
) IS
BEGIN
    UPDATE employees SET salary = p_amount WHERE employee_id = p_emp_id;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Updated employee ' || p_emp_id);
END;
```

**Databricks:**
```sql
CREATE OR REPLACE PROCEDURE update_salary(p_emp_id DECIMAL, p_amount DECIMAL)
LANGUAGE SQL
AS $$
BEGIN
  UPDATE employees SET salary = p_amount WHERE employee_id = p_emp_id;
  -- Note: Databricks uses auto-commit
  SELECT 'Updated employee ' || p_emp_id;  -- Converted from DBMS_OUTPUT.PUT_LINE
END;
$$;
```

## Contributing

Contributions are welcome! Areas that could use enhancement:

- Additional Oracle function mappings
- Improved CONNECT BY to recursive CTE conversion
- Better package body parsing
- Support for more PL/SQL constructs

## License

MIT License

