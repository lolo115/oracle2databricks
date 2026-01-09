# Custom Transformation Rules Configuration

This folder contains custom transformation rules that extend the default Oracle to Databricks SQL conversion capabilities.

## Overview

The custom rules feature allows you to define your own regex-based transformations to handle:

- **In-house Oracle functions** that are specific to your organization
- **Custom packages** with proprietary business logic
- **Schema/catalog renaming** for migration purposes
- **Any pattern-based SQL transformations** not covered by the default converter

## Quick Start

### 1. Create Your Configuration File

Copy the sample file and customize it:

```bash
# Option 1: Use the CLI to generate a new config
python ora2databricks.py init-config --output extra_config/my_rules.json

# Option 2: Copy the sample template
cp extra_config/custom_rules.sample.json extra_config/my_rules.json
```

### 2. Edit Your Rules

Open your configuration file and modify the rules:

```json
{
  "settings": {
    "apply_before_default": true,
    "continue_on_error": true
  },
  "custom_rules": [
    {
      "name": "Convert MY_FUNC",
      "description": "Converts MY_FUNC(x) to DATABRICKS_FUNC(x)",
      "pattern": "MY_FUNC\\s*\\(\\s*([^)]+)\\s*\\)",
      "replacement": "DATABRICKS_FUNC(\\1)",
      "flags": ["IGNORECASE"],
      "enabled": true,
      "priority": 100
    }
  ]
}
```

### 3. Use Your Configuration

```bash
# Single file conversion
python ora2databricks.py convert input.sql -o output.sql --config extra_config/my_rules.json

# Batch conversion
python ora2databricks.py batch ./oracle_scripts ./databricks_scripts --config extra_config/my_rules.json

# Inline translation
python ora2databricks.py inline "SELECT MY_FUNC(col1) FROM table1" --config extra_config/my_rules.json
```

## Configuration File Structure

### Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `apply_before_default` | boolean | `true` | If `true`, custom rules are applied before the built-in transformations. If `false`, they are applied after. |
| `continue_on_error` | boolean | `true` | If `true`, continue processing even if a rule fails. If `false`, stop on first error. |

### Rule Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | string | Yes | A descriptive name for the rule (shown in logs) |
| `description` | string | No | Detailed description of what the rule does |
| `pattern` | string | Yes | Regular expression pattern to match (use `\\` for backslashes) |
| `replacement` | string | Yes | Replacement string (use `\\1`, `\\2`, etc. for capture groups) |
| `flags` | array | No | Regex flags: `IGNORECASE`, `MULTILINE`, `DOTALL`, `VERBOSE` |
| `enabled` | boolean | No | Whether the rule is active (default: `true`) |
| `priority` | integer | No | Higher priority rules are applied first (default: `100`) |

## Pattern Writing Guide

### Basic Patterns

```json
// Match a simple function call
"pattern": "MY_FUNC\\(\\)"

// Match a function with one argument
"pattern": "MY_FUNC\\s*\\(\\s*([^)]+)\\s*\\)"

// Match a function with two arguments
"pattern": "MY_FUNC\\s*\\(\\s*([^,]+)\\s*,\\s*([^)]+)\\s*\\)"

// Match a function with three arguments
"pattern": "MY_FUNC\\s*\\(\\s*([^,]+)\\s*,\\s*([^,]+)\\s*,\\s*([^)]+)\\s*\\)"
```

### Replacement Syntax

- `\\1` - First captured group
- `\\2` - Second captured group
- `\\n` - Nth captured group

```json
// Swap arguments
"pattern": "FUNC\\(([^,]+),([^)]+)\\)",
"replacement": "NEW_FUNC(\\2, \\1)"

// Add default argument
"pattern": "FUNC\\(([^)]+)\\)",
"replacement": "FUNC(\\1, 'default_value')"
```

### Special Characters

Remember to escape special regex characters in JSON:

| Character | JSON Escape |
|-----------|-------------|
| `\` | `\\` |
| `(` | `\\(` |
| `)` | `\\)` |
| `.` | `\\.` |
| `*` | `\\*` |
| `+` | `\\+` |
| `?` | `\\?` |
| `[` | `\\[` |
| `]` | `\\]` |
| `{` | `\\{` |
| `}` | `\\}` |
| `^` | `\\^` |
| `$` | `\\$` |
| `|` | `\\|` |

### Regex Flags

| Flag | Short | Description |
|------|-------|-------------|
| `IGNORECASE` | `I` | Case-insensitive matching |
| `MULTILINE` | `M` | `^` and `$` match at line boundaries |
| `DOTALL` | `S` | `.` matches newlines too |
| `VERBOSE` | `X` | Allow whitespace and comments in pattern |

## Examples

### 1. Convert In-House String Function

**Oracle (in-house):**
```sql
SELECT CORP_CONCAT(first_name, ' ', last_name) FROM employees;
```

**Rule:**
```json
{
  "name": "Convert CORP_CONCAT",
  "pattern": "CORP_CONCAT\\s*\\(\\s*([^,]+)\\s*,\\s*([^,]+)\\s*,\\s*([^)]+)\\s*\\)",
  "replacement": "CONCAT(\\1, \\2, \\3)",
  "flags": ["IGNORECASE"]
}
```

**Databricks output:**
```sql
SELECT CONCAT(first_name, ' ', last_name) FROM employees;
```

### 2. Convert Custom Date Function

**Oracle (in-house):**
```sql
SELECT FMT_DATE(hire_date) FROM employees;
```

**Rule:**
```json
{
  "name": "Convert FMT_DATE",
  "pattern": "FMT_DATE\\s*\\(\\s*([^)]+)\\s*\\)",
  "replacement": "DATE_FORMAT(\\1, 'yyyy-MM-dd')",
  "flags": ["IGNORECASE"]
}
```

**Databricks output:**
```sql
SELECT DATE_FORMAT(hire_date, 'yyyy-MM-dd') FROM employees;
```

### 3. Replace Package Function with Databricks UDF

**Oracle:**
```sql
SELECT PKG_HR.GET_EMPLOYEE_NAME(emp_id) FROM dual;
```

**Rule:**
```json
{
  "name": "Convert PKG_HR.GET_EMPLOYEE_NAME",
  "pattern": "PKG_HR\\.GET_EMPLOYEE_NAME\\s*\\(\\s*([^)]+)\\s*\\)",
  "replacement": "hr_catalog.hr_schema.get_employee_name(\\1)",
  "flags": ["IGNORECASE"]
}
```

**Databricks output:**
```sql
SELECT hr_catalog.hr_schema.get_employee_name(emp_id);
```

### 4. Schema/Catalog Migration

**Oracle:**
```sql
SELECT * FROM OLD_SCHEMA.employees;
```

**Rule:**
```json
{
  "name": "Migrate OLD_SCHEMA to new catalog",
  "pattern": "\\bOLD_SCHEMA\\.",
  "replacement": "new_catalog.new_schema.",
  "flags": ["IGNORECASE"]
}
```

**Databricks output:**
```sql
SELECT * FROM new_catalog.new_schema.employees;
```

### 5. Convert Procedure Call to Comment (for manual review)

**Oracle:**
```sql
EXEC PKG_AUDIT.LOG_ACTION('INSERT', 'employees', v_emp_id);
```

**Rule:**
```json
{
  "name": "Flag PKG_AUDIT calls for review",
  "pattern": "PKG_AUDIT\\.LOG_ACTION\\s*\\(([^)]+)\\)",
  "replacement": "-- [TODO: Implement Databricks audit logging] PKG_AUDIT.LOG_ACTION(\\1)",
  "flags": ["IGNORECASE"]
}
```

**Databricks output:**
```sql
-- [TODO: Implement Databricks audit logging] PKG_AUDIT.LOG_ACTION('INSERT', 'employees', v_emp_id);
```

### 6. Convert NVL3 (Custom 3-argument NVL)

**Oracle (in-house):**
```sql
SELECT NVL3(bonus, salary + bonus, salary) FROM employees;
```

**Rule:**
```json
{
  "name": "Convert NVL3",
  "description": "NVL3(a, b, c) returns b if a is not null, else c",
  "pattern": "NVL3\\s*\\(\\s*([^,]+)\\s*,\\s*([^,]+)\\s*,\\s*([^)]+)\\s*\\)",
  "replacement": "IF(\\1 IS NOT NULL, \\2, \\3)",
  "flags": ["IGNORECASE"]
}
```

**Databricks output:**
```sql
SELECT IF(bonus IS NOT NULL, salary + bonus, salary) FROM employees;
```

## Validation

Before using your configuration file, validate it:

```bash
python ora2databricks.py validate-config extra_config/my_rules.json
```

This will check:
- JSON syntax validity
- Required fields presence
- Regex pattern compilation
- Rule priorities and settings

## Git Integration

Files in this folder follow these git rules:

| File | Git Status |
|------|------------|
| `custom_rules.sample.json` | ✅ **Tracked** - Default template |
| `README.md` | ✅ **Tracked** - This documentation |
| `*.json` (other files) | 🚫 **Ignored** - User configurations |

This allows you to:
- Keep the sample template synchronized with the team
- Have personal/project-specific configurations that don't pollute the repository

## Best Practices

1. **Start with the sample**: Copy `custom_rules.sample.json` as a starting point
2. **Use descriptive names**: Makes logs easier to understand
3. **Add descriptions**: Document what each rule does and why
4. **Test incrementally**: Add rules one at a time and verify
5. **Use priorities**: Control the order of rule application
6. **Disable rather than delete**: Set `"enabled": false` to temporarily disable rules
7. **Validate before use**: Run `validate-config` after changes

## Troubleshooting

### Rule not matching

1. Check regex escaping in JSON (double backslashes)
2. Verify the pattern with an online regex tester
3. Try with `IGNORECASE` flag if case might vary
4. Check for extra whitespace in the source SQL

### Unexpected replacements

1. Make patterns more specific to avoid false matches
2. Use word boundaries (`\\b`) to avoid partial matches
3. Increase priority of more specific rules

### Circular replacements

1. Ensure replacement doesn't match the pattern
2. Use more specific patterns
3. Consider rule ordering via priorities

## Support

For issues with custom rules:
1. Validate your config file first
2. Test with a simple inline example
3. Check the applied rules in the output (shown as warnings)
