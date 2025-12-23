"""
PL/SQL to Databricks SQL stored procedure converter.

This module handles the conversion of Oracle PL/SQL stored procedures,
functions, and packages to Databricks SQL stored procedures.

Supports:
- Stored procedures and functions
- Package specifications and bodies
- Anonymous blocks and triggers
- Exception handling (EXCEPTION WHEN ... THEN)
- Oracle built-in packages (DBMS_OUTPUT, DBMS_LOB, UTL_FILE, etc.)
- Databricks SQL scripting (DECLARE, SET, IF, WHILE, FOR, etc.)
- Cursor handling and REF CURSOR
- Collection types and records
"""

import re
from typing import Optional, List, Dict, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum

from .translator import OracleToDatabricksTranslator, strip_sql_comments
from .function_mappings import get_databricks_data_type


# =============================================================================
# ORACLE EXCEPTION MAPPINGS
# =============================================================================

# Oracle predefined exceptions mapped to Databricks equivalents or custom handling
ORACLE_EXCEPTIONS = {
    # Data exceptions
    'NO_DATA_FOUND': {
        'sqlstate': '02000',
        'description': 'SELECT INTO returns no rows',
        'databricks': 'WHEN SQLSTATE \'02000\' THEN  -- NO_DATA_FOUND'
    },
    'TOO_MANY_ROWS': {
        'sqlstate': '21000',
        'description': 'SELECT INTO returns more than one row',
        'databricks': 'WHEN SQLSTATE \'21000\' THEN  -- TOO_MANY_ROWS'
    },
    'DUP_VAL_ON_INDEX': {
        'sqlstate': '23505',
        'description': 'Duplicate value on unique index',
        'databricks': 'WHEN SQLSTATE \'23505\' THEN  -- DUP_VAL_ON_INDEX'
    },
    'VALUE_ERROR': {
        'sqlstate': '22000',
        'description': 'Arithmetic, conversion, truncation, or size constraint error',
        'databricks': 'WHEN SQLSTATE \'22000\' THEN  -- VALUE_ERROR'
    },
    'ZERO_DIVIDE': {
        'sqlstate': '22012',
        'description': 'Division by zero',
        'databricks': 'WHEN SQLSTATE \'22012\' THEN  -- ZERO_DIVIDE'
    },
    'INVALID_NUMBER': {
        'sqlstate': '22018',
        'description': 'Invalid number conversion',
        'databricks': 'WHEN SQLSTATE \'22018\' THEN  -- INVALID_NUMBER'
    },
    'INVALID_CURSOR': {
        'sqlstate': '24000',
        'description': 'Invalid cursor operation',
        'databricks': 'WHEN SQLSTATE \'24000\' THEN  -- INVALID_CURSOR'
    },
    'CURSOR_ALREADY_OPEN': {
        'sqlstate': '24000',
        'description': 'Cursor already open',
        'databricks': 'WHEN SQLSTATE \'24000\' THEN  -- CURSOR_ALREADY_OPEN'
    },
    'LOGIN_DENIED': {
        'sqlstate': '28000',
        'description': 'Invalid username/password',
        'databricks': 'WHEN SQLSTATE \'28000\' THEN  -- LOGIN_DENIED'
    },
    'NOT_LOGGED_ON': {
        'sqlstate': '08003',
        'description': 'Not connected to database',
        'databricks': 'WHEN SQLSTATE \'08003\' THEN  -- NOT_LOGGED_ON'
    },
    'PROGRAM_ERROR': {
        'sqlstate': 'P0001',
        'description': 'PL/SQL internal error',
        'databricks': 'WHEN OTHER THEN  -- PROGRAM_ERROR (catch-all)'
    },
    'STORAGE_ERROR': {
        'sqlstate': '53100',
        'description': 'Out of memory',
        'databricks': 'WHEN SQLSTATE \'53100\' THEN  -- STORAGE_ERROR'
    },
    'TIMEOUT_ON_RESOURCE': {
        'sqlstate': '57014',
        'description': 'Timeout waiting for resource',
        'databricks': 'WHEN SQLSTATE \'57014\' THEN  -- TIMEOUT_ON_RESOURCE'
    },
    'CASE_NOT_FOUND': {
        'sqlstate': '20000',
        'description': 'No matching WHEN clause in CASE',
        'databricks': 'WHEN SQLSTATE \'20000\' THEN  -- CASE_NOT_FOUND'
    },
    'ROWTYPE_MISMATCH': {
        'sqlstate': '42804',
        'description': 'Host cursor variable and PL/SQL cursor variable have incompatible return types',
        'databricks': 'WHEN SQLSTATE \'42804\' THEN  -- ROWTYPE_MISMATCH'
    },
    'ACCESS_INTO_NULL': {
        'sqlstate': '22004',
        'description': 'Object or LOB not initialized',
        'databricks': 'WHEN SQLSTATE \'22004\' THEN  -- ACCESS_INTO_NULL'
    },
    'COLLECTION_IS_NULL': {
        'sqlstate': '22004',
        'description': 'Collection not initialized',
        'databricks': 'WHEN SQLSTATE \'22004\' THEN  -- COLLECTION_IS_NULL'
    },
    'SUBSCRIPT_BEYOND_COUNT': {
        'sqlstate': '22003',
        'description': 'Collection subscript beyond count',
        'databricks': 'WHEN SQLSTATE \'22003\' THEN  -- SUBSCRIPT_BEYOND_COUNT'
    },
    'SUBSCRIPT_OUTSIDE_LIMIT': {
        'sqlstate': '22003',
        'description': 'Collection subscript outside limit',
        'databricks': 'WHEN SQLSTATE \'22003\' THEN  -- SUBSCRIPT_OUTSIDE_LIMIT'
    },
    'SELF_IS_NULL': {
        'sqlstate': '22004',
        'description': 'Member method invoked on NULL instance',
        'databricks': 'WHEN SQLSTATE \'22004\' THEN  -- SELF_IS_NULL'
    },
    'OTHERS': {
        'sqlstate': None,
        'description': 'Catch-all exception handler',
        'databricks': 'WHEN OTHER THEN'
    }
}


# =============================================================================
# ORACLE BUILT-IN PACKAGE MAPPINGS
# =============================================================================

# Mappings for Oracle built-in packages to Databricks equivalents
ORACLE_PACKAGE_MAPPINGS = {
    # DBMS_OUTPUT - Debug/trace output
    'DBMS_OUTPUT': {
        'PUT_LINE': {
            'databricks': 'SELECT',
            'pattern': r"DBMS_OUTPUT\.PUT_LINE\s*\(\s*(.+?)\s*\)",
            'replacement': r"SELECT \1 AS debug_output",
            'note': 'Use SELECT for debugging output in Databricks'
        },
        'PUT': {
            'databricks': 'SELECT',
            'pattern': r"DBMS_OUTPUT\.PUT\s*\(\s*(.+?)\s*\)",
            'replacement': r"-- DBMS_OUTPUT.PUT: \1",
            'note': 'PUT without newline - concatenate values'
        },
        'NEW_LINE': {
            'databricks': None,
            'pattern': r"DBMS_OUTPUT\.NEW_LINE\s*(?:\(\s*\))?\s*;",
            'replacement': r"-- DBMS_OUTPUT.NEW_LINE removed",
            'note': 'No equivalent needed in Databricks'
        },
        'ENABLE': {
            'databricks': None,
            'pattern': r"DBMS_OUTPUT\.ENABLE\s*(?:\([^)]*\))?\s*;",
            'replacement': r"-- DBMS_OUTPUT.ENABLE removed (not needed in Databricks)",
            'note': 'Not needed in Databricks'
        },
        'DISABLE': {
            'databricks': None,
            'pattern': r"DBMS_OUTPUT\.DISABLE\s*(?:\([^)]*\))?\s*;",
            'replacement': r"-- DBMS_OUTPUT.DISABLE removed (not needed in Databricks)",
            'note': 'Not needed in Databricks'
        },
        'GET_LINE': {
            'databricks': None,
            'pattern': r"DBMS_OUTPUT\.GET_LINE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_OUTPUT.GET_LINE needs manual conversion",
            'note': 'Requires alternative implementation'
        },
        'GET_LINES': {
            'databricks': None,
            'pattern': r"DBMS_OUTPUT\.GET_LINES\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_OUTPUT.GET_LINES needs manual conversion",
            'note': 'Requires alternative implementation'
        }
    },
    
    # DBMS_LOB - Large Object manipulation
    'DBMS_LOB': {
        'GETLENGTH': {
            'databricks': 'LENGTH',
            'pattern': r"DBMS_LOB\.GETLENGTH\s*\(\s*(.+?)\s*\)",
            'replacement': r"LENGTH(\1)",
            'note': 'Use LENGTH() for string/binary length'
        },
        'SUBSTR': {
            'databricks': 'SUBSTRING',
            'pattern': r"DBMS_LOB\.SUBSTR\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"SUBSTRING(\1, \3, \2)",
            'note': 'Note: Parameter order differs (lob, amount, offset) -> (str, start, length)'
        },
        'INSTR': {
            'databricks': 'INSTR',
            'pattern': r"DBMS_LOB\.INSTR\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"INSTR(\1, \2)",
            'note': 'Use INSTR() for substring search'
        },
        'APPEND': {
            'databricks': 'CONCAT',
            'pattern': r"DBMS_LOB\.APPEND\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"SET \1 = CONCAT(\1, \2)",
            'note': 'Use CONCAT() for appending'
        },
        'WRITE': {
            'databricks': None,
            'pattern': r"DBMS_LOB\.WRITE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_LOB.WRITE needs manual conversion",
            'note': 'Use string operations for LOB manipulation'
        },
        'WRITEAPPEND': {
            'databricks': 'CONCAT',
            'pattern': r"DBMS_LOB\.WRITEAPPEND\s*\(\s*(\w+)\s*,\s*\d+\s*,\s*(.+?)\s*\)",
            'replacement': r"SET \1 = CONCAT(\1, \2)",
            'note': 'Use CONCAT() for appending'
        },
        'COMPARE': {
            'databricks': '=',
            'pattern': r"DBMS_LOB\.COMPARE\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"CASE WHEN \1 = \2 THEN 0 ELSE 1 END",
            'note': 'Use comparison operators'
        },
        'CREATETEMPORARY': {
            'databricks': None,
            'pattern': r"DBMS_LOB\.CREATETEMPORARY\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_LOB.CREATETEMPORARY: Use regular variable declaration",
            'note': 'Not needed - use regular string variables'
        },
        'FREETEMPORARY': {
            'databricks': None,
            'pattern': r"DBMS_LOB\.FREETEMPORARY\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_LOB.FREETEMPORARY removed (automatic in Databricks)",
            'note': 'Memory is automatically managed'
        },
        'TRIM': {
            'databricks': 'SUBSTRING',
            'pattern': r"DBMS_LOB\.TRIM\s*\(\s*(\w+)\s*,\s*(\d+)\s*\)",
            'replacement': r"SET \1 = SUBSTRING(\1, 1, \2)",
            'note': 'Use SUBSTRING to trim LOB'
        },
        'OPEN': {
            'databricks': None,
            'pattern': r"DBMS_LOB\.OPEN\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_LOB.OPEN removed (not needed in Databricks)",
            'note': 'Not needed in Databricks'
        },
        'CLOSE': {
            'databricks': None,
            'pattern': r"DBMS_LOB\.CLOSE\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_LOB.CLOSE removed (not needed in Databricks)",
            'note': 'Not needed in Databricks'
        },
        'ISOPEN': {
            'databricks': 'TRUE',
            'pattern': r"DBMS_LOB\.ISOPEN\s*\([^)]+\)",
            'replacement': r"TRUE",
            'note': 'LOBs are always accessible'
        },
        'COPY': {
            'databricks': 'SUBSTRING',
            'pattern': r"DBMS_LOB\.COPY\s*\(\s*(\w+)\s*,\s*(\w+)\s*,\s*(\d+)\s*(?:,\s*(\d+)\s*)?(?:,\s*(\d+)\s*)?\)",
            'replacement': r"SET \1 = SUBSTRING(\2, COALESCE(\5, 1), \3)",
            'note': 'Use SUBSTRING/CONCAT for copying'
        }
    },
    
    # DBMS_SQL - Dynamic SQL
    'DBMS_SQL': {
        'OPEN_CURSOR': {
            'databricks': None,
            'pattern': r"DBMS_SQL\.OPEN_CURSOR",
            'replacement': r"-- TODO: DBMS_SQL.OPEN_CURSOR - Use EXECUTE IMMEDIATE instead",
            'note': 'Use EXECUTE IMMEDIATE for dynamic SQL'
        },
        'PARSE': {
            'databricks': None,
            'pattern': r"DBMS_SQL\.PARSE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SQL.PARSE - Convert to EXECUTE IMMEDIATE",
            'note': 'Use EXECUTE IMMEDIATE for dynamic SQL'
        },
        'EXECUTE': {
            'databricks': 'EXECUTE IMMEDIATE',
            'pattern': r"DBMS_SQL\.EXECUTE\s*\([^)]+\)",
            'replacement': r"-- TODO: DBMS_SQL.EXECUTE - Use EXECUTE IMMEDIATE",
            'note': 'Use EXECUTE IMMEDIATE for dynamic SQL'
        },
        'CLOSE_CURSOR': {
            'databricks': None,
            'pattern': r"DBMS_SQL\.CLOSE_CURSOR\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_SQL.CLOSE_CURSOR removed (not needed with EXECUTE IMMEDIATE)",
            'note': 'Not needed with EXECUTE IMMEDIATE'
        },
        'BIND_VARIABLE': {
            'databricks': None,
            'pattern': r"DBMS_SQL\.BIND_VARIABLE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SQL.BIND_VARIABLE - Use parameterized EXECUTE IMMEDIATE",
            'note': 'Use USING clause with EXECUTE IMMEDIATE'
        },
        'DEFINE_COLUMN': {
            'databricks': None,
            'pattern': r"DBMS_SQL\.DEFINE_COLUMN\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SQL.DEFINE_COLUMN - Use INTO clause with EXECUTE IMMEDIATE",
            'note': 'Use INTO clause with EXECUTE IMMEDIATE'
        },
        'COLUMN_VALUE': {
            'databricks': None,
            'pattern': r"DBMS_SQL\.COLUMN_VALUE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SQL.COLUMN_VALUE - Use INTO clause with EXECUTE IMMEDIATE",
            'note': 'Use INTO clause with EXECUTE IMMEDIATE'
        },
        'FETCH_ROWS': {
            'databricks': None,
            'pattern': r"DBMS_SQL\.FETCH_ROWS\s*\([^)]+\)",
            'replacement': r"-- TODO: DBMS_SQL.FETCH_ROWS - Use cursor FOR loop instead",
            'note': 'Use cursor FOR loop or table function'
        }
    },
    
    # DBMS_UTILITY - Miscellaneous utilities
    'DBMS_UTILITY': {
        'GET_TIME': {
            'databricks': 'UNIX_MILLIS',
            'pattern': r"DBMS_UTILITY\.GET_TIME",
            'replacement': r"UNIX_MILLIS(CURRENT_TIMESTAMP())",
            'note': 'Use UNIX_MILLIS for timing'
        },
        'GET_CPU_TIME': {
            'databricks': 'UNIX_MILLIS',
            'pattern': r"DBMS_UTILITY\.GET_CPU_TIME",
            'replacement': r"UNIX_MILLIS(CURRENT_TIMESTAMP())  -- Note: CPU time not available",
            'note': 'CPU time not directly available in Databricks'
        },
        'FORMAT_ERROR_STACK': {
            'databricks': 'SQLERRM',
            'pattern': r"DBMS_UTILITY\.FORMAT_ERROR_STACK",
            'replacement': r"SQLERRM  -- Error message",
            'note': 'Use SQLERRM for error message'
        },
        'FORMAT_ERROR_BACKTRACE': {
            'databricks': 'SQLERRM',
            'pattern': r"DBMS_UTILITY\.FORMAT_ERROR_BACKTRACE",
            'replacement': r"SQLERRM  -- Backtrace not available",
            'note': 'Stack trace not available'
        },
        'FORMAT_CALL_STACK': {
            'databricks': None,
            'pattern': r"DBMS_UTILITY\.FORMAT_CALL_STACK",
            'replacement': r"'Call stack not available'",
            'note': 'Call stack not available in Databricks'
        },
        'COMMA_TO_TABLE': {
            'databricks': 'SPLIT',
            'pattern': r"DBMS_UTILITY\.COMMA_TO_TABLE\s*\(\s*(.+?)\s*,",
            'replacement': r"SPLIT(\1, ',')",
            'note': 'Use SPLIT() for comma-separated values'
        },
        'TABLE_TO_COMMA': {
            'databricks': 'ARRAY_JOIN',
            'pattern': r"DBMS_UTILITY\.TABLE_TO_COMMA\s*\(\s*(.+?)\s*,",
            'replacement': r"ARRAY_JOIN(\1, ',')",
            'note': 'Use ARRAY_JOIN() to create comma-separated string'
        },
        'DB_VERSION': {
            'databricks': 'VERSION',
            'pattern': r"DBMS_UTILITY\.DB_VERSION\s*\([^)]+\)\s*;",
            'replacement': r"SELECT VERSION() INTO version;  -- Databricks version",
            'note': 'Use VERSION() function'
        },
        'CURRENT_INSTANCE': {
            'databricks': None,
            'pattern': r"DBMS_UTILITY\.CURRENT_INSTANCE",
            'replacement': r"1  -- Single instance in Databricks",
            'note': 'Databricks does not have instances like Oracle RAC'
        },
        'NAME_RESOLVE': {
            'databricks': None,
            'pattern': r"DBMS_UTILITY\.NAME_RESOLVE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_UTILITY.NAME_RESOLVE - Use catalog.schema.table naming",
            'note': 'Use explicit three-part naming'
        }
    },
    
    # DBMS_SCHEDULER - Job scheduling
    'DBMS_SCHEDULER': {
        'CREATE_JOB': {
            'databricks': None,
            'pattern': r"DBMS_SCHEDULER\.CREATE_JOB\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SCHEDULER.CREATE_JOB - Use Databricks Workflows or Jobs API",
            'note': 'Use Databricks Workflows for scheduling'
        },
        'DROP_JOB': {
            'databricks': None,
            'pattern': r"DBMS_SCHEDULER\.DROP_JOB\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SCHEDULER.DROP_JOB - Use Databricks Jobs API",
            'note': 'Use Databricks Jobs API'
        },
        'ENABLE': {
            'databricks': None,
            'pattern': r"DBMS_SCHEDULER\.ENABLE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SCHEDULER.ENABLE - Use Databricks Jobs API",
            'note': 'Use Databricks Jobs API'
        },
        'DISABLE': {
            'databricks': None,
            'pattern': r"DBMS_SCHEDULER\.DISABLE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SCHEDULER.DISABLE - Use Databricks Jobs API",
            'note': 'Use Databricks Jobs API'
        },
        'RUN_JOB': {
            'databricks': None,
            'pattern': r"DBMS_SCHEDULER\.RUN_JOB\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SCHEDULER.RUN_JOB - Use Databricks Jobs API run-now",
            'note': 'Use Databricks Jobs API run-now'
        },
        'STOP_JOB': {
            'databricks': None,
            'pattern': r"DBMS_SCHEDULER\.STOP_JOB\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: DBMS_SCHEDULER.STOP_JOB - Use Databricks Jobs API",
            'note': 'Use Databricks Jobs API'
        }
    },
    
    # DBMS_LOCK - Locking mechanisms
    'DBMS_LOCK': {
        'SLEEP': {
            'databricks': None,
            'pattern': r"DBMS_LOCK\.SLEEP\s*\(\s*(\d+(?:\.\d+)?)\s*\)\s*;",
            'replacement': r"-- Note: DBMS_LOCK.SLEEP(\1) - No direct equivalent, consider restructuring",
            'note': 'Sleep not available in Databricks SQL'
        },
        'REQUEST': {
            'databricks': None,
            'pattern': r"DBMS_LOCK\.REQUEST\s*\([^)]+\)",
            'replacement': r"-- TODO: DBMS_LOCK.REQUEST - Consider Delta Lake transactions or table locks",
            'note': 'Use Delta Lake transaction isolation'
        },
        'RELEASE': {
            'databricks': None,
            'pattern': r"DBMS_LOCK\.RELEASE\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_LOCK.RELEASE - Locks auto-released in Databricks",
            'note': 'Locks are automatically released'
        },
        'CONVERT': {
            'databricks': None,
            'pattern': r"DBMS_LOCK\.CONVERT\s*\([^)]+\)",
            'replacement': r"-- TODO: DBMS_LOCK.CONVERT - Not supported in Databricks",
            'note': 'Not supported in Databricks'
        }
    },
    
    # DBMS_RANDOM - Random number generation
    'DBMS_RANDOM': {
        'VALUE': {
            'databricks': 'RAND',
            'pattern': r"DBMS_RANDOM\.VALUE(?:\s*\(\s*\))?",
            'replacement': r"RAND()",
            'note': 'Use RAND() for random numbers between 0 and 1'
        },
        'VALUE_RANGE': {
            'databricks': 'RAND',
            'pattern': r"DBMS_RANDOM\.VALUE\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"(\1 + RAND() * (\2 - \1))",
            'note': 'Use RAND() with range calculation'
        },
        'STRING': {
            'databricks': None,
            'pattern': r"DBMS_RANDOM\.STRING\s*\(\s*'([ULAXPulaxp])'\s*,\s*(\d+)\s*\)",
            'replacement': r"-- TODO: DBMS_RANDOM.STRING - Use custom UDF or array/transform",
            'note': 'Implement custom random string function'
        },
        'NORMAL': {
            'databricks': 'RANDN',
            'pattern': r"DBMS_RANDOM\.NORMAL",
            'replacement': r"RANDN()",
            'note': 'Use RANDN() for normal distribution'
        },
        'SEED': {
            'databricks': None,
            'pattern': r"DBMS_RANDOM\.SEED\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_RANDOM.SEED - Note: RAND() seed set globally, consider alternative",
            'note': 'Use deterministic approach if reproducibility needed'
        },
        'INITIALIZE': {
            'databricks': None,
            'pattern': r"DBMS_RANDOM\.INITIALIZE\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_RANDOM.INITIALIZE removed (not needed in Databricks)",
            'note': 'Not needed in Databricks'
        },
        'TERMINATE': {
            'databricks': None,
            'pattern': r"DBMS_RANDOM\.TERMINATE\s*(?:\([^)]*\))?\s*;",
            'replacement': r"-- DBMS_RANDOM.TERMINATE removed (not needed in Databricks)",
            'note': 'Not needed in Databricks'
        }
    },
    
    # DBMS_CRYPTO - Cryptographic functions
    'DBMS_CRYPTO': {
        'HASH': {
            'databricks': 'SHA2/MD5',
            'pattern': r"DBMS_CRYPTO\.HASH\s*\(\s*(.+?)\s*,\s*DBMS_CRYPTO\.HASH_(\w+)\s*\)",
            'replacement': r"-- Use SHA2(\1, 256) or MD5(\1) based on algorithm",
            'note': 'Use SHA2() or MD5() functions'
        },
        'ENCRYPT': {
            'databricks': 'AES_ENCRYPT',
            'pattern': r"DBMS_CRYPTO\.ENCRYPT\s*\([^)]+\)",
            'replacement': r"-- TODO: Use AES_ENCRYPT(data, key) for encryption",
            'note': 'Use AES_ENCRYPT() function'
        },
        'DECRYPT': {
            'databricks': 'AES_DECRYPT',
            'pattern': r"DBMS_CRYPTO\.DECRYPT\s*\([^)]+\)",
            'replacement': r"-- TODO: Use AES_DECRYPT(data, key) for decryption",
            'note': 'Use AES_DECRYPT() function'
        },
        'MAC': {
            'databricks': 'SHA2',
            'pattern': r"DBMS_CRYPTO\.MAC\s*\([^)]+\)",
            'replacement': r"-- TODO: DBMS_CRYPTO.MAC - Use HMAC or SHA2() with key",
            'note': 'Use SHA2() with concatenated key for simple MAC'
        },
        'RANDOMBYTES': {
            'databricks': None,
            'pattern': r"DBMS_CRYPTO\.RANDOMBYTES\s*\(\s*(\d+)\s*\)",
            'replacement': r"-- TODO: DBMS_CRYPTO.RANDOMBYTES - Use UUID() or custom implementation",
            'note': 'Use UUID() or implement custom random bytes'
        }
    },
    
    # UTL_FILE - File I/O operations
    'UTL_FILE': {
        'FOPEN': {
            'databricks': None,
            'pattern': r"UTL_FILE\.FOPEN\s*\([^)]+\)",
            'replacement': r"-- TODO: UTL_FILE.FOPEN - Use Databricks COPY INTO or spark.read",
            'note': 'Use cloud storage APIs or Databricks file utilities'
        },
        'FCLOSE': {
            'databricks': None,
            'pattern': r"UTL_FILE\.FCLOSE\s*\([^)]+\)\s*;",
            'replacement': r"-- UTL_FILE.FCLOSE removed (handled automatically)",
            'note': 'File handles managed automatically'
        },
        'FCLOSE_ALL': {
            'databricks': None,
            'pattern': r"UTL_FILE\.FCLOSE_ALL\s*(?:\([^)]*\))?\s*;",
            'replacement': r"-- UTL_FILE.FCLOSE_ALL removed (handled automatically)",
            'note': 'File handles managed automatically'
        },
        'PUT_LINE': {
            'databricks': None,
            'pattern': r"UTL_FILE\.PUT_LINE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: UTL_FILE.PUT_LINE - Use INSERT INTO delta table or dbutils.fs",
            'note': 'Write to Delta table or use cloud storage'
        },
        'PUT': {
            'databricks': None,
            'pattern': r"UTL_FILE\.PUT\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: UTL_FILE.PUT - Accumulate data for batch write",
            'note': 'Accumulate data for batch write to storage'
        },
        'GET_LINE': {
            'databricks': None,
            'pattern': r"UTL_FILE\.GET_LINE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: UTL_FILE.GET_LINE - Use SELECT from file-based table",
            'note': 'Query file as table using Databricks'
        },
        'NEW_LINE': {
            'databricks': None,
            'pattern': r"UTL_FILE\.NEW_LINE\s*\([^)]+\)\s*;",
            'replacement': r"-- UTL_FILE.NEW_LINE handled by PUT_LINE equivalent",
            'note': 'Line breaks handled automatically'
        },
        'FFLUSH': {
            'databricks': None,
            'pattern': r"UTL_FILE\.FFLUSH\s*\([^)]+\)\s*;",
            'replacement': r"-- UTL_FILE.FFLUSH removed (not needed in Databricks)",
            'note': 'Writes are automatically flushed'
        },
        'IS_OPEN': {
            'databricks': 'TRUE',
            'pattern': r"UTL_FILE\.IS_OPEN\s*\([^)]+\)",
            'replacement': r"TRUE  -- File operations are transactional",
            'note': 'File operations are atomic in cloud storage'
        },
        'FCOPY': {
            'databricks': None,
            'pattern': r"UTL_FILE\.FCOPY\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: UTL_FILE.FCOPY - Use dbutils.fs.cp() or cloud storage API",
            'note': 'Use dbutils.fs.cp() for file copy'
        },
        'FRENAME': {
            'databricks': None,
            'pattern': r"UTL_FILE\.FRENAME\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: UTL_FILE.FRENAME - Use dbutils.fs.mv() or cloud storage API",
            'note': 'Use dbutils.fs.mv() for file rename'
        },
        'FREMOVE': {
            'databricks': None,
            'pattern': r"UTL_FILE\.FREMOVE\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: UTL_FILE.FREMOVE - Use dbutils.fs.rm() or cloud storage API",
            'note': 'Use dbutils.fs.rm() for file deletion'
        },
        'FGETATTR': {
            'databricks': None,
            'pattern': r"UTL_FILE\.FGETATTR\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: UTL_FILE.FGETATTR - Use dbutils.fs.ls() for file attributes",
            'note': 'Use dbutils.fs.ls() for file attributes'
        }
    },
    
    # UTL_HTTP - HTTP calls
    'UTL_HTTP': {
        'REQUEST': {
            'databricks': None,
            'pattern': r"UTL_HTTP\.REQUEST\s*\([^)]+\)",
            'replacement': r"-- TODO: UTL_HTTP.REQUEST - Use Python requests library in notebook/UDF",
            'note': 'Use Python requests or Databricks REST API utilities'
        },
        'REQUEST_PIECES': {
            'databricks': None,
            'pattern': r"UTL_HTTP\.REQUEST_PIECES\s*\([^)]+\)",
            'replacement': r"-- TODO: UTL_HTTP.REQUEST_PIECES - Use Python requests library",
            'note': 'Use Python requests library'
        },
        'BEGIN_REQUEST': {
            'databricks': None,
            'pattern': r"UTL_HTTP\.BEGIN_REQUEST\s*\([^)]+\)",
            'replacement': r"-- TODO: UTL_HTTP - Use Python HTTP library or Databricks SQL connector",
            'note': 'Use Python for complex HTTP operations'
        },
        'SET_HEADER': {
            'databricks': None,
            'pattern': r"UTL_HTTP\.SET_HEADER\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: Set HTTP header using Python requests",
            'note': 'Use Python requests library for headers'
        },
        'GET_RESPONSE': {
            'databricks': None,
            'pattern': r"UTL_HTTP\.GET_RESPONSE\s*\([^)]+\)",
            'replacement': r"-- TODO: Get HTTP response using Python requests",
            'note': 'Use Python requests library'
        },
        'READ_TEXT': {
            'databricks': None,
            'pattern': r"UTL_HTTP\.READ_TEXT\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: Read HTTP response text using Python",
            'note': 'Use Python requests library'
        },
        'END_RESPONSE': {
            'databricks': None,
            'pattern': r"UTL_HTTP\.END_RESPONSE\s*\([^)]+\)\s*;",
            'replacement': r"-- UTL_HTTP.END_RESPONSE removed (automatic in Python)",
            'note': 'Response handling is automatic'
        }
    },
    
    # UTL_MAIL - Email
    'UTL_MAIL': {
        'SEND': {
            'databricks': None,
            'pattern': r"UTL_MAIL\.SEND\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: UTL_MAIL.SEND - Use Databricks notification service or Python smtplib",
            'note': 'Use Databricks alerts or Python email libraries'
        },
        'SEND_ATTACH_RAW': {
            'databricks': None,
            'pattern': r"UTL_MAIL\.SEND_ATTACH_RAW\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: UTL_MAIL.SEND_ATTACH_RAW - Use Python email with attachments",
            'note': 'Use Python email libraries with attachments'
        },
        'SEND_ATTACH_VARCHAR2': {
            'databricks': None,
            'pattern': r"UTL_MAIL\.SEND_ATTACH_VARCHAR2\s*\([^)]+\)\s*;",
            'replacement': r"-- TODO: Use Python email library for text attachments",
            'note': 'Use Python email libraries'
        }
    },
    
    # UTL_RAW - Raw data manipulation
    'UTL_RAW': {
        'CAST_TO_VARCHAR2': {
            'databricks': 'CAST',
            'pattern': r"UTL_RAW\.CAST_TO_VARCHAR2\s*\(\s*(.+?)\s*\)",
            'replacement': r"CAST(\1 AS STRING)",
            'note': 'Use CAST to STRING'
        },
        'CAST_TO_RAW': {
            'databricks': 'CAST',
            'pattern': r"UTL_RAW\.CAST_TO_RAW\s*\(\s*(.+?)\s*\)",
            'replacement': r"CAST(\1 AS BINARY)",
            'note': 'Use CAST to BINARY'
        },
        'CONCAT': {
            'databricks': 'CONCAT',
            'pattern': r"UTL_RAW\.CONCAT\s*\(",
            'replacement': r"CONCAT(",
            'note': 'Use CONCAT() for binary concatenation'
        },
        'LENGTH': {
            'databricks': 'LENGTH',
            'pattern': r"UTL_RAW\.LENGTH\s*\(\s*(.+?)\s*\)",
            'replacement': r"LENGTH(\1)",
            'note': 'Use LENGTH() function'
        },
        'SUBSTR': {
            'databricks': 'SUBSTRING',
            'pattern': r"UTL_RAW\.SUBSTR\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"SUBSTRING(\1, \2, \3)",
            'note': 'Use SUBSTRING() function'
        },
        'BIT_AND': {
            'databricks': '&',
            'pattern': r"UTL_RAW\.BIT_AND\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"(\1 & \2)",
            'note': 'Use & operator for bitwise AND'
        },
        'BIT_OR': {
            'databricks': '|',
            'pattern': r"UTL_RAW\.BIT_OR\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"(\1 | \2)",
            'note': 'Use | operator for bitwise OR'
        },
        'BIT_XOR': {
            'databricks': '^',
            'pattern': r"UTL_RAW\.BIT_XOR\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"(\1 ^ \2)",
            'note': 'Use ^ operator for bitwise XOR'
        },
        'COMPARE': {
            'databricks': '=',
            'pattern': r"UTL_RAW\.COMPARE\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)",
            'replacement': r"CASE WHEN \1 = \2 THEN 0 ELSE 1 END",
            'note': 'Use comparison operator'
        }
    },
    
    # DBMS_SESSION - Session management
    'DBMS_SESSION': {
        'SET_CONTEXT': {
            'databricks': 'SET',
            'pattern': r"DBMS_SESSION\.SET_CONTEXT\s*\(\s*'(\w+)'\s*,\s*'(\w+)'\s*,\s*(.+?)\s*\)\s*;",
            'replacement': r"SET \1.\2 = \3;  -- Session variable",
            'note': 'Use SET for session variables'
        },
        'CLEAR_CONTEXT': {
            'databricks': 'SET',
            'pattern': r"DBMS_SESSION\.CLEAR_CONTEXT\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_SESSION.CLEAR_CONTEXT - Session context cleared automatically",
            'note': 'Session contexts are session-scoped'
        },
        'SET_IDENTIFIER': {
            'databricks': None,
            'pattern': r"DBMS_SESSION\.SET_IDENTIFIER\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_SESSION.SET_IDENTIFIER - Use SET spark.sql.session.* for session config",
            'note': 'Use Spark session configuration'
        },
        'CLEAR_IDENTIFIER': {
            'databricks': None,
            'pattern': r"DBMS_SESSION\.CLEAR_IDENTIFIER\s*(?:\([^)]*\))?\s*;",
            'replacement': r"-- DBMS_SESSION.CLEAR_IDENTIFIER removed",
            'note': 'Not needed in Databricks'
        },
        'UNIQUE_SESSION_ID': {
            'databricks': 'UUID',
            'pattern': r"DBMS_SESSION\.UNIQUE_SESSION_ID",
            'replacement': r"UUID()",
            'note': 'Use UUID() for unique identifier'
        }
    },
    
    # DBMS_METADATA - Metadata extraction (for DDL)
    'DBMS_METADATA': {
        'GET_DDL': {
            'databricks': 'SHOW CREATE TABLE',
            'pattern': r"DBMS_METADATA\.GET_DDL\s*\(\s*'TABLE'\s*,\s*'(\w+)'\s*(?:,\s*'(\w+)')?\s*\)",
            'replacement': r"SHOW CREATE TABLE \2.\1",
            'note': 'Use SHOW CREATE TABLE for DDL'
        },
        'GET_DEPENDENT_DDL': {
            'databricks': None,
            'pattern': r"DBMS_METADATA\.GET_DEPENDENT_DDL\s*\([^)]+\)",
            'replacement': r"-- TODO: DBMS_METADATA.GET_DEPENDENT_DDL - Query information_schema",
            'note': 'Query information_schema for dependent objects'
        },
        'SET_TRANSFORM_PARAM': {
            'databricks': None,
            'pattern': r"DBMS_METADATA\.SET_TRANSFORM_PARAM\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_METADATA.SET_TRANSFORM_PARAM removed (not applicable)",
            'note': 'Not applicable in Databricks'
        }
    },
    
    # DBMS_STATS - Statistics management
    'DBMS_STATS': {
        'GATHER_TABLE_STATS': {
            'databricks': 'ANALYZE TABLE',
            'pattern': r"DBMS_STATS\.GATHER_TABLE_STATS\s*\(\s*'(\w+)'\s*,\s*'(\w+)'[^)]*\)\s*;",
            'replacement': r"ANALYZE TABLE \1.\2 COMPUTE STATISTICS;",
            'note': 'Use ANALYZE TABLE for statistics'
        },
        'GATHER_SCHEMA_STATS': {
            'databricks': None,
            'pattern': r"DBMS_STATS\.GATHER_SCHEMA_STATS\s*\(\s*'(\w+)'[^)]*\)\s*;",
            'replacement': r"-- TODO: ANALYZE TABLE for each table in schema \1",
            'note': 'Run ANALYZE TABLE for each table in schema'
        },
        'GATHER_INDEX_STATS': {
            'databricks': None,
            'pattern': r"DBMS_STATS\.GATHER_INDEX_STATS\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_STATS.GATHER_INDEX_STATS - Not needed (Delta Lake auto-optimizes)",
            'note': 'Delta Lake handles optimization automatically'
        },
        'DELETE_TABLE_STATS': {
            'databricks': None,
            'pattern': r"DBMS_STATS\.DELETE_TABLE_STATS\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_STATS.DELETE_TABLE_STATS - Not typically needed in Databricks",
            'note': 'Statistics are automatically managed'
        }
    },
    
    # DBMS_XMLGEN - XML generation
    'DBMS_XMLGEN': {
        'GETXML': {
            'databricks': 'TO_XML',
            'pattern': r"DBMS_XMLGEN\.GETXML\s*\(\s*(.+?)\s*\)",
            'replacement': r"-- TODO: Use TO_XML() or schema_of_xml() for XML operations",
            'note': 'Use Spark XML functions'
        },
        'NEWCONTEXT': {
            'databricks': None,
            'pattern': r"DBMS_XMLGEN\.NEWCONTEXT\s*\([^)]+\)",
            'replacement': r"-- TODO: DBMS_XMLGEN - Use Spark XML functions directly",
            'note': 'Use Spark XML functions'
        },
        'SETROWSETTAG': {
            'databricks': None,
            'pattern': r"DBMS_XMLGEN\.SETROWSETTAG\s*\([^)]+\)\s*;",
            'replacement': r"-- XML row tag configured in Spark XML options",
            'note': 'Configure in Spark XML options'
        },
        'SETROWTAG': {
            'databricks': None,
            'pattern': r"DBMS_XMLGEN\.SETROWTAG\s*\([^)]+\)\s*;",
            'replacement': r"-- XML row tag configured in Spark XML options",
            'note': 'Configure in Spark XML options'
        },
        'CLOSECONTEXT': {
            'databricks': None,
            'pattern': r"DBMS_XMLGEN\.CLOSECONTEXT\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_XMLGEN.CLOSECONTEXT removed (automatic in Databricks)",
            'note': 'Context managed automatically'
        }
    },
    
    # DBMS_APPLICATION_INFO - Application info for monitoring
    'DBMS_APPLICATION_INFO': {
        'SET_MODULE': {
            'databricks': 'SET',
            'pattern': r"DBMS_APPLICATION_INFO\.SET_MODULE\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)\s*;",
            'replacement': r"-- SET module/action: \1, \2 (for logging/monitoring)",
            'note': 'Use comments or logging for monitoring'
        },
        'SET_ACTION': {
            'databricks': None,
            'pattern': r"DBMS_APPLICATION_INFO\.SET_ACTION\s*\(\s*(.+?)\s*\)\s*;",
            'replacement': r"-- SET action: \1 (for logging/monitoring)",
            'note': 'Use comments or logging'
        },
        'SET_CLIENT_INFO': {
            'databricks': None,
            'pattern': r"DBMS_APPLICATION_INFO\.SET_CLIENT_INFO\s*\(\s*(.+?)\s*\)\s*;",
            'replacement': r"-- SET client_info: \1 (for logging/monitoring)",
            'note': 'Use comments or logging'
        },
        'READ_MODULE': {
            'databricks': None,
            'pattern': r"DBMS_APPLICATION_INFO\.READ_MODULE\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_APPLICATION_INFO.READ_MODULE - Check query history instead",
            'note': 'Use Databricks query history'
        },
        'READ_CLIENT_INFO': {
            'databricks': None,
            'pattern': r"DBMS_APPLICATION_INFO\.READ_CLIENT_INFO\s*\([^)]+\)\s*;",
            'replacement': r"-- DBMS_APPLICATION_INFO.READ_CLIENT_INFO - Check query history instead",
            'note': 'Use Databricks query history'
        }
    },
    
    # APEX_JSON - JSON processing (Oracle APEX)
    'APEX_JSON': {
        'PARSE': {
            'databricks': 'FROM_JSON',
            'pattern': r"APEX_JSON\.PARSE\s*\(\s*(.+?)\s*\)\s*;",
            'replacement': r"-- Use FROM_JSON(\1, schema) to parse JSON",
            'note': 'Use FROM_JSON() with schema'
        },
        'GET_VARCHAR2': {
            'databricks': 'GET_JSON_OBJECT',
            'pattern': r"APEX_JSON\.GET_VARCHAR2\s*\(\s*p_path\s*=>\s*'(.+?)'\s*\)",
            'replacement': r"GET_JSON_OBJECT(json_col, '$.\1')",
            'note': 'Use GET_JSON_OBJECT() or json_col:path syntax'
        },
        'GET_NUMBER': {
            'databricks': 'GET_JSON_OBJECT',
            'pattern': r"APEX_JSON\.GET_NUMBER\s*\(\s*p_path\s*=>\s*'(.+?)'\s*\)",
            'replacement': r"CAST(GET_JSON_OBJECT(json_col, '$.\1') AS DOUBLE)",
            'note': 'Use GET_JSON_OBJECT() with CAST'
        },
        'WRITE': {
            'databricks': 'TO_JSON',
            'pattern': r"APEX_JSON\.WRITE\s*\(",
            'replacement': r"-- Use TO_JSON() for JSON output",
            'note': 'Use TO_JSON() for JSON generation'
        },
        'OPEN_OBJECT': {
            'databricks': 'STRUCT',
            'pattern': r"APEX_JSON\.OPEN_OBJECT\s*(?:\([^)]*\))?\s*;",
            'replacement': r"-- Build JSON using STRUCT and TO_JSON",
            'note': 'Use STRUCT and TO_JSON()'
        },
        'CLOSE_OBJECT': {
            'databricks': None,
            'pattern': r"APEX_JSON\.CLOSE_OBJECT\s*(?:\([^)]*\))?\s*;",
            'replacement': r"-- JSON object closed automatically",
            'note': 'Automatic in Databricks'
        },
        'OPEN_ARRAY': {
            'databricks': 'ARRAY',
            'pattern': r"APEX_JSON\.OPEN_ARRAY\s*(?:\([^)]*\))?\s*;",
            'replacement': r"-- Build array using ARRAY() and TO_JSON",
            'note': 'Use ARRAY() and TO_JSON()'
        },
        'CLOSE_ARRAY': {
            'databricks': None,
            'pattern': r"APEX_JSON\.CLOSE_ARRAY\s*(?:\([^)]*\))?\s*;",
            'replacement': r"-- JSON array closed automatically",
            'note': 'Automatic in Databricks'
        }
    }
}


# =============================================================================
# PL/SQL DATA TYPE MAPPINGS
# =============================================================================

# Extended PL/SQL data types to Databricks SQL types
PLSQL_TYPE_MAPPINGS = {
    # Numeric types
    'PLS_INTEGER': 'INT',
    'BINARY_INTEGER': 'INT',
    'SIMPLE_INTEGER': 'INT',
    'NATURAL': 'INT',  # Non-negative integer
    'NATURALN': 'INT',  # Non-negative, NOT NULL
    'POSITIVE': 'INT',  # Positive integer
    'POSITIVEN': 'INT',  # Positive, NOT NULL
    'SIGNTYPE': 'SMALLINT',  # -1, 0, or 1
    
    # Boolean
    'BOOLEAN': 'BOOLEAN',
    
    # String types
    'STRING': 'STRING',
    'LONG': 'STRING',
    'LONG RAW': 'BINARY',
    'RAW': 'BINARY',
    'ROWID': 'STRING',
    'UROWID': 'STRING',
    
    # LOB types
    'CLOB': 'STRING',
    'NCLOB': 'STRING',
    'BLOB': 'BINARY',
    'BFILE': 'STRING',  # File reference as path string
    
    # Record and collection placeholders
    'RECORD': 'STRUCT',
    'VARRAY': 'ARRAY',
    'TABLE': 'ARRAY',  # PL/SQL nested table
    'ASSOCIATIVE ARRAY': 'MAP',
    
    # REF CURSOR
    'SYS_REFCURSOR': 'CURSOR',  # Needs special handling
    'REF CURSOR': 'CURSOR',
    
    # XML types
    'XMLTYPE': 'STRING',
    'SYS.XMLTYPE': 'STRING',
    
    # JSON type (Oracle 21c+)
    'JSON': 'STRING',
    
    # Interval types
    'INTERVAL YEAR TO MONTH': 'STRING',  # Store as string, convert as needed
    'INTERVAL DAY TO SECOND': 'STRING',
    
    # Object types
    'OBJECT': 'STRUCT',
    'ANYDATA': 'STRING',  # Serialize as JSON
    'ANYTYPE': 'STRING',
    'ANYDATASET': 'ARRAY<STRING>',
}


# =============================================================================
# DATABRICKS SQL SCRIPTING CONSTRUCTS
# =============================================================================

# Databricks SQL supports these scripting constructs (as of 2024):
DATABRICKS_SQL_SCRIPTING = {
    # Variable declarations
    'DECLARE': 'DECLARE variable_name data_type [DEFAULT value];',
    'SET': 'SET variable_name = expression;',
    
    # Conditional logic
    'IF': '''
IF condition THEN
    statements;
[ELSEIF condition THEN
    statements;]
[ELSE
    statements;]
END IF;
''',
    'CASE': '''
CASE expression
    WHEN value THEN statements;
    [WHEN value THEN statements;]
    [ELSE statements;]
END CASE;
''',
    
    # Loops
    'WHILE': '''
[label:] WHILE condition DO
    statements;
END WHILE [label];
''',
    'REPEAT': '''
[label:] REPEAT
    statements;
UNTIL condition
END REPEAT [label];
''',
    'LOOP': '''
[label:] LOOP
    statements;
END LOOP [label];
''',
    'FOR': '''
[label:] FOR loop_variable IN [REVERSE] start..end DO
    statements;
END FOR [label];
''',
    'FOR_CURSOR': '''
FOR record_variable IN (SELECT ...) DO
    statements;
END FOR;
''',
    
    # Loop control
    'ITERATE': 'ITERATE label;  -- Continue to next iteration (like CONTINUE)',
    'LEAVE': 'LEAVE label;  -- Exit loop (like EXIT)',
    
    # Return
    'RETURN': 'RETURN [expression];',
    
    # Exception handling
    'EXCEPTION': '''
BEGIN
    statements;
EXCEPTION
    WHEN condition_name THEN
        handler_statements;
    WHEN SQLSTATE 'xxxxx' THEN
        handler_statements;
    WHEN OTHER THEN
        handler_statements;
END;
''',
    
    # Error signaling
    'SIGNAL': "SIGNAL SQLSTATE 'xxxxx' SET MESSAGE_TEXT = 'error message';",
    'RESIGNAL': "RESIGNAL [SQLSTATE 'xxxxx'] [SET MESSAGE_TEXT = 'message'];",
    
    # Compound statements
    'BEGIN_END': '''
BEGIN
    [DECLARE ...]
    statements;
    [EXCEPTION ...]
END;
''',
    
    # Cursor operations
    'DECLARE_CURSOR': 'DECLARE cursor_name CURSOR FOR SELECT ...;',
    'OPEN_CURSOR': 'OPEN cursor_name;',
    'FETCH_CURSOR': 'FETCH cursor_name INTO variable1, variable2, ...;',
    'CLOSE_CURSOR': 'CLOSE cursor_name;',
}


# =============================================================================
# PLSQL TO DATABRICKS CONTROL FLOW MAPPINGS
# =============================================================================

PLSQL_CONTROL_FLOW_MAPPINGS = {
    # Loop control
    'EXIT': {
        'pattern': r'\bEXIT\s*;',
        'replacement': 'LEAVE;',
        'note': 'Use LEAVE to exit loop'
    },
    'EXIT_WHEN': {
        'pattern': r'\bEXIT\s+WHEN\s+(.+?)\s*;',
        'replacement': r'IF \1 THEN LEAVE; END IF;',
        'note': 'Convert EXIT WHEN to IF/LEAVE'
    },
    'EXIT_LABEL': {
        'pattern': r'\bEXIT\s+(\w+)\s*;',
        'replacement': r'LEAVE \1;',
        'note': 'Use LEAVE with label'
    },
    'EXIT_LABEL_WHEN': {
        'pattern': r'\bEXIT\s+(\w+)\s+WHEN\s+(.+?)\s*;',
        'replacement': r'IF \2 THEN LEAVE \1; END IF;',
        'note': 'Convert EXIT label WHEN to IF/LEAVE'
    },
    'CONTINUE': {
        'pattern': r'\bCONTINUE\s*;',
        'replacement': 'ITERATE;',
        'note': 'Use ITERATE to continue loop'
    },
    'CONTINUE_WHEN': {
        'pattern': r'\bCONTINUE\s+WHEN\s+(.+?)\s*;',
        'replacement': r'IF \1 THEN ITERATE; END IF;',
        'note': 'Convert CONTINUE WHEN to IF/ITERATE'
    },
    'CONTINUE_LABEL': {
        'pattern': r'\bCONTINUE\s+(\w+)\s*;',
        'replacement': r'ITERATE \1;',
        'note': 'Use ITERATE with label'
    },
    
    # GOTO - not supported, needs restructuring
    'GOTO': {
        'pattern': r'\bGOTO\s+(\w+)\s*;',
        'replacement': r'-- TODO: GOTO \1 - Restructure code to eliminate GOTO',
        'note': 'GOTO not supported in Databricks - restructure code'
    },
    'LABEL': {
        'pattern': r'<<(\w+)>>',
        'replacement': r'\1:',
        'note': 'Convert label syntax'
    },
    
    # NULL statement
    'NULL_STMT': {
        'pattern': r'\bNULL\s*;',
        'replacement': 'SELECT 1;  -- NULL statement (no-op)',
        'note': 'NULL statement as no-op'
    },
    
    # RAISE without exception name (re-raise)
    'RAISE_RERAISE': {
        'pattern': r'\bRAISE\s*;',
        'replacement': 'RESIGNAL;',
        'note': 'Use RESIGNAL to re-raise exception'
    },
    
    # RAISE with exception name
    'RAISE_NAMED': {
        'pattern': r'\bRAISE\s+(\w+)\s*;',
        'replacement': r"SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '\1';",
        'note': 'Use SIGNAL to raise named exception'
    },
    
    # PIPE ROW (pipelined functions)
    'PIPE_ROW': {
        'pattern': r'\bPIPE\s+ROW\s*\(\s*(.+?)\s*\)\s*;',
        'replacement': r'-- TODO: PIPE ROW(\1) - Use table-valued function or temporary table',
        'note': 'Pipelined functions need manual conversion'
    },
}


# =============================================================================
# CURSOR ATTRIBUTE MAPPINGS
# =============================================================================

CURSOR_ATTRIBUTE_MAPPINGS = {
    '%FOUND': {
        'pattern': r'(\w+)%FOUND\b',
        'replacement': r'(\1_found = TRUE)',
        'note': 'Track cursor found status in variable'
    },
    '%NOTFOUND': {
        'pattern': r'(\w+)%NOTFOUND\b',
        'replacement': r'(\1_found = FALSE)',
        'note': 'Track cursor notfound status in variable'
    },
    '%ROWCOUNT': {
        'pattern': r'(\w+)%ROWCOUNT\b',
        'replacement': r'\1_rowcount',
        'note': 'Track cursor rowcount in variable'
    },
    '%ISOPEN': {
        'pattern': r'(\w+)%ISOPEN\b',
        'replacement': r'\1_isopen',
        'note': 'Track cursor open status in variable'
    },
    # SQL implicit cursor attributes
    'SQL%FOUND': {
        'pattern': r'SQL%FOUND\b',
        'replacement': r'(ROW_COUNT() > 0)',
        'note': 'Use ROW_COUNT() for implicit cursor'
    },
    'SQL%NOTFOUND': {
        'pattern': r'SQL%NOTFOUND\b',
        'replacement': r'(ROW_COUNT() = 0)',
        'note': 'Use ROW_COUNT() for implicit cursor'
    },
    'SQL%ROWCOUNT': {
        'pattern': r'SQL%ROWCOUNT\b',
        'replacement': r'ROW_COUNT()',
        'note': 'Use ROW_COUNT() for affected rows'
    },
    'SQL%ISOPEN': {
        'pattern': r'SQL%ISOPEN\b',
        'replacement': r'FALSE',
        'note': 'Implicit cursor is never open in Databricks'
    },
}


# =============================================================================
# COLLECTION METHOD MAPPINGS
# =============================================================================

COLLECTION_METHOD_MAPPINGS = {
    'COUNT': {
        'pattern': r'(\w+)\.COUNT\b',
        'replacement': r'SIZE(\1)',
        'note': 'Use SIZE() for collection count'
    },
    'FIRST': {
        'pattern': r'(\w+)\.FIRST\b',
        'replacement': r'1',
        'note': 'First index is 1 for arrays'
    },
    'LAST': {
        'pattern': r'(\w+)\.LAST\b',
        'replacement': r'SIZE(\1)',
        'note': 'Last index equals size'
    },
    'EXISTS': {
        'pattern': r'(\w+)\.EXISTS\s*\(\s*(.+?)\s*\)',
        'replacement': r'(\2 > 0 AND \2 <= SIZE(\1))',
        'note': 'Check index bounds'
    },
    'PRIOR': {
        'pattern': r'(\w+)\.PRIOR\s*\(\s*(.+?)\s*\)',
        'replacement': r'(\2 - 1)',
        'note': 'Previous index'
    },
    'NEXT': {
        'pattern': r'(\w+)\.NEXT\s*\(\s*(.+?)\s*\)',
        'replacement': r'(\2 + 1)',
        'note': 'Next index'
    },
    'DELETE': {
        'pattern': r'(\w+)\.DELETE\s*(?:\(\s*\))?\s*;',
        'replacement': r'SET \1 = ARRAY();  -- Clear collection',
        'note': 'Clear collection by assigning empty array'
    },
    'DELETE_ELEM': {
        'pattern': r'(\w+)\.DELETE\s*\(\s*(.+?)\s*\)\s*;',
        'replacement': r'-- TODO: Delete element at index \2 from \1 - use FILTER or rebuild array',
        'note': 'Element deletion needs FILTER or array reconstruction'
    },
    'EXTEND': {
        'pattern': r'(\w+)\.EXTEND\s*(?:\(\s*(\d+)?\s*\))?\s*;',
        'replacement': r'-- Note: EXTEND not needed - arrays grow automatically',
        'note': 'Arrays grow automatically in Databricks'
    },
    'TRIM': {
        'pattern': r'(\w+)\.TRIM\s*(?:\(\s*(\d+)?\s*\))?\s*;',
        'replacement': r'SET \1 = SLICE(\1, 1, SIZE(\1) - COALESCE(\2, 1));',
        'note': 'Use SLICE to trim elements from end'
    },
    'LIMIT': {
        'pattern': r'(\w+)\.LIMIT\b',
        'replacement': r'2147483647',
        'note': 'No practical limit for arrays'
    },
}


class PLSQLObjectType(Enum):
    """Types of PL/SQL objects."""
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    PACKAGE = "PACKAGE"
    PACKAGE_BODY = "PACKAGE_BODY"
    TRIGGER = "TRIGGER"
    ANONYMOUS_BLOCK = "ANONYMOUS_BLOCK"


@dataclass
class PLSQLParameter:
    """Represents a PL/SQL procedure/function parameter."""
    name: str
    data_type: str
    mode: str = "IN"  # IN, OUT, IN OUT
    default_value: Optional[str] = None
    
    def to_databricks(self) -> str:
        """Convert to Databricks parameter syntax."""
        db_type = get_databricks_data_type(self.data_type)
        
        # Databricks doesn't support OUT parameters in the same way
        # We'll convert OUT/IN OUT to return values where possible
        if self.mode == "IN":
            return f"{self.name} {db_type}"
        else:
            return f"{self.name} {db_type}  -- Original mode: {self.mode}"


@dataclass
class PLSQLVariable:
    """Represents a PL/SQL variable declaration."""
    name: str
    data_type: str
    default_value: Optional[str] = None
    is_constant: bool = False
    
    def to_databricks(self) -> str:
        """Convert to Databricks variable declaration."""
        db_type = get_databricks_data_type(self.data_type)
        
        # Use DECLARE VARIABLE syntax for Databricks SQL
        if self.default_value:
            return f"DECLARE VARIABLE {self.name} {db_type} DEFAULT {self.default_value};"
        else:
            return f"DECLARE VARIABLE {self.name} {db_type};"


@dataclass
class ConversionResult:
    """Result of a PL/SQL conversion."""
    original_code: str
    converted_code: str
    object_type: PLSQLObjectType
    object_name: str
    success: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    manual_review_required: List[str] = field(default_factory=list)


class PLSQLConverter:
    """
    Converter for Oracle PL/SQL code to Databricks SQL.
    
    Handles:
    - Stored procedures
    - Functions
    - Package specifications and bodies
    - Anonymous blocks
    - Triggers (with warnings about limitations)
    
    Example:
        >>> converter = PLSQLConverter()
        >>> result = converter.convert('''
        ...     CREATE OR REPLACE PROCEDURE hello_world IS
        ...     BEGIN
        ...         DBMS_OUTPUT.PUT_LINE('Hello, World!');
        ...     END;
        ... ''')
        >>> print(result.converted_code)
    """
    
    def __init__(self):
        """Initialize the PL/SQL converter."""
        self.sql_translator = OracleToDatabricksTranslator()
        
        # Patterns for parsing PL/SQL constructs
        self._proc_pattern = re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(\w+(?:\.\w+)?)\s*'
            r'(?:\((.*?)\))?\s*'
            r'(?:IS|AS)\s*'
            r'(.*?)'
            r'BEGIN\s*'
            r'(.*?)'
            r'END\s*\1?\s*;',
            re.IGNORECASE | re.DOTALL
        )
        
        self._func_pattern = re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+(?:\.\w+)?)\s*'
            r'(?:\((.*?)\))?\s*'
            r'RETURN\s+(\w+(?:\s*\([^)]*\))?)\s*'
            r'(?:IS|AS)\s*'
            r'(.*?)'
            r'BEGIN\s*'
            r'(.*?)'
            r'END\s*\1?\s*;',
            re.IGNORECASE | re.DOTALL
        )
        
        self._package_pattern = re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+(?:BODY\s+)?(\w+(?:\.\w+)?)\s*'
            r'(?:IS|AS)\s*'
            r'(.*?)'
            r'END\s*\1?\s*;',
            re.IGNORECASE | re.DOTALL
        )
        
        # PL/SQL statement patterns
        self._if_pattern = re.compile(
            r'\bIF\s+(.+?)\s+THEN\s*',
            re.IGNORECASE | re.DOTALL
        )
        
        self._for_loop_pattern = re.compile(
            r'\bFOR\s+(\w+)\s+IN\s+(.+?)\s+LOOP\s*',
            re.IGNORECASE | re.DOTALL
        )
        
        self._while_loop_pattern = re.compile(
            r'\bWHILE\s+(.+?)\s+LOOP\s*',
            re.IGNORECASE | re.DOTALL
        )
        
        self._cursor_for_pattern = re.compile(
            r'\bFOR\s+(\w+)\s+IN\s+\(\s*(SELECT.+?)\s*\)\s*LOOP\s*',
            re.IGNORECASE | re.DOTALL
        )
    
    def convert(self, plsql_code: str) -> ConversionResult:
        """
        Convert PL/SQL code to Databricks SQL.
        
        Args:
            plsql_code: PL/SQL source code to convert
            
        Returns:
            ConversionResult with converted code and any issues
        """
        # Strip comments before processing
        plsql_code = strip_sql_comments(plsql_code.strip())
        
        # Check if there's any code content after stripping comments
        if not plsql_code.strip():
            return ConversionResult(
                original_code="",
                converted_code="",
                object_type=PLSQLObjectType.ANONYMOUS_BLOCK,
                object_name="",
                success=True,
                errors=[],
                warnings=["Empty or comment-only block skipped"],
                manual_review_required=False
            )
        
        # Detect object type
        obj_type = self._detect_object_type(plsql_code)
        
        if obj_type == PLSQLObjectType.PROCEDURE:
            return self._convert_procedure(plsql_code)
        elif obj_type == PLSQLObjectType.FUNCTION:
            return self._convert_function(plsql_code)
        elif obj_type == PLSQLObjectType.PACKAGE:
            return self._convert_package(plsql_code)
        elif obj_type == PLSQLObjectType.PACKAGE_BODY:
            return self._convert_package_body(plsql_code)
        elif obj_type == PLSQLObjectType.TRIGGER:
            return self._convert_trigger(plsql_code)
        else:
            return self._convert_anonymous_block(plsql_code)
    
    def convert_file(self, input_path: str, output_path: Optional[str] = None) -> List[ConversionResult]:
        """
        Convert a PL/SQL file to Databricks SQL.
        
        Args:
            input_path: Path to input PL/SQL file
            output_path: Optional path to output file
            
        Returns:
            List of ConversionResult for each object in the file
        """
        with open(input_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split into individual objects
        objects = self._split_plsql_objects(content)
        results = []
        
        for obj in objects:
            if obj.strip():
                result = self.convert(obj)
                results.append(result)
        
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                for result in results:
                    f.write(f"-- Converted from Oracle PL/SQL: {result.object_name}\n")
                    if result.warnings:
                        for warning in result.warnings:
                            f.write(f"-- WARNING: {warning}\n")
                    if result.manual_review_required:
                        f.write("-- MANUAL REVIEW REQUIRED:\n")
                        for item in result.manual_review_required:
                            f.write(f"--   - {item}\n")
                    f.write(result.converted_code)
                    f.write("\n\n")
        
        return results
    
    def _detect_object_type(self, code: str) -> PLSQLObjectType:
        """Detect the type of PL/SQL object."""
        upper_code = code.upper().strip()
        
        if 'CREATE' in upper_code[:50]:
            if 'PACKAGE BODY' in upper_code:
                return PLSQLObjectType.PACKAGE_BODY
            elif 'PACKAGE' in upper_code:
                return PLSQLObjectType.PACKAGE
            elif 'PROCEDURE' in upper_code:
                return PLSQLObjectType.PROCEDURE
            elif 'FUNCTION' in upper_code:
                return PLSQLObjectType.FUNCTION
            elif 'TRIGGER' in upper_code:
                return PLSQLObjectType.TRIGGER
        
        if upper_code.startswith('DECLARE') or upper_code.startswith('BEGIN'):
            return PLSQLObjectType.ANONYMOUS_BLOCK
        
        return PLSQLObjectType.ANONYMOUS_BLOCK
    
    def _convert_procedure(self, code: str) -> ConversionResult:
        """Convert a PL/SQL procedure to Databricks SQL."""
        errors = []
        warnings = []
        manual_review = []
        
        # Parse procedure components
        match = self._proc_pattern.search(code)
        
        if not match:
            # Try simpler parsing
            return self._convert_procedure_simple(code)
        
        proc_name = match.group(1)
        params_str = match.group(2) or ""
        declarations = match.group(3) or ""
        body = match.group(4) or ""
        
        # Parse parameters
        params = self._parse_parameters(params_str)
        
        # Check for OUT parameters
        out_params = [p for p in params if p.mode in ("OUT", "IN OUT")]
        if out_params:
            warnings.append(
                f"OUT parameters detected: {[p.name for p in out_params]}. "
                "Databricks SQL doesn't support OUT parameters directly. "
                "Consider returning a STRUCT or using temporary tables."
            )
            manual_review.append("Convert OUT parameters to alternative approach")
        
        # Parse declarations
        variables = self._parse_declarations(declarations)
        
        # Convert body
        converted_body = self._convert_plsql_body(body, warnings, manual_review)
        
        # Build Databricks procedure
        db_params = ", ".join(p.to_databricks() for p in params)
        db_vars = "\n  ".join(v.to_databricks() for v in variables)
        
        converted_code = f"""CREATE OR REPLACE PROCEDURE {proc_name}({db_params})
LANGUAGE SQL
AS $$
BEGIN
  {db_vars}
  
{self._indent(converted_body, 2)}
END;
$$;"""
        
        return ConversionResult(
            original_code=code,
            converted_code=converted_code,
            object_type=PLSQLObjectType.PROCEDURE,
            object_name=proc_name,
            success=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            manual_review_required=manual_review
        )
    
    def _convert_procedure_simple(self, code: str) -> ConversionResult:
        """Simpler procedure conversion for edge cases."""
        warnings = []
        manual_review = []
        
        # Extract procedure name
        name_match = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(\w+(?:\.\w+)?)',
            code, re.IGNORECASE
        )
        proc_name = name_match.group(1) if name_match else "unknown_procedure"
        
        # Basic conversion
        converted = code
        
        # Replace IS/AS with LANGUAGE SQL AS $$
        converted = re.sub(
            r'\b(IS|AS)\s*(?=\s*(DECLARE|BEGIN))',
            'LANGUAGE SQL\nAS $$\n',
            converted,
            count=1,
            flags=re.IGNORECASE
        )
        
        # Add closing $$;
        converted = re.sub(r'END\s*;?\s*/?$', 'END;\n$$;', converted, flags=re.IGNORECASE)
        
        # Apply body conversions
        converted = self._apply_plsql_replacements(converted, warnings, manual_review)
        
        return ConversionResult(
            original_code=code,
            converted_code=converted,
            object_type=PLSQLObjectType.PROCEDURE,
            object_name=proc_name,
            success=True,
            warnings=warnings,
            manual_review_required=manual_review
        )
    
    def _convert_function(self, code: str) -> ConversionResult:
        """Convert a PL/SQL function to Databricks SQL."""
        errors = []
        warnings = []
        manual_review = []
        
        match = self._func_pattern.search(code)
        
        if not match:
            return self._convert_function_simple(code)
        
        func_name = match.group(1)
        params_str = match.group(2) or ""
        return_type = match.group(3)
        declarations = match.group(4) or ""
        body = match.group(5) or ""
        
        # Parse parameters
        params = self._parse_parameters(params_str)
        
        # Convert return type
        db_return_type = get_databricks_data_type(return_type)
        
        # Parse declarations
        variables = self._parse_declarations(declarations)
        
        # Convert body
        converted_body = self._convert_plsql_body(body, warnings, manual_review)
        
        # Build Databricks function
        db_params = ", ".join(p.to_databricks() for p in params)
        db_vars = "\n  ".join(v.to_databricks() for v in variables)
        
        converted_code = f"""CREATE OR REPLACE FUNCTION {func_name}({db_params})
RETURNS {db_return_type}
LANGUAGE SQL
AS $$
BEGIN
  {db_vars}
  
{self._indent(converted_body, 2)}
END;
$$;"""
        
        return ConversionResult(
            original_code=code,
            converted_code=converted_code,
            object_type=PLSQLObjectType.FUNCTION,
            object_name=func_name,
            success=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            manual_review_required=manual_review
        )
    
    def _convert_function_simple(self, code: str) -> ConversionResult:
        """Simpler function conversion for edge cases."""
        warnings = []
        manual_review = []
        
        # Extract function name
        name_match = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\w+(?:\.\w+)?)',
            code, re.IGNORECASE
        )
        func_name = name_match.group(1) if name_match else "unknown_function"
        
        # Basic conversion
        converted = code
        
        # Replace IS/AS with LANGUAGE SQL AS $$
        converted = re.sub(
            r'\b(IS|AS)\s*(?=\s*(DECLARE|BEGIN))',
            'LANGUAGE SQL\nAS $$\n',
            converted,
            count=1,
            flags=re.IGNORECASE
        )
        
        # Add closing $$;
        converted = re.sub(r'END\s*;?\s*/?$', 'END;\n$$;', converted, flags=re.IGNORECASE)
        
        # Apply body conversions
        converted = self._apply_plsql_replacements(converted, warnings, manual_review)
        
        return ConversionResult(
            original_code=code,
            converted_code=converted,
            object_type=PLSQLObjectType.FUNCTION,
            object_name=func_name,
            success=True,
            warnings=warnings,
            manual_review_required=manual_review
        )
    
    def _convert_package(self, code: str) -> ConversionResult:
        """Convert a PL/SQL package specification."""
        warnings = [
            "Databricks doesn't support packages directly. "
            "The package will be converted to individual procedures/functions."
        ]
        manual_review = ["Restructure package into individual stored procedures/functions"]
        
        # Extract package name
        name_match = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+(\w+(?:\.\w+)?)',
            code, re.IGNORECASE
        )
        pkg_name = name_match.group(1) if name_match else "unknown_package"
        
        converted = f"""-- Package: {pkg_name}
-- NOTE: Databricks doesn't support packages. 
-- This package specification should be converted to individual functions/procedures.
-- Original package contents are preserved below as comments for reference.

/*
{code}
*/

-- TODO: Extract and convert individual procedures/functions from this package
"""
        
        return ConversionResult(
            original_code=code,
            converted_code=converted,
            object_type=PLSQLObjectType.PACKAGE,
            object_name=pkg_name,
            success=True,
            warnings=warnings,
            manual_review_required=manual_review
        )
    
    def _convert_package_body(self, code: str) -> ConversionResult:
        """Convert a PL/SQL package body."""
        warnings = []
        manual_review = []
        
        # Extract package name
        name_match = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+BODY\s+(\w+(?:\.\w+)?)',
            code, re.IGNORECASE
        )
        pkg_name = name_match.group(1) if name_match else "unknown_package"
        
        # Try to extract individual procedures/functions
        procs_and_funcs = self._extract_package_members(code)
        
        converted_parts = []
        converted_parts.append(f"-- Converted from Oracle Package Body: {pkg_name}")
        converted_parts.append("-- Individual procedures and functions extracted below:\n")
        
        for member_code, member_type in procs_and_funcs:
            if member_type == "PROCEDURE":
                result = self._convert_procedure(member_code)
            else:
                result = self._convert_function(member_code)
            
            converted_parts.append(result.converted_code)
            warnings.extend(result.warnings)
            manual_review.extend(result.manual_review_required)
        
        if not procs_and_funcs:
            warnings.append("Could not extract procedures/functions from package body")
            manual_review.append("Manually extract and convert package body members")
            converted_parts.append(f"/* Original package body:\n{code}\n*/")
        
        return ConversionResult(
            original_code=code,
            converted_code="\n\n".join(converted_parts),
            object_type=PLSQLObjectType.PACKAGE_BODY,
            object_name=pkg_name,
            success=True,
            warnings=warnings,
            manual_review_required=manual_review
        )
    
    def _convert_trigger(self, code: str) -> ConversionResult:
        """Convert a PL/SQL trigger - note limitations."""
        warnings = [
            "Databricks has limited trigger support compared to Oracle. "
            "Consider using Delta Lake Change Data Feed or Structured Streaming instead."
        ]
        manual_review = [
            "Review trigger logic and implement using Delta Lake features",
            "Consider using workflows or scheduled jobs for similar functionality"
        ]
        
        # Extract trigger name
        name_match = re.search(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+(\w+(?:\.\w+)?)',
            code, re.IGNORECASE
        )
        trigger_name = name_match.group(1) if name_match else "unknown_trigger"
        
        converted = f"""-- Trigger: {trigger_name}
-- WARNING: Databricks doesn't support database triggers like Oracle.
-- Consider these alternatives:
--   1. Delta Lake Change Data Feed for capturing changes
--   2. Structured Streaming for real-time processing
--   3. Databricks Workflows for scheduled operations
--   4. Unity Catalog event hooks (if applicable)

/* Original Oracle Trigger:
{code}
*/

-- TODO: Implement equivalent logic using Databricks-native features
"""
        
        return ConversionResult(
            original_code=code,
            converted_code=converted,
            object_type=PLSQLObjectType.TRIGGER,
            object_name=trigger_name,
            success=True,
            warnings=warnings,
            manual_review_required=manual_review
        )
    
    def _convert_anonymous_block(self, code: str) -> ConversionResult:
        """Convert a PL/SQL anonymous block."""
        warnings = []
        manual_review = []
        
        # Detect cursor usage
        if re.search(r'\bCURSOR\s+\w+\s+IS\b', code, re.IGNORECASE):
            manual_review.append("Explicit cursor detected - review cursor handling")
            warnings.append("Explicit cursors may need manual conversion to Databricks SQL")
        
        # Detect cursor FOR loops
        if re.search(r'\bFOR\s+\w+\s+IN\s+\w+\s+LOOP\b', code, re.IGNORECASE):
            warnings.append("Cursor FOR loop detected - may need conversion to set-based operations")
        
        # Detect FETCH statements
        if re.search(r'\bFETCH\s+\w+\s+INTO\b', code, re.IGNORECASE):
            warnings.append("FETCH INTO detected - consider set-based alternatives")
        
        # Detect %NOTFOUND, %FOUND, %ROWCOUNT
        if re.search(r'\w+%(?:NOTFOUND|FOUND|ROWCOUNT)\b', code, re.IGNORECASE):
            manual_review.append("Cursor attributes (%NOTFOUND, %FOUND, %ROWCOUNT) need manual review")
        
        # Detect EXIT WHEN
        if re.search(r'\bEXIT\s+WHEN\b', code, re.IGNORECASE):
            warnings.append("EXIT WHEN clause - control flow may need adjustment")
        
        # Apply standard replacements
        converted = self._apply_plsql_replacements(code, warnings, manual_review)
        
        # Parse and convert variable declarations if DECLARE block exists
        converted = self._convert_declare_section(converted, warnings, manual_review)
        
        # Add header comment
        converted = f"-- Converted PL/SQL Anonymous Block\n-- Note: Databricks SQL procedural support is limited. Review and test carefully.\n\n{converted}"
        
        return ConversionResult(
            original_code=code,
            converted_code=converted,
            object_type=PLSQLObjectType.ANONYMOUS_BLOCK,
            object_name="anonymous_block",
            success=True,
            warnings=warnings,
            manual_review_required=manual_review
        )
    
    def _convert_declare_section(self, code: str, 
                                  warnings: List[str], 
                                  manual_review: List[str]) -> str:
        """
        Convert DECLARE section variables to Databricks format.
        
        Args:
            code: PL/SQL code with DECLARE section
            warnings: List to append warnings to
            manual_review: List to append manual review items to
            
        Returns:
            Converted code
        """
        # Match DECLARE section
        declare_match = re.match(
            r'(--[^\n]*\n)*\s*DECLARE\s+(.*?)\s*BEGIN\s+',
            code,
            re.IGNORECASE | re.DOTALL
        )
        
        if not declare_match:
            return code
        
        declare_section = declare_match.group(2) if declare_match.group(2) else ""
        
        # Parse cursor declarations
        cursor_pattern = re.compile(
            r'CURSOR\s+(\w+)\s+IS\s+(.*?);',
            re.IGNORECASE | re.DOTALL
        )
        
        cursors = {}
        for cursor_match in cursor_pattern.finditer(declare_section):
            cursor_name = cursor_match.group(1)
            cursor_query = cursor_match.group(2).strip()
            cursors[cursor_name.upper()] = cursor_query
            
        if cursors:
            warnings.append(f"Found {len(cursors)} cursor(s): {', '.join(cursors.keys())}")
        
        # Parse variable declarations
        var_pattern = re.compile(
            r'(\w+)\s+(VARCHAR2|NUMBER|DATE|BOOLEAN|INTEGER|PLS_INTEGER|BINARY_INTEGER|CHAR|CLOB|BLOB)\s*(?:\([^)]*\))?\s*(?:(?::=|DEFAULT)\s*([^;]+))?\s*;',
            re.IGNORECASE
        )
        
        variables = []
        for var_match in var_pattern.finditer(declare_section):
            var_name = var_match.group(1)
            var_type = var_match.group(2)
            var_default = var_match.group(3)
            
            # Convert type
            db_type = get_databricks_data_type(var_type)
            
            # Use DECLARE VARIABLE syntax for Databricks SQL
            if var_default:
                variables.append(f"DECLARE VARIABLE {var_name} {db_type} DEFAULT {var_default};")
            else:
                variables.append(f"DECLARE VARIABLE {var_name} {db_type};")
        
        # Parse %TYPE and %ROWTYPE declarations
        type_pattern = re.compile(
            r'(\w+)\s+(\w+(?:\.\w+)?)%(?:TYPE|ROWTYPE)\s*;',
            re.IGNORECASE
        )
        
        for type_match in type_pattern.finditer(declare_section):
            var_name = type_match.group(1)
            ref_name = type_match.group(2)
            manual_review.append(f"Variable {var_name} uses %TYPE/%ROWTYPE reference to {ref_name}")
            variables.append(f"-- TODO: {var_name} references {ref_name}%TYPE - determine actual type")
        
        if variables:
            # Insert converted variable declarations, replacing original DECLARE section
            # Keep VARIABLE declarations with proper format
            clean_vars = []
            for v in variables:
                # Remove leading "DECLARE VARIABLE " prefix - we'll add DECLARE block wrapper
                clean_v = re.sub(r'^DECLARE\s+VARIABLE\s+', 'VARIABLE ', v)
                clean_v = re.sub(r'^DECLARE\s+', '', clean_v)  # Fallback for old format
                clean_vars.append(clean_v)
            
            var_block = "\n  ".join(clean_vars)
            code = re.sub(
                r'DECLARE\s+.*?(?=BEGIN\s+)',
                f'DECLARE\n  -- Converted variable declarations:\n  {var_block}\n',
                code,
                count=1,
                flags=re.IGNORECASE | re.DOTALL
            )
        
        return code
    
    def _parse_parameters(self, params_str: str) -> List[PLSQLParameter]:
        """Parse PL/SQL parameter list."""
        params = []
        if not params_str.strip():
            return params
        
        # Split by comma (handling nested parentheses)
        param_list = self._split_params(params_str)
        
        for param in param_list:
            param = param.strip()
            if not param:
                continue
            
            # Parse: name [IN|OUT|IN OUT] datatype [DEFAULT|:= value]
            match = re.match(
                r'(\w+)\s+'
                r'(?:(IN\s+OUT|IN|OUT)\s+)?'
                r'(\w+(?:\s*\([^)]*\))?)'
                r'(?:\s+(?:DEFAULT|:=)\s+(.+))?',
                param.strip(),
                re.IGNORECASE
            )
            
            if match:
                params.append(PLSQLParameter(
                    name=match.group(1),
                    mode=(match.group(2) or "IN").upper().replace("IN OUT", "IN OUT"),
                    data_type=match.group(3),
                    default_value=match.group(4)
                ))
        
        return params
    
    def _parse_declarations(self, decl_str: str) -> List[PLSQLVariable]:
        """Parse PL/SQL variable declarations."""
        variables = []
        
        # Match variable declarations
        pattern = re.compile(
            r'(\w+)\s+(?:(CONSTANT)\s+)?(\w+(?:\s*\([^)]*\))?)'
            r'(?:\s*(?::=|DEFAULT)\s*(.+?))?;',
            re.IGNORECASE
        )
        
        for match in pattern.finditer(decl_str):
            variables.append(PLSQLVariable(
                name=match.group(1),
                is_constant=match.group(2) is not None,
                data_type=match.group(3),
                default_value=match.group(4)
            ))
        
        return variables
    
    def _convert_plsql_body(self, body: str, 
                            warnings: List[str], 
                            manual_review: List[str]) -> str:
        """Convert PL/SQL body statements to Databricks SQL."""
        return self._apply_plsql_replacements(body, warnings, manual_review)
    
    def _apply_plsql_replacements(self, code: str, 
                                   warnings: List[str], 
                                   manual_review: List[str]) -> str:
        """
        Apply comprehensive PL/SQL to Databricks SQL replacements.
        
        Converts:
        - Oracle built-in packages (DBMS_OUTPUT, DBMS_LOB, UTL_FILE, etc.)
        - Exception handling
        - Control flow statements
        - Cursor attributes
        - Collection methods
        - Data type conversions
        """
        result = code
        
        # 1. Apply Oracle built-in package conversions
        result = self._convert_oracle_packages(result, warnings, manual_review)
        
        # 2. Apply exception handling conversions
        result = self._convert_exception_handling(result, warnings, manual_review)
        
        # 3. Apply control flow conversions
        result = self._convert_control_flow(result, warnings, manual_review)
        
        # 4. Apply cursor attribute conversions
        result = self._convert_cursor_attributes(result, warnings, manual_review)
        
        # 5. Apply collection method conversions
        result = self._convert_collection_methods(result, warnings, manual_review)
        
        # 6. Apply SELECT INTO conversions (before basic replacements)
        result = self._convert_select_into(result, warnings, manual_review)
        
        # 7. Translate embedded SQL statements using the SQL translator
        result = self._translate_embedded_sql(result, warnings, manual_review)
        
        # 8. Apply basic PL/SQL to Databricks replacements
        result = self._apply_basic_replacements(result, warnings, manual_review)
        
        return result
    
    def _convert_oracle_packages(self, code: str, 
                                  warnings: List[str], 
                                  manual_review: List[str]) -> str:
        """Convert Oracle built-in package calls to Databricks equivalents."""
        result = code
        packages_used = set()
        
        # Detect which packages are used
        for pkg_name in ORACLE_PACKAGE_MAPPINGS.keys():
            if pkg_name in result.upper():
                packages_used.add(pkg_name)
        
        # Apply conversions for each detected package
        for pkg_name in packages_used:
            pkg_mappings = ORACLE_PACKAGE_MAPPINGS[pkg_name]
            
            for method_name, mapping in pkg_mappings.items():
                pattern = mapping.get('pattern')
                replacement = mapping.get('replacement')
                note = mapping.get('note', '')
                
                if pattern and replacement:
                    # Check if this pattern matches
                    if re.search(pattern, result, re.IGNORECASE):
                        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
                        
                        # Add appropriate warnings/reviews
                        if mapping.get('databricks') is None:
                            manual_review.append(f"{pkg_name}.{method_name}: {note}")
                        else:
                            warnings.append(f"{pkg_name}.{method_name} converted: {note}")
        
        return result
    
    def _convert_exception_handling(self, code: str, 
                                     warnings: List[str], 
                                     manual_review: List[str]) -> str:
        """Convert Oracle exception handling to Databricks SQL."""
        result = code
        
        # Check if there's exception handling
        if 'EXCEPTION' not in result.upper():
            return result
        
        warnings.append("Exception handling converted to Databricks SQL EXCEPTION block")
        
        # Convert WHEN OTHERS THEN
        result = re.sub(
            r'\bWHEN\s+OTHERS\s+THEN\b',
            'WHEN OTHER THEN',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert named Oracle exceptions to SQLSTATE
        for exc_name, exc_info in ORACLE_EXCEPTIONS.items():
            if exc_name == 'OTHERS':
                continue  # Already handled above
            
            pattern = rf'\bWHEN\s+{exc_name}\s+THEN\b'
            if re.search(pattern, result, re.IGNORECASE):
                databricks_handler = exc_info['databricks']
                result = re.sub(pattern, databricks_handler, result, flags=re.IGNORECASE)
                warnings.append(f"Exception {exc_name} converted to SQLSTATE '{exc_info['sqlstate']}'")
        
        # Convert RAISE_APPLICATION_ERROR to SIGNAL
        if 'RAISE_APPLICATION_ERROR' in result.upper():
            result = re.sub(
                r"RAISE_APPLICATION_ERROR\s*\(\s*(-?\d+)\s*,\s*(.+?)\s*\)\s*;",
                r"SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = \2;  -- Error code: \1",
                result,
                flags=re.IGNORECASE
            )
            warnings.append("RAISE_APPLICATION_ERROR converted to SIGNAL SQLSTATE")
        
        # Convert RAISE (re-raise) to RESIGNAL
        result = re.sub(
            r'\bRAISE\s*;(?!\s*USING)',
            'RESIGNAL;',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert RAISE exception_name to SIGNAL
        for exc_name, exc_info in ORACLE_EXCEPTIONS.items():
            pattern = rf'\bRAISE\s+{exc_name}\s*;'
            if re.search(pattern, result, re.IGNORECASE):
                sqlstate = exc_info.get('sqlstate', '45000')
                result = re.sub(
                    pattern,
                    f"SIGNAL SQLSTATE '{sqlstate}' SET MESSAGE_TEXT = '{exc_name}';",
                    result,
                    flags=re.IGNORECASE
                )
        
        # Convert SQLERRM
        if 'SQLERRM' in result.upper():
            result = re.sub(r'\bSQLERRM\b', 'ERROR_MESSAGE()', result, flags=re.IGNORECASE)
            warnings.append("SQLERRM converted to ERROR_MESSAGE()")
        
        # Convert SQLCODE
        if 'SQLCODE' in result.upper():
            result = re.sub(r'\bSQLCODE\b', 'ERROR_CODE()', result, flags=re.IGNORECASE)
            warnings.append("SQLCODE converted to ERROR_CODE()")
        
        return result
    
    def _convert_control_flow(self, code: str, 
                               warnings: List[str], 
                               manual_review: List[str]) -> str:
        """Convert PL/SQL control flow to Databricks SQL scripting."""
        result = code
        
        # Apply control flow mappings
        for name, mapping in PLSQL_CONTROL_FLOW_MAPPINGS.items():
            pattern = mapping['pattern']
            replacement = mapping['replacement']
            note = mapping.get('note', '')
            
            if re.search(pattern, result, re.IGNORECASE):
                result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
                if 'TODO' in replacement:
                    manual_review.append(f"{name}: {note}")
                else:
                    warnings.append(f"Control flow {name} converted: {note}")
        
        # Convert LOOP...END LOOP (simple loop)
        result = re.sub(
            r'\bLOOP\s*\n',
            'LOOP\n',
            result,
            flags=re.IGNORECASE
        )
        result = re.sub(
            r'\bEND\s+LOOP\s*;',
            'END LOOP;',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert labeled loops: <<label>> FOR/WHILE -> label: FOR/WHILE
        result = re.sub(
            r'<<(\w+)>>\s*(FOR|WHILE|LOOP)',
            r'\1: \2',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert FOR..IN..LOOP to Databricks FOR..IN..DO
        # FOR i IN 1..10 LOOP -> FOR i IN 1 TO 10 DO
        result = re.sub(
            r'\bFOR\s+(\w+)\s+IN\s+(\w+)\.\.(\w+)\s+LOOP\b',
            r'FOR \1 IN \2 TO \3 DO',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert FOR i IN REVERSE 1..10 LOOP -> FOR i IN REVERSE 10 TO 1 DO  
        result = re.sub(
            r'\bFOR\s+(\w+)\s+IN\s+REVERSE\s+(\w+)\.\.(\w+)\s+LOOP\b',
            r'FOR \1 IN REVERSE \3 TO \2 DO',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert END LOOP to END FOR for range loops
        # (This is a simplification - may need more context-aware conversion)
        
        # Convert WHILE..LOOP to WHILE..DO
        result = re.sub(
            r'\bWHILE\s+(.+?)\s+LOOP\b',
            r'WHILE \1 DO',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert cursor FOR loops: FOR rec IN cursor_name LOOP
        result = re.sub(
            r'\bFOR\s+(\w+)\s+IN\s+(\w+)\s+LOOP\b',
            r'FOR \1 IN \2 DO',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert cursor FOR loops with inline SELECT: FOR rec IN (SELECT...) LOOP
        result = re.sub(
            r'\bFOR\s+(\w+)\s+IN\s+\(\s*(SELECT.+?)\s*\)\s+LOOP\b',
            r'FOR \1 IN (\2) DO',
            result,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        return result
    
    def _convert_cursor_attributes(self, code: str, 
                                    warnings: List[str], 
                                    manual_review: List[str]) -> str:
        """Convert PL/SQL cursor attributes to Databricks equivalents."""
        result = code
        
        # Check for cursor attributes
        if '%' not in result:
            return result
        
        # Apply cursor attribute mappings
        for attr_name, mapping in CURSOR_ATTRIBUTE_MAPPINGS.items():
            pattern = mapping['pattern']
            replacement = mapping['replacement']
            note = mapping.get('note', '')
            
            if re.search(pattern, result, re.IGNORECASE):
                result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
                if attr_name.startswith('SQL%'):
                    warnings.append(f"Implicit cursor attribute {attr_name} converted")
                else:
                    manual_review.append(f"Cursor attribute {attr_name}: {note}")
        
        # Convert %TYPE declarations
        if '%TYPE' in result.upper():
            result = re.sub(
                r'(\w+)\s+(\w+(?:\.\w+)?)%TYPE\b',
                r'\1 STRING  -- TODO: Determine actual type from \2',
                result,
                flags=re.IGNORECASE
            )
            manual_review.append("%TYPE references need actual type lookup")
        
        # Convert %ROWTYPE declarations
        if '%ROWTYPE' in result.upper():
            result = re.sub(
                r'(\w+)\s+(\w+(?:\.\w+)?)%ROWTYPE\b',
                r'\1 STRUCT<>  -- TODO: Define struct from \2 table/cursor',
                result,
                flags=re.IGNORECASE
            )
            manual_review.append("%ROWTYPE references need struct definition from table schema")
        
        return result
    
    def _convert_collection_methods(self, code: str, 
                                     warnings: List[str], 
                                     manual_review: List[str]) -> str:
        """Convert PL/SQL collection methods to Databricks equivalents."""
        result = code
        
        # Apply collection method mappings
        for method_name, mapping in COLLECTION_METHOD_MAPPINGS.items():
            pattern = mapping['pattern']
            replacement = mapping['replacement']
            note = mapping.get('note', '')
            
            if re.search(pattern, result, re.IGNORECASE):
                result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
                if 'TODO' in replacement:
                    manual_review.append(f"Collection.{method_name}: {note}")
                else:
                    warnings.append(f"Collection.{method_name} converted: {note}")
        
        # Convert BULK COLLECT INTO
        if 'BULK COLLECT' in result.upper():
            result = re.sub(
                r'\bBULK\s+COLLECT\s+INTO\s+(\w+)\b',
                r'INTO \1  -- Note: Returns array automatically',
                result,
                flags=re.IGNORECASE
            )
            warnings.append("BULK COLLECT converted - result is now array type")
        
        # Convert FORALL with INDICES OF
        result = re.sub(
            r'\bFORALL\s+(\w+)\s+IN\s+INDICES\s+OF\s+(\w+)\b',
            r'-- FORALL with INDICES OF: Use array functions or TRANSFORM\nFOR \1 IN 1 TO SIZE(\2) DO',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert FORALL with VALUES OF
        result = re.sub(
            r'\bFORALL\s+(\w+)\s+IN\s+VALUES\s+OF\s+(\w+)\b',
            r'-- FORALL with VALUES OF: Use array functions\nFOR \1 IN ARRAY_ELEMENTS(\2) DO',
            result,
            flags=re.IGNORECASE
        )
        
        # Convert FORALL with range
        result = re.sub(
            r'\bFORALL\s+(\w+)\s+IN\s+(\w+)\.\.(\w+)\b',
            r'FOR \1 IN \2 TO \3 DO  -- Converted from FORALL',
            result,
            flags=re.IGNORECASE
        )
        
        if 'FORALL' in result.upper():
            manual_review.append("FORALL needs review - batch DML converted to loop")
        
        return result
    
    def _convert_select_into(self, code: str,
                              warnings: List[str],
                              manual_review: List[str]) -> str:
        """
        Convert Oracle SELECT INTO statements to Databricks SET variable = (SELECT ...) syntax.
        
        Handles:
        - Single variable: SELECT col INTO v_name FROM table WHERE ...
          -> SET v_name = (SELECT col FROM table WHERE ...)
        - Multiple variables: SELECT col1, col2 INTO v1, v2 FROM table WHERE ...
          -> SET (v1, v2) = (SELECT col1, col2 FROM table WHERE ...)
          OR split into multiple SET statements
        - BULK COLLECT INTO: Already handled separately
        
        Args:
            code: PL/SQL code
            warnings: List to append warnings
            manual_review: List to append manual review items
            
        Returns:
            Converted code
        """
        result = code
        
        # Pattern for single or multiple column SELECT INTO
        # SELECT column(s) INTO variable(s) FROM table [WHERE ...]
        # Note: BULK COLLECT INTO is handled separately - we skip it in the callback
        select_into_pattern = re.compile(
            r'\bSELECT\s+'                    # SELECT keyword
            r'([^;]+?)'                       # columns and possible BULK COLLECT (capture group 1)
            r'\s+INTO\s+'                     # INTO keyword
            r'([\w\s,\.]+?)'                  # variables (capture group 2)
            r'\s+(FROM\s+[^;]+?)'             # FROM clause and rest of query (capture group 3)
            r';',                             # Statement terminator
            re.IGNORECASE | re.DOTALL
        )
        
        def convert_select_into_match(match):
            full_match = match.group(0)
            columns = match.group(1).strip()
            variables = match.group(2).strip()
            from_clause = match.group(3).strip()
            
            # Skip BULK COLLECT INTO - let existing handler deal with it
            if 'BULK' in columns.upper() and 'COLLECT' in columns.upper():
                return full_match  # Return unchanged
            
            # Parse variables (handle comma-separated list)
            var_list = [v.strip() for v in variables.split(',')]
            
            # Clean up columns (remove any BULK COLLECT keywords)
            columns_clean = columns.strip()
            
            if len(var_list) == 1:
                # Single variable assignment
                var_name = var_list[0]
                return f"SET {var_name} = (SELECT {columns_clean} {from_clause});"
            else:
                # Multiple variables - Databricks supports structured assignment
                # Use individual SET statements for each variable for compatibility
                col_list = [c.strip() for c in columns_clean.split(',')]
                
                if len(col_list) == len(var_list):
                    # Create subquery alias and assign each variable
                    subquery_alias = '_temp_result'
                    col_aliases = [f"c{i}" for i in range(len(col_list))]
                    
                    # Build SELECT with aliases
                    aliased_cols = ', '.join(f"({c.strip()}) AS {col_aliases[i]}" 
                                            for i, c in enumerate(col_list))
                    
                    # Create SET statements
                    set_statements = []
                    for i, var_name in enumerate(var_list):
                        set_statements.append(
                            f"SET {var_name} = (SELECT {col_list[i].strip()} {from_clause});"
                        )
                    
                    return '\n'.join(set_statements) + f"\n-- Note: Converted from multi-variable SELECT INTO"
                else:
                    # Column/variable count mismatch - add manual review
                    return f"-- TODO: SELECT INTO variable count mismatch\n-- Original: SELECT {columns_clean} INTO {variables} {from_clause};\n{match.group(0)}"
        
        # Apply conversion
        result = select_into_pattern.sub(convert_select_into_match, result)
        
        # Check if any SELECT INTO was converted
        if result != code:
            warnings.append("SELECT INTO converted to SET variable = (SELECT ...) syntax")
        
        # Handle FETCH INTO for cursors
        # FETCH cursor_name INTO v1, v2 -> FETCH cursor_name INTO v1, v2 (same in Databricks)
        # This is already compatible, but we can add a note
        if 'FETCH' in result.upper() and 'INTO' in result.upper():
            warnings.append("FETCH INTO cursor operations may need review for Databricks compatibility")
        
        return result
    
    def _apply_basic_replacements(self, code: str, 
                                   warnings: List[str], 
                                   manual_review: List[str]) -> str:
        """Apply basic PL/SQL to Databricks SQL replacements."""
        result = code
        
        # := assignment to SET (only for standalone statements, not in declarations)
        # Match := that appears at statement level (after ; or newline or BEGIN keyword)
        # Use a more specific pattern: variable followed by := at statement start
        # Handle both simple variables (v_name) and struct members (v_rec.field)
        result = re.sub(
            r'(^|\n|;\s*|\bBEGIN\s*\n\s*)([\w.]+)\s*:=\s*',
            r'\1SET \2 = ',
            result,
            flags=re.IGNORECASE
        )
        # Handle remaining := after whitespace at line start (including struct members)
        result = re.sub(
            r'(\n\s+)([\w.]+)\s*:=\s*',
            r'\1SET \2 = ',
            result
        )
        
        # SYSDATE -> CURRENT_DATE()
        result = re.sub(r'\bSYSDATE\b', 'CURRENT_DATE()', result, flags=re.IGNORECASE)
        
        # SYSTIMESTAMP -> CURRENT_TIMESTAMP()
        result = re.sub(r'\bSYSTIMESTAMP\b', 'CURRENT_TIMESTAMP()', result, flags=re.IGNORECASE)
        
        # NVL -> COALESCE
        result = re.sub(r'\bNVL\s*\(', 'COALESCE(', result, flags=re.IGNORECASE)
        
        # NVL2 -> CASE expression
        result = re.sub(
            r'\bNVL2\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)',
            r'CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END',
            result,
            flags=re.IGNORECASE
        )
        
        # DECODE -> CASE
        # Simple DECODE(expr, val1, res1, val2, res2, default)
        # This is a basic conversion - complex DECODE needs manual review
        decode_pattern = r'\bDECODE\s*\('
        if re.search(decode_pattern, result, re.IGNORECASE):
            warnings.append("DECODE converted to CASE - review for correctness")
        
        # EXECUTE IMMEDIATE (dynamic SQL)
        if 'EXECUTE IMMEDIATE' in result.upper():
            # Convert simple EXECUTE IMMEDIATE
            result = re.sub(
                r'EXECUTE\s+IMMEDIATE\s+(.+?)\s*;',
                r'EXECUTE IMMEDIATE \1;',
                result,
                flags=re.IGNORECASE
            )
            # EXECUTE IMMEDIATE with INTO
            result = re.sub(
                r'EXECUTE\s+IMMEDIATE\s+(.+?)\s+INTO\s+(.+?)\s*;',
                r'EXECUTE IMMEDIATE \1 INTO \2;',
                result,
                flags=re.IGNORECASE
            )
            # EXECUTE IMMEDIATE with USING
            result = re.sub(
                r'EXECUTE\s+IMMEDIATE\s+(.+?)\s+USING\s+(.+?)\s*;',
                r'EXECUTE IMMEDIATE \1 USING \2;',
                result,
                flags=re.IGNORECASE
            )
            manual_review.append("EXECUTE IMMEDIATE: Verify dynamic SQL compatibility")
        
        # COMMIT/ROLLBACK - note difference
        if re.search(r'\bCOMMIT\b', result, re.IGNORECASE):
            warnings.append("COMMIT: Databricks uses auto-commit. Explicit COMMIT may be no-op.")
        if re.search(r'\bROLLBACK\b', result, re.IGNORECASE):
            warnings.append("ROLLBACK: Limited transaction support in Databricks SQL.")
        
        # SAVEPOINT
        if re.search(r'\bSAVEPOINT\b', result, re.IGNORECASE):
            manual_review.append("SAVEPOINT not supported in Databricks - restructure transaction logic")
        
        # AUTONOMOUS_TRANSACTION pragma
        if 'AUTONOMOUS_TRANSACTION' in result.upper():
            manual_review.append("AUTONOMOUS_TRANSACTION not supported - use separate procedure call")
            result = re.sub(
                r'PRAGMA\s+AUTONOMOUS_TRANSACTION\s*;',
                '-- PRAGMA AUTONOMOUS_TRANSACTION: Not supported. Use separate procedure.',
                result,
                flags=re.IGNORECASE
            )
        
        # Other PRAGMAs
        result = re.sub(
            r'PRAGMA\s+EXCEPTION_INIT\s*\([^)]+\)\s*;',
            '-- PRAGMA EXCEPTION_INIT removed (use SQLSTATE directly)',
            result,
            flags=re.IGNORECASE
        )
        result = re.sub(
            r'PRAGMA\s+RESTRICT_REFERENCES\s*\([^)]+\)\s*;',
            '-- PRAGMA RESTRICT_REFERENCES removed (not applicable)',
            result,
            flags=re.IGNORECASE
        )
        result = re.sub(
            r'PRAGMA\s+SERIALLY_REUSABLE\s*;',
            '-- PRAGMA SERIALLY_REUSABLE removed (not applicable)',
            result,
            flags=re.IGNORECASE
        )
        
        # REF CURSOR
        if 'REF CURSOR' in result.upper() or 'SYS_REFCURSOR' in result.upper():
            result = re.sub(
                r'\bSYS_REFCURSOR\b',
                'CURSOR  -- Converted from SYS_REFCURSOR',
                result,
                flags=re.IGNORECASE
            )
            result = re.sub(
                r'(\w+)\s+IS\s+REF\s+CURSOR\b',
                r'\1 CURSOR  -- Converted from REF CURSOR',
                result,
                flags=re.IGNORECASE
            )
            manual_review.append("REF CURSOR: Review cursor handling in Databricks")
        
        # OPEN cursor FOR SELECT
        result = re.sub(
            r'\bOPEN\s+(\w+)\s+FOR\s+(SELECT.+?)\s*;',
            r'-- Open cursor \1 for query\nDECLARE \1 CURSOR FOR \2;\nOPEN \1;',
            result,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        # Record type access: rec.field stays the same (Databricks supports struct.field)
        
        # Oracle sequence: seq.NEXTVAL -> No direct equivalent
        if '.NEXTVAL' in result.upper() or '.CURRVAL' in result.upper():
            result = re.sub(
                r'(\w+)\.NEXTVAL\b',
                r'-- TODO: \1.NEXTVAL - Use Identity column or UUID()',
                result,
                flags=re.IGNORECASE
            )
            result = re.sub(
                r'(\w+)\.CURRVAL\b',
                r'-- TODO: \1.CURRVAL - Track sequence value manually',
                result,
                flags=re.IGNORECASE
            )
            manual_review.append("Sequences need conversion to Identity columns or UUID()")
        
        # PL/SQL table type: TYPE ... IS TABLE OF
        if re.search(r'\bTYPE\s+\w+\s+IS\s+TABLE\s+OF\b', result, re.IGNORECASE):
            result = re.sub(
                r'\bTYPE\s+(\w+)\s+IS\s+TABLE\s+OF\s+(\w+(?:\.\w+)?(?:%\w+)?)\s*(?:INDEX\s+BY\s+\w+)?',
                r'-- TYPE \1: Use ARRAY<\2> for table types',
                result,
                flags=re.IGNORECASE
            )
            warnings.append("TABLE OF types converted to comments - use ARRAY<type>")
        
        # PL/SQL record type: TYPE ... IS RECORD
        if re.search(r'\bTYPE\s+\w+\s+IS\s+RECORD\b', result, re.IGNORECASE):
            result = re.sub(
                r'\bTYPE\s+(\w+)\s+IS\s+RECORD\s*\([^)]+\)\s*;',
                r'-- TYPE \1: Use STRUCT<field1 type1, field2 type2, ...> for record types',
                result,
                flags=re.IGNORECASE | re.DOTALL
            )
            manual_review.append("RECORD types need conversion to STRUCT<>")
        
        # VARRAY type
        if re.search(r'\bTYPE\s+\w+\s+IS\s+VARRAY\b', result, re.IGNORECASE):
            result = re.sub(
                r'\bTYPE\s+(\w+)\s+IS\s+VARRAY\s*\(\s*\d+\s*\)\s+OF\s+(\w+)',
                r'-- TYPE \1: Use ARRAY<\2> for VARRAY',
                result,
                flags=re.IGNORECASE
            )
            warnings.append("VARRAY types converted - use ARRAY<type>")
        
        return result
    
    def _translate_embedded_sql(self, code: str,
                                 warnings: List[str],
                                 manual_review: List[str]) -> str:
        """
        Translate embedded SQL statements within PL/SQL code using the SQL translator.
        
        This ensures that Oracle SQL features like CONNECT BY, (+) joins, DECODE,
        and other Oracle-specific SQL constructs are properly converted to Databricks SQL
        even when they appear within stored procedures, functions, or anonymous blocks.
        
        Handles:
        - SELECT statements (including subqueries)
        - INSERT statements
        - UPDATE statements  
        - DELETE statements
        - MERGE statements
        - Inline views and CTEs
        """
        result = code
        translator = OracleToDatabricksTranslator(pretty=False)
        
        # Pattern to find SQL DML statements that are standalone (end with ;)
        # We need to be careful not to translate SQL that's part of cursor declarations,
        # EXECUTE IMMEDIATE, or other constructs that need special handling
        
        # Find standalone SELECT statements (not SELECT INTO which is already handled)
        # Look for SELECT ... FROM ... ; patterns that are not part of SELECT INTO
        # Also translate SELECT statements in cursor declarations
        
        # Pattern for standalone DML statements
        dml_patterns = [
            # INSERT statements
            (r'(\bINSERT\s+INTO\s+[\w.]+\s*\([^)]*\)\s*(?:VALUES\s*\([^;]+\)|SELECT[^;]+);)',
             'INSERT'),
            # INSERT ... SELECT
            (r'(\bINSERT\s+INTO\s+[\w.]+\s+SELECT[^;]+;)',
             'INSERT SELECT'),
            # UPDATE statements
            (r'(\bUPDATE\s+[\w.]+\s+SET\s+[^;]+(?:WHERE[^;]+)?;)',
             'UPDATE'),
            # DELETE statements  
            (r'(\bDELETE\s+FROM\s+[\w.]+\s*(?:WHERE[^;]+)?;)',
             'DELETE'),
            # MERGE statements
            (r'(\bMERGE\s+INTO\s+[^;]+;)',
             'MERGE'),
        ]
        
        # Track if we made any translations
        translations_made = False
        
        for pattern, stmt_type in dml_patterns:
            matches = list(re.finditer(pattern, result, re.IGNORECASE | re.DOTALL))
            
            for match in reversed(matches):  # Reverse to maintain positions
                original_sql = match.group(1)
                # Remove trailing semicolon for translation
                sql_to_translate = original_sql.rstrip(';').strip()
                
                try:
                    # Translate the SQL statement
                    trans_result = translator.translate(sql_to_translate)
                    
                    if trans_result.success and trans_result.translated_sql:
                        translated_sql = trans_result.translated_sql.strip()
                        # Add back the semicolon
                        if not translated_sql.endswith(';'):
                            translated_sql += ';'
                        
                        # Only replace if translation is different
                        if translated_sql.lower() != original_sql.lower():
                            result = result[:match.start(1)] + translated_sql + result[match.end(1):]
                            translations_made = True
                            
                            # Add any warnings from the translation
                            for warning in trans_result.warnings:
                                if warning not in warnings:
                                    warnings.append(f"Embedded {stmt_type}: {warning}")
                except Exception:
                    # If translation fails, keep original
                    pass
        
        # Handle SELECT statements in cursor FOR loops and other contexts
        # Pattern: FOR rec IN (SELECT ...) LOOP or DO (DO after control flow conversion)
        cursor_for_pattern = re.compile(
            r'(\bFOR\s+\w+\s+IN\s*\(\s*)(SELECT\s+[^)]+)(\s*\)\s*(?:LOOP|DO)\b)',
            re.IGNORECASE | re.DOTALL
        )
        
        for match in reversed(list(cursor_for_pattern.finditer(result))):
            prefix = match.group(1)
            select_sql = match.group(2)
            suffix = match.group(3)
            
            try:
                trans_result = translator.translate(select_sql)
                if trans_result.success and trans_result.translated_sql:
                    translated_sql = trans_result.translated_sql.strip()
                    if translated_sql.lower() != select_sql.lower():
                        result = result[:match.start()] + prefix + translated_sql + suffix + result[match.end():]
                        translations_made = True
            except Exception:
                pass
        
        # Handle Oracle CURSOR ... IS SELECT ... and Databricks DECLARE cursor CURSOR FOR SELECT
        cursor_patterns = [
            # Oracle: CURSOR name IS SELECT
            re.compile(r'(\bCURSOR\s+\w+\s+IS\s+)(SELECT\s+[^;]+)(;)', re.IGNORECASE | re.DOTALL),
            # Databricks: DECLARE cursor_name CURSOR FOR SELECT
            re.compile(r'(\bDECLARE\s+(?:CURSOR\s+)?\w+\s+CURSOR\s+FOR\s+)(SELECT\s+[^;]+)(;)', re.IGNORECASE | re.DOTALL),
        ]
        
        for cursor_decl_pattern in cursor_patterns:
            for match in reversed(list(cursor_decl_pattern.finditer(result))):
                prefix = match.group(1)
                select_sql = match.group(2)
                suffix = match.group(3)
                
                try:
                    trans_result = translator.translate(select_sql)
                    if trans_result.success and trans_result.translated_sql:
                        translated_sql = trans_result.translated_sql.strip()
                        if translated_sql.lower() != select_sql.lower():
                            result = result[:match.start()] + prefix + translated_sql + suffix + result[match.end():]
                            translations_made = True
                except Exception:
                    pass
        
        # Handle SET var = (SELECT ...) - translate the SELECT inside
        set_select_pattern = re.compile(
            r'(\bSET\s+[\w.]+\s*=\s*\(\s*)(SELECT\s+[^)]+)(\s*\)\s*;)',
            re.IGNORECASE | re.DOTALL
        )
        
        for match in reversed(list(set_select_pattern.finditer(result))):
            prefix = match.group(1)
            select_sql = match.group(2)
            suffix = match.group(3)
            
            try:
                trans_result = translator.translate(select_sql)
                if trans_result.success and trans_result.translated_sql:
                    translated_sql = trans_result.translated_sql.strip()
                    if translated_sql.lower() != select_sql.lower():
                        result = result[:match.start()] + prefix + translated_sql + suffix + result[match.end():]
                        translations_made = True
            except Exception:
                pass
        
        # Handle standalone SELECT statements (not INTO, not in SET)
        # These might be in FOR EACH ROW triggers or other contexts
        standalone_select_pattern = re.compile(
            r'(?<![=(])\s*(SELECT\s+(?!.*\bINTO\b)[^;]+FROM\s+[^;]+;)',
            re.IGNORECASE | re.DOTALL
        )
        
        for match in reversed(list(standalone_select_pattern.finditer(result))):
            select_sql = match.group(1).strip()
            sql_to_translate = select_sql.rstrip(';').strip()
            
            try:
                trans_result = translator.translate(sql_to_translate)
                if trans_result.success and trans_result.translated_sql:
                    translated_sql = trans_result.translated_sql.strip()
                    if not translated_sql.endswith(';'):
                        translated_sql += ';'
                    if translated_sql.lower() != select_sql.lower():
                        result = result[:match.start(1)] + translated_sql + result[match.end(1):]
                        translations_made = True
            except Exception:
                pass
        
        if translations_made:
            warnings.append("Embedded SQL statements translated to Databricks SQL")
        
        return result
    
    def _extract_package_members(self, code: str) -> List[Tuple[str, str]]:
        """Extract procedures and functions from package body."""
        members = []
        
        # Extract procedures
        proc_pattern = re.compile(
            r'(PROCEDURE\s+\w+.*?END\s+\w*\s*;)',
            re.IGNORECASE | re.DOTALL
        )
        
        # Extract functions
        func_pattern = re.compile(
            r'(FUNCTION\s+\w+.*?END\s+\w*\s*;)',
            re.IGNORECASE | re.DOTALL
        )
        
        for match in proc_pattern.finditer(code):
            members.append((f"CREATE OR REPLACE {match.group(1)}", "PROCEDURE"))
        
        for match in func_pattern.finditer(code):
            members.append((f"CREATE OR REPLACE {match.group(1)}", "FUNCTION"))
        
        return members
    
    def _split_plsql_objects(self, content: str) -> List[str]:
        """
        Split a file into individual PL/SQL objects.
        
        Handles:
        - Oracle SQL*Plus / terminator between objects
        - Package specifications and bodies
        - Procedures, functions, triggers
        - Anonymous blocks (DECLARE/BEGIN)
        """
        objects = []
        
        # Strip comments for analysis but keep original for splitting
        content_without_comments = strip_sql_comments(content)
        
        # Check if content is empty after stripping comments
        if not content_without_comments.strip():
            return objects
        
        # Check if the content starts with DECLARE or BEGIN (anonymous block)
        content_upper = content_without_comments.strip().upper()
        is_anonymous_block = (
            content_upper.startswith('DECLARE') or 
            content_upper.startswith('BEGIN')
        )
        
        if is_anonymous_block:
            # Check if there are no CREATE statements - treat as single anonymous block
            if not re.search(r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION|PACKAGE|TRIGGER)\b', 
                           content_without_comments, flags=re.IGNORECASE):
                # Remove trailing / if present
                content = re.sub(r'\s*/\s*$', '', content.strip())
                objects.append(content)
                return objects
        
        # Split on / (SQL*Plus terminator) that appears on its own line
        # This is the proper way to separate PL/SQL blocks
        parts = re.split(r'\n\s*/\s*\n', content)
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            # Remove trailing / if present
            part = re.sub(r'\s*/\s*$', '', part)
            
            # Strip comments to check for actual content
            part_without_comments = strip_sql_comments(part)
            part_upper = part_without_comments.upper().strip()
            
            # Skip empty parts
            if not part_upper:
                continue
            
            # Check if this part has actual PL/SQL content
            has_plsql_content = (
                re.search(r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION|PACKAGE|TRIGGER)\b', 
                         part_upper) or
                part_upper.startswith('DECLARE') or
                part_upper.startswith('BEGIN')
            )
            
            if has_plsql_content:
                objects.append(part)
        
        # If no / separators were found, try splitting on CREATE statements
        if len(objects) <= 1 and len(parts) == 1:
            # Try to split on CREATE statements
            create_parts = re.split(
                r'(?=\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION|PACKAGE|TRIGGER)\b)', 
                content, 
                flags=re.IGNORECASE
            )
            
            objects = []
            for part in create_parts:
                part = part.strip()
                if part:
                    # Remove trailing /
                    part = re.sub(r'\s*/\s*$', '', part)
                    
                    part_without_comments = strip_sql_comments(part)
                    part_upper = part_without_comments.upper().strip()
                    
                    has_plsql_content = (
                        re.search(r'\bCREATE\s+', part_upper) or
                        part_upper.startswith('DECLARE') or
                        part_upper.startswith('BEGIN')
                    )
                    
                    if has_plsql_content:
                        objects.append(part)
        
        return objects
    
    def _split_params(self, params_str: str) -> List[str]:
        """Split parameter string handling nested parentheses."""
        params = []
        current = []
        depth = 0
        
        for char in params_str:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                params.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        
        if current:
            params.append(''.join(current).strip())
        
        return params
    
    def _indent(self, text: str, spaces: int) -> str:
        """Indent text by specified number of spaces."""
        indent = ' ' * spaces
        lines = text.split('\n')
        return '\n'.join(indent + line if line.strip() else line for line in lines)

