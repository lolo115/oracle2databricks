"""
Function detection and analysis utilities for Oracle to Databricks SQL translation.

This module provides utilities to detect and analyze Oracle SQL functions,
packages, and constructs within SQL statements.
"""

import re
from typing import Set, Tuple, Dict, List, Optional

from oracle2databricks.function_mappings import DIRECT_FUNCTION_MAPPINGS


def get_line_number(text: str, position: int) -> int:
    """
    Get the line number (1-based) for a character position in text.
    
    Args:
        text: The full text
        position: Character position (0-based)
        
    Returns:
        Line number (1-based)
    """
    return text[:position].count('\n') + 1


class FunctionDetector:
    """
    Utility class for detecting Oracle functions, packages, and constructs in SQL.
    
    This class provides static methods to analyze SQL statements and identify:
    - Known Oracle functions
    - All function calls (including custom/internal)
    - Unknown/custom functions
    - Functions with no Databricks equivalent
    - Unsupported Oracle constructs
    
    Example usage:
        sql = "SELECT SYSDATE, NVL(col, 0) FROM DUAL"
        oracle_funcs = FunctionDetector.detect_oracle_functions(sql)
        # Returns: {'SYSDATE', 'NVL'}
    """
    
    # SQL keywords and constructs that look like functions but aren't
    SQL_KEYWORDS = {
        'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN',
        'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'TABLE', 'VIEW',
        'INDEX', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'TRIGGER', 'TYPE', 'BODY',
        'BEGIN', 'DECLARE', 'EXCEPTION', 'LOOP', 'FOR', 'WHILE', 'IF', 'ELSIF',
        'RETURN', 'RETURNING', 'INTO', 'VALUES', 'SET', 'AS', 'IS', 'WITH',
        'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'NATURAL', 'ON',
        'GROUP', 'ORDER', 'BY', 'HAVING', 'UNION', 'INTERSECT', 'MINUS', 'EXCEPT',
        'FETCH', 'FIRST', 'NEXT', 'ONLY', 'ROWS', 'ROW', 'PERCENT', 'OFFSET',
        'PARTITION', 'OVER', 'WINDOW', 'RANGE', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING',
        'CURRENT', 'NULLS', 'LAST', 'ASC', 'DESC', 'DISTINCT', 'ALL', 'ANY', 'SOME',
        'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'CONSTRAINT', 'UNIQUE', 'CHECK',
        'DEFAULT', 'USING', 'MERGE', 'MATCHED', 'CONNECT', 'START', 'PRIOR', 'LEVEL',
        'PIVOT', 'UNPIVOT', 'MODEL', 'DIMENSION', 'MEASURES', 'RULES', 'ITERATE',
        'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'GRANT', 'REVOKE', 'EXECUTE', 'IMMEDIATE',
        'CURSOR', 'OPEN', 'CLOSE', 'BULK', 'COLLECT', 'FORALL', 'LIMIT', 'SAVE',
        'EXCEPTIONS', 'RAISE', 'PRAGMA', 'AUTONOMOUS_TRANSACTION', 'SERIALLY_REUSABLE',
        'RESTRICT_REFERENCES', 'INTERFACE', 'EXCEPTION_INIT', 'INLINE',
    }
    
    # Data types that might appear with parentheses
    DATA_TYPES = {
        'VARCHAR', 'VARCHAR2', 'NVARCHAR', 'NVARCHAR2', 'CHAR', 'NCHAR',
        'NUMBER', 'NUMERIC', 'DECIMAL', 'DEC', 'INTEGER', 'INT', 'SMALLINT',
        'FLOAT', 'REAL', 'DOUBLE', 'BINARY_FLOAT', 'BINARY_DOUBLE',
        'DATE', 'TIMESTAMP', 'INTERVAL', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
        'CLOB', 'NCLOB', 'BLOB', 'BFILE', 'RAW', 'LONG', 'ROWID', 'UROWID',
        'BOOLEAN', 'PLS_INTEGER', 'BINARY_INTEGER', 'NATURAL', 'NATURALN',
        'POSITIVE', 'POSITIVEN', 'SIGNTYPE', 'SIMPLE_INTEGER',
        'STRING', 'BIGINT', 'TINYINT', 'ARRAY', 'MAP', 'STRUCT',
    }
    
    # Known Databricks/Spark SQL built-in functions
    KNOWN_DATABRICKS_FUNCTIONS = {
        # Spark SQL built-in functions
        'ABS', 'ACOS', 'ACOSH', 'ADD_MONTHS', 'AGGREGATE', 'AND', 'ANY', 'APPROX_COUNT_DISTINCT',
        'APPROX_PERCENTILE', 'ARRAY', 'ARRAY_AGG', 'ARRAY_APPEND', 'ARRAY_COMPACT', 'ARRAY_CONTAINS',
        'ARRAY_DISTINCT', 'ARRAY_EXCEPT', 'ARRAY_INSERT', 'ARRAY_INTERSECT', 'ARRAY_JOIN',
        'ARRAY_MAX', 'ARRAY_MIN', 'ARRAY_POSITION', 'ARRAY_PREPEND', 'ARRAY_REMOVE',
        'ARRAY_REPEAT', 'ARRAY_SIZE', 'ARRAY_SORT', 'ARRAY_UNION', 'ARRAYS_OVERLAP',
        'ARRAYS_ZIP', 'ASCII', 'ASIN', 'ASINH', 'ASSERT_TRUE', 'ATAN', 'ATAN2', 'ATANH',
        'AVG', 'BASE64', 'BETWEEN', 'BIGINT', 'BIN', 'BINARY', 'BIT_AND', 'BIT_COUNT',
        'BIT_GET', 'BIT_LENGTH', 'BIT_OR', 'BIT_XOR', 'BOOL_AND', 'BOOL_OR', 'BOOLEAN',
        'BROUND', 'BTRIM', 'CARDINALITY', 'CASE', 'CAST', 'CBRT', 'CEIL', 'CEILING',
        'CHAR', 'CHAR_LENGTH', 'CHARACTER_LENGTH', 'CHR', 'COALESCE', 'COLLECT_LIST',
        'COLLECT_SET', 'CONCAT', 'CONCAT_WS', 'CONTAINS', 'CONV', 'CORR', 'COS', 'COSH',
        'COT', 'COUNT', 'COUNT_IF', 'COUNT_MIN_SKETCH', 'COVAR_POP', 'COVAR_SAMP',
        'CRC32', 'CUBE', 'CUME_DIST', 'CURRENT_CATALOG', 'CURRENT_DATABASE', 'CURRENT_DATE',
        'CURRENT_SCHEMA', 'CURRENT_TIMESTAMP', 'CURRENT_TIMEZONE', 'CURRENT_USER',
        'DATE', 'DATE_ADD', 'DATE_DIFF', 'DATE_FORMAT', 'DATE_FROM_UNIX_DATE',
        'DATE_PART', 'DATE_SUB', 'DATE_TRUNC', 'DATEADD', 'DATEDIFF', 'DATEPART',
        'DAY', 'DAYNAME', 'DAYOFMONTH', 'DAYOFWEEK', 'DAYOFYEAR', 'DECIMAL', 'DECODE',
        'DEGREES', 'DENSE_RANK', 'DIV', 'DOUBLE', 'E', 'ELEMENT_AT', 'ELT', 'ENCODE',
        'ENDSWITH', 'EQUAL_NULL', 'EVERY', 'EXISTS', 'EXP', 'EXPLODE', 'EXPLODE_OUTER',
        'EXPM1', 'EXTRACT', 'FACTORIAL', 'FILTER', 'FIND_IN_SET', 'FIRST', 'FIRST_VALUE',
        'FLATTEN', 'FLOAT', 'FLOOR', 'FORALL', 'FORMAT_NUMBER', 'FORMAT_STRING',
        'FROM_CSV', 'FROM_JSON', 'FROM_UNIXTIME', 'FROM_UTC_TIMESTAMP', 'GET_JSON_OBJECT',
        'GETBIT', 'GREATEST', 'GROUPING', 'GROUPING_ID', 'HASH', 'HEX', 'HOUR',
        'HYPOT', 'IF', 'IFNULL', 'IIF', 'IN', 'INITCAP', 'INLINE', 'INLINE_OUTER',
        'INPUT_FILE_BLOCK_LENGTH', 'INPUT_FILE_BLOCK_START', 'INPUT_FILE_NAME',
        'INSTR', 'INT', 'ISNAN', 'ISNOTNULL', 'ISNULL', 'JAVA_METHOD', 'JSON_ARRAY_LENGTH',
        'JSON_OBJECT_KEYS', 'JSON_TUPLE', 'KURTOSIS', 'LAG', 'LAST', 'LAST_DAY',
        'LAST_VALUE', 'LCASE', 'LEAD', 'LEAST', 'LEFT', 'LENGTH', 'LEVENSHTEIN',
        'LIKE', 'LN', 'LOCATE', 'LOG', 'LOG10', 'LOG1P', 'LOG2', 'LOWER', 'LPAD',
        'LTRIM', 'MAKE_DATE', 'MAKE_DT_INTERVAL', 'MAKE_INTERVAL', 'MAKE_TIMESTAMP',
        'MAKE_TIMESTAMP_LTZ', 'MAKE_TIMESTAMP_NTZ', 'MAKE_YM_INTERVAL', 'MAP',
        'MAP_CONCAT', 'MAP_CONTAINS_KEY', 'MAP_ENTRIES', 'MAP_FILTER', 'MAP_FROM_ARRAYS',
        'MAP_FROM_ENTRIES', 'MAP_KEYS', 'MAP_VALUES', 'MAP_ZIP_WITH', 'MAX', 'MAX_BY',
        'MD5', 'MEAN', 'MIN', 'MIN_BY', 'MINUTE', 'MOD', 'MODE', 'MONOTONICALLY_INCREASING_ID',
        'MONTH', 'MONTHS_BETWEEN', 'NAMED_STRUCT', 'NANVL', 'NEGATIVE', 'NEXT_DAY',
        'NOT', 'NOW', 'NTH_VALUE', 'NTILE', 'NULLIF', 'NVL', 'NVL2', 'OCTET_LENGTH',
        'OR', 'OVERLAY', 'PARSE_URL', 'PERCENT_RANK', 'PERCENTILE', 'PERCENTILE_APPROX',
        'PERCENTILE_CONT', 'PERCENTILE_DISC', 'PI', 'PMOD', 'POSEXPLODE', 'POSEXPLODE_OUTER',
        'POSITION', 'POSITIVE', 'POW', 'POWER', 'PRINTF', 'QUARTER', 'RADIANS',
        'RAISE_ERROR', 'RAND', 'RANDN', 'RANDOM', 'RANK', 'REDUCE', 'REFLECT',
        'REGEXP', 'REGEXP_COUNT', 'REGEXP_EXTRACT', 'REGEXP_EXTRACT_ALL', 'REGEXP_INSTR',
        'REGEXP_LIKE', 'REGEXP_REPLACE', 'REGEXP_SUBSTR', 'REGR_AVGX', 'REGR_AVGY',
        'REGR_COUNT', 'REGR_INTERCEPT', 'REGR_R2', 'REGR_SLOPE', 'REGR_SXX',
        'REGR_SXY', 'REGR_SYY', 'REPEAT', 'REPLACE', 'REVERSE', 'RIGHT', 'RINT',
        'RLIKE', 'ROLLUP', 'ROUND', 'ROW', 'ROW_NUMBER', 'RPAD', 'RTRIM', 'SCHEMA_OF_CSV',
        'SCHEMA_OF_JSON', 'SEC', 'SECOND', 'SENTENCES', 'SEQUENCE', 'SESSION_USER',
        'SHA', 'SHA1', 'SHA2', 'SHIFTLEFT', 'SHIFTRIGHT', 'SHIFTRIGHTUNSIGNED',
        'SHUFFLE', 'SIGN', 'SIGNUM', 'SIN', 'SINH', 'SIZE', 'SKEWNESS', 'SLICE',
        'SMALLINT', 'SOME', 'SORT_ARRAY', 'SOUNDEX', 'SPACE', 'SPARK_PARTITION_ID',
        'SPLIT', 'SPLIT_PART', 'SQRT', 'STACK', 'STARTSWITH', 'STD', 'STDDEV',
        'STDDEV_POP', 'STDDEV_SAMP', 'STR_TO_MAP', 'STRING', 'STRUCT', 'SUBSTR',
        'SUBSTRING', 'SUBSTRING_INDEX', 'SUM', 'TAN', 'TANH', 'TIMESTAMP',
        'TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS', 'TIMESTAMP_SECONDS', 'TINYINT',
        'TO_BINARY', 'TO_CHAR', 'TO_CSV', 'TO_DATE', 'TO_JSON', 'TO_NUMBER',
        'TO_TIMESTAMP', 'TO_UNIX_TIMESTAMP', 'TO_UTC_TIMESTAMP', 'TRANSFORM',
        'TRANSFORM_KEYS', 'TRANSFORM_VALUES', 'TRANSLATE', 'TRIM', 'TRUNC',
        'TRY_ADD', 'TRY_AVG', 'TRY_CAST', 'TRY_DIVIDE', 'TRY_ELEMENT_AT',
        'TRY_MULTIPLY', 'TRY_SUBTRACT', 'TRY_SUM', 'TRY_TO_BINARY', 'TRY_TO_NUMBER',
        'TRY_TO_TIMESTAMP', 'TYPEOF', 'UCASE', 'UNBASE64', 'UNHEX', 'UNIX_DATE',
        'UNIX_MICROS', 'UNIX_MILLIS', 'UNIX_SECONDS', 'UNIX_TIMESTAMP', 'UPPER',
        'UUID', 'VAR_POP', 'VAR_SAMP', 'VARIANCE', 'VERSION', 'WEEKDAY', 'WEEKOFYEAR',
        'WHEN', 'WIDTH_BUCKET', 'WINDOW', 'XPATH', 'XPATH_BOOLEAN', 'XPATH_DOUBLE',
        'XPATH_FLOAT', 'XPATH_INT', 'XPATH_LONG', 'XPATH_NUMBER', 'XPATH_SHORT',
        'XPATH_STRING', 'XXHASH64', 'YEAR', 'ZIP_WITH',
    }
    
    # Suggestions for Oracle functions/packages with no direct equivalent
    EQUIVALENT_SUGGESTIONS = {
        # Functions with no equivalent
        'ROWID': 'Use a unique identifier column or ROW_NUMBER() window function',
        'UID': 'Use CURRENT_USER() for user identification',
        'USERENV': 'Use CURRENT_USER(), CURRENT_SCHEMA(), or CURRENT_CATALOG()',
        'SYS_CONTEXT': 'Use Databricks session variables or CURRENT_* functions',
        'NLSSORT': 'Use COLLATE clause or custom sort functions',
        'COMPOSE': 'Use Spark UDF for Unicode normalization',
        'DECOMPOSE': 'Use Spark UDF for Unicode decomposition',
        'CHARTOROWID': 'No equivalent - redesign using unique keys',
        'ROWIDTOCHAR': 'No equivalent - redesign using unique keys',
        'BFILENAME': 'Use Unity Catalog volumes or cloud storage paths',
        'EMPTY_BLOB': "Use CAST('' AS BINARY)",
        'EMPTY_CLOB': "Use ''",
        
        # Common DBMS packages
        'DBMS_OUTPUT.PUT_LINE': 'Use Python print() in notebooks',
        'DBMS_OUTPUT.PUT': 'Use Python print() in notebooks',
        'DBMS_LOB': 'Use Spark SQL string/binary functions',
        'DBMS_SQL': 'Use dynamic SQL with spark.sql()',
        'DBMS_JOB': 'Use Databricks Workflows or Jobs',
        'DBMS_SCHEDULER': 'Use Databricks Workflows or Jobs',
        'DBMS_LOCK': 'Use Delta Lake optimistic concurrency',
        'DBMS_RANDOM': 'Use RAND(), RANDN(), or UUID()',
        'DBMS_CRYPTO': 'Use SHA2(), MD5(), or AES_ENCRYPT/DECRYPT',
        'DBMS_XMLGEN': 'Use TO_XML() or construct XML manually',
        'DBMS_XMLDOM': 'Use Spark XML functions',
        'DBMS_UTILITY': 'Implement specific functionality as needed',
        'DBMS_SESSION': 'Use Databricks session management',
        'DBMS_METADATA': 'Use DESCRIBE or information_schema',
        'DBMS_STATS': 'Use ANALYZE TABLE command',
        'DBMS_FLASHBACK': 'Use Delta Lake time travel',
        'DBMS_APPLICATION_INFO': 'Use Databricks tags or comments',
        
        # UTL packages
        'UTL_FILE': 'Use Databricks dbutils.fs or cloud storage APIs',
        'UTL_HTTP': 'Use Python requests library in notebooks',
        'UTL_SMTP': 'Use external email service APIs',
        'UTL_MAIL': 'Use external email service APIs',
        'UTL_TCP': 'Use Python socket library in notebooks',
        'UTL_RAW': 'Use HEX(), UNHEX(), ENCODE(), DECODE()',
        'UTL_ENCODE': 'Use BASE64(), UNBASE64()',
        'UTL_COMPRESS': 'Use external compression libraries',
        
        # Other packages
        'CTX_': 'Use Databricks full-text search or Delta Lake',
        'SDO_': 'Use H3 library or GeoSpark for spatial operations',
        'APEX_': 'Redesign using Databricks Apps or notebooks',
    }
    
    # Unsupported Oracle constructs patterns
    UNSUPPORTED_CONSTRUCTS = {
        'CONNECT BY': r'\bCONNECT\s+BY\b',
        'START WITH': r'\bSTART\s+WITH\b',
        'MODEL clause': r'\bMODEL\s+',
        'PIVOT': r'\bPIVOT\s*\(',
        'UNPIVOT': r'\bUNPIVOT\s*\(',
        'FLASHBACK (AS OF)': r'\bAS\s+OF\s+(TIMESTAMP|SCN)\b',
        'VERSIONS BETWEEN': r'\bVERSIONS\s+BETWEEN\b',
        'Oracle hints': r'/\*\+',
        'ROWID': r'\bROWID\b',
        'Sequences (.NEXTVAL/.CURRVAL)': r'\.\s*(NEXTVAL|CURRVAL)\b',
        'PRAGMA AUTONOMOUS_TRANSACTION': r'\bPRAGMA\s+AUTONOMOUS_TRANSACTION\b',
        'KEEP (DENSE_RANK FIRST/LAST)': r'\bKEEP\s*\(\s*DENSE_RANK\b',
        'SAMPLE clause': r'\bSAMPLE\s*\(',
        'DBMS_* packages': r'\bDBMS_\w+\.\w+',
        'UTL_* packages': r'\bUTL_\w+\.\w+',
        'XMLType operations': r'\b(XMLTYPE|XMLELEMENT|XMLFOREST|XMLAGG)\b',
        'Oracle (+) outer join': r'\(\+\)',
        'REF/DEREF operations': r'\b(REF|DEREF)\s*\(',
        'TREAT function': r'\bTREAT\s*\(',
        'TABLE() collection': r'\bTABLE\s*\(\s*\w+',
    }
    
    # Oracle package patterns for detection
    ORACLE_PACKAGE_PATTERNS = [
        r'\b(DBMS_\w+)\.(\w+)',     # DBMS_* packages
        r'\b(UTL_\w+)\.(\w+)',      # UTL_* packages
        r'\b(CTX_\w+)\.(\w+)',      # Oracle Text
        r'\b(SDO_\w+)\.(\w+)',      # Spatial
        r'\b(OWA_\w+)\.(\w+)',      # PL/SQL Web Toolkit
        r'\b(HTP)\.(\w+)',          # HTTP package
        r'\b(HTF)\.(\w+)',          # HTML functions
        r'\b(WPG_DOCLOAD)\.(\w+)',  # Document loading
        r'\b(APEX_\w+)\.(\w+)',     # APEX packages
    ]
    
    @staticmethod
    def detect_oracle_functions(sql: str) -> Set[str]:
        """
        Detect known Oracle functions used in SQL statement.
        
        Args:
            sql: SQL statement to analyze
            
        Returns:
            Set of Oracle function names found
        """
        functions_found = set()
        sql_upper = sql.upper()
        
        # Check for each known Oracle function
        for func_name in DIRECT_FUNCTION_MAPPINGS.keys():
            # Match function name followed by ( or as standalone keyword
            pattern = r'\b' + re.escape(func_name) + r'\s*\('
            if re.search(pattern, sql_upper):
                functions_found.add(func_name)
            
            # Special case for functions that can appear without parentheses
            if func_name in ('SYSDATE', 'SYSTIMESTAMP', 'ROWNUM', 'ROWID', 'USER', 'UID'):
                if re.search(r'\b' + re.escape(func_name) + r'\b', sql_upper):
                    functions_found.add(func_name)
        
        return functions_found
    
    @classmethod
    def detect_all_function_calls(cls, sql: str) -> Set[str]:
        """
        Detect ALL function calls in SQL (not just known Oracle functions).
        
        This helps identify custom/internal functions that are not standard Oracle functions.
        
        Args:
            sql: SQL statement to analyze
            
        Returns:
            Set of all function names found in the SQL
        """
        functions_found = set()
        
        # Pattern to match function calls: FUNCTION_NAME(
        # Excludes SQL keywords, common table operations, and data types
        func_pattern = r'\b([A-Za-z_][A-Za-z0-9_$#]*)\s*\('
        
        excluded = cls.SQL_KEYWORDS | cls.DATA_TYPES
        
        for match in re.finditer(func_pattern, sql, re.IGNORECASE):
            func_name = match.group(1).upper()
            if func_name not in excluded and len(func_name) > 1:
                functions_found.add(func_name)
        
        return functions_found
    
    @classmethod
    def detect_unknown_functions(cls, sql: str) -> Set[str]:
        """
        Detect functions that are NOT known Oracle or Databricks standard functions.
        
        These are likely custom/internal functions that will need to be migrated.
        
        Args:
            sql: SQL statement to analyze
            
        Returns:
            Set of unknown function names
        """
        all_functions = cls.detect_all_function_calls(sql)
        
        # Known Oracle functions (from our mappings)
        known_oracle = set(DIRECT_FUNCTION_MAPPINGS.keys())
        
        # Combine all known functions
        all_known = known_oracle | cls.KNOWN_DATABRICKS_FUNCTIONS
        
        # Find functions that are not known
        unknown = set()
        for func in all_functions:
            if func not in all_known:
                unknown.add(func)
        
        return unknown
    
    @classmethod
    def detect_functions_no_equivalent(cls, sql: str) -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Detect Oracle functions and packages that have no Databricks equivalent,
        plus unknown/custom functions.
        
        Args:
            sql: SQL statement to analyze
            
        Returns:
            Tuple of (functions_no_equiv, packages_no_equiv, unknown_functions)
        """
        functions_no_equiv = set()
        packages_no_equiv = set()
        sql_upper = sql.upper()
        
        # Check for functions mapped to None (no equivalent)
        for func_name, mapping in DIRECT_FUNCTION_MAPPINGS.items():
            if mapping is None:  # No Databricks equivalent
                pattern = r'\b' + re.escape(func_name) + r'\s*\('
                if re.search(pattern, sql_upper):
                    functions_no_equiv.add(func_name)
                
                # Special case for functions without parentheses
                if func_name in ('ROWID', 'UID'):
                    if re.search(r'\b' + re.escape(func_name) + r'\b', sql_upper):
                        functions_no_equiv.add(func_name)
        
        # Detect Oracle package calls
        for pkg_pattern in cls.ORACLE_PACKAGE_PATTERNS:
            for match in re.finditer(pkg_pattern, sql, re.IGNORECASE):
                package_name = match.group(1).upper()
                proc_name = match.group(2).upper()
                full_name = f"{package_name}.{proc_name}"
                packages_no_equiv.add(full_name)
        
        # Detect unknown/custom functions
        unknown_functions = cls.detect_unknown_functions(sql)
        
        return functions_no_equiv, packages_no_equiv, unknown_functions
    
    @classmethod
    def get_equivalent_suggestion(cls, func_or_pkg: str) -> str:
        """
        Get a suggestion for an Oracle function/package with no direct equivalent.
        
        Args:
            func_or_pkg: Function or package name
            
        Returns:
            Suggestion string for migration
        """
        # Check for exact match
        if func_or_pkg in cls.EQUIVALENT_SUGGESTIONS:
            return cls.EQUIVALENT_SUGGESTIONS[func_or_pkg]
        
        # Check for partial match (package prefix)
        for prefix, suggestion in cls.EQUIVALENT_SUGGESTIONS.items():
            if func_or_pkg.startswith(prefix):
                return suggestion
        
        return 'No direct equivalent - requires manual implementation'
    
    @classmethod
    def detect_unsupported_constructs(cls, sql: str) -> Set[str]:
        """
        Detect unsupported Oracle constructs.
        
        Args:
            sql: SQL statement to analyze
            
        Returns:
            Set of unsupported construct names found
        """
        unsupported = set()
        sql_upper = sql.upper()
        
        for name, pattern in cls.UNSUPPORTED_CONSTRUCTS.items():
            if re.search(pattern, sql_upper if 'DBMS_' not in name else sql):
                unsupported.add(name)
        
        return unsupported
    
    @classmethod
    def analyze_sql(cls, sql: str) -> Dict[str, Set[str]]:
        """
        Perform comprehensive analysis of a SQL statement.
        
        Args:
            sql: SQL statement to analyze
            
        Returns:
            Dictionary with analysis results:
                - oracle_functions: Known Oracle functions found
                - all_functions: All function calls found
                - unknown_functions: Functions not in known lists
                - functions_no_equivalent: Oracle functions with no Databricks equivalent
                - packages_no_equivalent: Oracle packages with no Databricks equivalent
                - unsupported_constructs: Unsupported Oracle constructs found
        """
        funcs_no_equiv, pkgs_no_equiv, unknown_funcs = cls.detect_functions_no_equivalent(sql)
        
        return {
            'oracle_functions': cls.detect_oracle_functions(sql),
            'all_functions': cls.detect_all_function_calls(sql),
            'unknown_functions': unknown_funcs,
            'functions_no_equivalent': funcs_no_equiv,
            'packages_no_equivalent': pkgs_no_equiv,
            'unsupported_constructs': cls.detect_unsupported_constructs(sql),
        }
    
    @classmethod
    def detect_unsupported_constructs_with_lines(cls, sql: str, base_line: int = 1) -> Dict[str, List[int]]:
        """
        Detect unsupported Oracle constructs with their line numbers.
        
        Args:
            sql: SQL statement to analyze
            base_line: Base line number to add to detected line numbers (for file context)
            
        Returns:
            Dictionary mapping construct names to list of line numbers where found
        """
        unsupported = {}
        sql_upper = sql.upper()
        
        for name, pattern in cls.UNSUPPORTED_CONSTRUCTS.items():
            search_text = sql_upper if 'DBMS_' not in name and 'UTL_' not in name else sql
            for match in re.finditer(pattern, search_text, re.IGNORECASE):
                line_num = get_line_number(sql, match.start()) + base_line - 1
                if name not in unsupported:
                    unsupported[name] = []
                if line_num not in unsupported[name]:
                    unsupported[name].append(line_num)
        
        return unsupported
    
    @classmethod
    def detect_unknown_functions_with_lines(cls, sql: str, base_line: int = 1) -> Dict[str, List[int]]:
        """
        Detect unknown/custom functions with their line numbers.
        
        Args:
            sql: SQL statement to analyze
            base_line: Base line number to add to detected line numbers
            
        Returns:
            Dictionary mapping function names to list of line numbers where found
        """
        unknown_with_lines = {}
        
        # Pattern to match function calls: FUNCTION_NAME(
        func_pattern = r'\b([A-Za-z_][A-Za-z0-9_$#]*)\s*\('
        
        excluded = cls.SQL_KEYWORDS | cls.DATA_TYPES
        known_oracle = set(DIRECT_FUNCTION_MAPPINGS.keys())
        all_known = known_oracle | cls.KNOWN_DATABRICKS_FUNCTIONS
        
        for match in re.finditer(func_pattern, sql, re.IGNORECASE):
            func_name = match.group(1).upper()
            if func_name not in excluded and len(func_name) > 1 and func_name not in all_known:
                line_num = get_line_number(sql, match.start()) + base_line - 1
                if func_name not in unknown_with_lines:
                    unknown_with_lines[func_name] = []
                if line_num not in unknown_with_lines[func_name]:
                    unknown_with_lines[func_name].append(line_num)
        
        return unknown_with_lines
    
    @classmethod
    def detect_functions_no_equivalent_with_lines(cls, sql: str, base_line: int = 1) -> Tuple[Dict[str, List[int]], Dict[str, List[int]], Dict[str, List[int]]]:
        """
        Detect Oracle functions and packages with no Databricks equivalent, with line numbers.
        
        Args:
            sql: SQL statement to analyze
            base_line: Base line number to add to detected line numbers
            
        Returns:
            Tuple of (functions_no_equiv, packages_no_equiv, unknown_functions)
            Each is a dictionary mapping names to lists of line numbers
        """
        functions_no_equiv = {}
        packages_no_equiv = {}
        sql_upper = sql.upper()
        
        # Check for functions mapped to None (no equivalent)
        for func_name, mapping in DIRECT_FUNCTION_MAPPINGS.items():
            if mapping is None:
                pattern = r'\b' + re.escape(func_name) + r'\s*\('
                for match in re.finditer(pattern, sql_upper):
                    line_num = get_line_number(sql, match.start()) + base_line - 1
                    if func_name not in functions_no_equiv:
                        functions_no_equiv[func_name] = []
                    if line_num not in functions_no_equiv[func_name]:
                        functions_no_equiv[func_name].append(line_num)
                
                # Special case for functions without parentheses
                if func_name in ('ROWID', 'UID'):
                    pattern = r'\b' + re.escape(func_name) + r'\b'
                    for match in re.finditer(pattern, sql_upper):
                        line_num = get_line_number(sql, match.start()) + base_line - 1
                        if func_name not in functions_no_equiv:
                            functions_no_equiv[func_name] = []
                        if line_num not in functions_no_equiv[func_name]:
                            functions_no_equiv[func_name].append(line_num)
        
        # Detect Oracle package calls
        for pkg_pattern in cls.ORACLE_PACKAGE_PATTERNS:
            for match in re.finditer(pkg_pattern, sql, re.IGNORECASE):
                package_name = match.group(1).upper()
                proc_name = match.group(2).upper()
                full_name = f"{package_name}.{proc_name}"
                line_num = get_line_number(sql, match.start()) + base_line - 1
                if full_name not in packages_no_equiv:
                    packages_no_equiv[full_name] = []
                if line_num not in packages_no_equiv[full_name]:
                    packages_no_equiv[full_name].append(line_num)
        
        # Detect unknown/custom functions
        unknown_functions = cls.detect_unknown_functions_with_lines(sql, base_line)
        
        return functions_no_equiv, packages_no_equiv, unknown_functions

