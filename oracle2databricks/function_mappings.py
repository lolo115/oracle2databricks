"""
Oracle to Databricks function mappings.

This module contains comprehensive mappings for Oracle-specific functions
to their Databricks SQL equivalents.
"""

# Oracle functions that map directly to Databricks equivalents
DIRECT_FUNCTION_MAPPINGS = {
    # ==========================================
    # STRING FUNCTIONS
    # ==========================================
    "NVL": "COALESCE",
    "NVL2": "NVL2",  # Databricks supports NVL2
    "DECODE": "DECODE",  # Handled specially → CASE
    "SUBSTR": "SUBSTRING",
    "SUBSTRB": "SUBSTRING",  # Byte-based → character-based
    "INSTR": "INSTR",  # Databricks supports INSTR
    "INSTRB": "INSTR",  # Byte-based → character-based
    "LENGTH": "LENGTH",
    "LENGTHB": "LENGTH",  # Byte length → character length
    "LENGTHC": "LENGTH",  # Unicode char length
    "LENGTH2": "LENGTH",  # UCS2 length
    "LENGTH4": "LENGTH",  # UCS4 length
    "UPPER": "UPPER",
    "LOWER": "LOWER",
    "INITCAP": "INITCAP",
    "TRIM": "TRIM",
    "LTRIM": "LTRIM",
    "RTRIM": "RTRIM",
    "LPAD": "LPAD",
    "RPAD": "RPAD",
    "REPLACE": "REPLACE",
    "TRANSLATE": "TRANSLATE",
    "CONCAT": "CONCAT",
    "CHR": "CHR",
    "ASCII": "ASCII",
    "REVERSE": "REVERSE",
    "SOUNDEX": "SOUNDEX",
    "REGEXP_REPLACE": "REGEXP_REPLACE",
    "REGEXP_SUBSTR": "REGEXP_EXTRACT",  # Handled specially
    "REGEXP_INSTR": "REGEXP_INSTR",  # May need custom handling
    "REGEXP_LIKE": "RLIKE",  # Handled specially
    "REGEXP_COUNT": "REGEXP_COUNT",  # Handled specially → custom
    "NLSSORT": None,  # NLS-specific, handled specially
    "NLS_INITCAP": "INITCAP",
    "NLS_LOWER": "LOWER",
    "NLS_UPPER": "UPPER",
    "NCHR": "CHR",  # National character → CHR
    "UNISTR": "DECODE",  # Unicode string literal, handled specially
    "COMPOSE": None,  # Unicode composition, no direct equivalent
    "DECOMPOSE": None,  # Unicode decomposition, no direct equivalent
    
    # ==========================================
    # NUMERIC FUNCTIONS
    # ==========================================
    "ABS": "ABS",
    "CEIL": "CEIL",
    "CEILING": "CEIL",
    "FLOOR": "FLOOR",
    "ROUND": "ROUND",
    "TRUNC": "TRUNC",
    "TRUNCATE": "TRUNC",
    "MOD": "MOD",
    "REMAINDER": "MOD",  # Similar to MOD with different sign behavior
    "POWER": "POWER",
    "POW": "POWER",
    "SQRT": "SQRT",
    "SIGN": "SIGN",
    "SIGNUM": "SIGN",
    "EXP": "EXP",
    "LN": "LN",
    "LOG": "LOG",
    "SIN": "SIN",
    "COS": "COS",
    "TAN": "TAN",
    "ASIN": "ASIN",
    "ACOS": "ACOS",
    "ATAN": "ATAN",
    "ATAN2": "ATAN2",
    "SINH": "SINH",
    "COSH": "COSH",
    "TANH": "TANH",
    "BITAND": "BITAND",  # Databricks supports this
    "NANVL": "NANVL",  # Handled specially → CASE with isnan()
    "WIDTH_BUCKET": "WIDTH_BUCKET",
    "BIN_TO_NUM": None,  # Handled specially
    "DEGREES": "DEGREES",
    "RADIANS": "RADIANS",
    "PI": "PI",
    
    # ==========================================
    # DATE/TIME FUNCTIONS
    # ==========================================
    "SYSDATE": "CURRENT_DATE",  # Handled specially → CURRENT_TIMESTAMP()
    "SYSTIMESTAMP": "CURRENT_TIMESTAMP",  # Handled specially
    "CURRENT_DATE": "CURRENT_DATE",
    "CURRENT_TIMESTAMP": "CURRENT_TIMESTAMP",
    "LOCALTIMESTAMP": "CURRENT_TIMESTAMP",
    "SESSIONTIMEZONE": "CURRENT_TIMEZONE",
    "DBTIMEZONE": "CURRENT_TIMEZONE",  # Approximation
    "ADD_MONTHS": "ADD_MONTHS",
    "MONTHS_BETWEEN": "MONTHS_BETWEEN",
    "LAST_DAY": "LAST_DAY",
    "NEXT_DAY": "NEXT_DAY",
    "EXTRACT": "EXTRACT",
    "NUMTODSINTERVAL": None,  # Handled specially
    "NUMTOYMINTERVAL": None,  # Handled specially
    "TO_DSINTERVAL": None,  # Handled specially
    "TO_YMINTERVAL": None,  # Handled specially
    "FROM_TZ": "FROM_UTC_TIMESTAMP",  # Approximation
    "SYS_EXTRACT_UTC": "TO_UTC_TIMESTAMP",  # Handled specially
    "TZ_OFFSET": None,  # No direct equivalent
    "NEW_TIME": None,  # Deprecated, handled specially
    "TRUNC": "TRUNC",  # For dates too, format handled specially
    "ROUND": "ROUND",  # For dates too
    "TO_TIMESTAMP_TZ": "TO_TIMESTAMP",  # TZ info may be lost
    
    # ==========================================
    # CONVERSION FUNCTIONS
    # ==========================================
    "TO_CHAR": "TO_CHAR",  # Handled specially for format conversion
    "TO_DATE": "TO_DATE",  # Format handled specially
    "TO_NUMBER": "CAST",  # Handled specially → CAST AS DECIMAL
    "TO_TIMESTAMP": "TO_TIMESTAMP",
    "CAST": "CAST",
    "CONVERT": "ENCODE",  # Character set conversion, approximation
    "RAWTOHEX": "HEX",
    "HEXTORAW": "UNHEX",
    "TO_BINARY_FLOAT": "CAST",  # → CAST AS FLOAT
    "TO_BINARY_DOUBLE": "CAST",  # → CAST AS DOUBLE
    "TO_CLOB": "CAST",  # → CAST AS STRING
    "TO_LOB": "CAST",  # → CAST AS STRING
    "TO_NCHAR": "CAST",  # → CAST AS STRING
    "TO_NCLOB": "CAST",  # → CAST AS STRING
    "TO_SINGLE_BYTE": None,  # No equivalent
    "TO_MULTI_BYTE": None,  # No equivalent
    "CHARTOROWID": None,  # No equivalent
    "ROWIDTOCHAR": None,  # No equivalent
    "ROWIDTONCHAR": None,  # No equivalent
    "ASCIISTR": "ENCODE",  # Handled specially
    "VSIZE": None,  # Handled specially → LENGTH
    "DUMP": None,  # Handled specially, for debugging
    
    # ==========================================
    # AGGREGATE FUNCTIONS
    # ==========================================
    "COUNT": "COUNT",
    "SUM": "SUM",
    "AVG": "AVG",
    "MIN": "MIN",
    "MAX": "MAX",
    "LISTAGG": "ARRAY_JOIN",  # Handled specially → ARRAY_JOIN(COLLECT_LIST())
    "STDDEV": "STDDEV",
    "STDDEV_POP": "STDDEV_POP",
    "STDDEV_SAMP": "STDDEV_SAMP",
    "VARIANCE": "VARIANCE",
    "VAR_POP": "VAR_POP",
    "VAR_SAMP": "VAR_SAMP",
    "MEDIAN": "PERCENTILE",  # Handled specially → PERCENTILE(col, 0.5)
    "STATS_MODE": None,  # Handled specially → most frequent value
    "CORR": "CORR",
    "COVAR_POP": "COVAR_POP",
    "COVAR_SAMP": "COVAR_SAMP",
    "REGR_SLOPE": "REGR_SLOPE",  # Databricks has these
    "REGR_INTERCEPT": "REGR_INTERCEPT",
    "REGR_COUNT": "REGR_COUNT",
    "REGR_R2": "REGR_R2",
    "REGR_AVGX": "REGR_AVGX",
    "REGR_AVGY": "REGR_AVGY",
    "REGR_SXX": "REGR_SXX",
    "REGR_SYY": "REGR_SYY",
    "REGR_SXY": "REGR_SXY",
    "WM_CONCAT": "ARRAY_JOIN",  # Deprecated Oracle, handled like LISTAGG
    "COLLECT": "COLLECT_LIST",
    "APPROX_COUNT_DISTINCT": "APPROX_COUNT_DISTINCT",
    "APPROX_COUNT": "APPROX_COUNT_DISTINCT",
    "APPROX_MEDIAN": "APPROX_PERCENTILE",  # Handled specially
    "APPROX_PERCENTILE": "APPROX_PERCENTILE",
    
    # ==========================================
    # ANALYTIC/WINDOW FUNCTIONS
    # ==========================================
    "ROW_NUMBER": "ROW_NUMBER",
    "RANK": "RANK",
    "DENSE_RANK": "DENSE_RANK",
    "NTILE": "NTILE",
    "LEAD": "LEAD",
    "LAG": "LAG",
    "FIRST_VALUE": "FIRST_VALUE",
    "LAST_VALUE": "LAST_VALUE",
    "NTH_VALUE": "NTH_VALUE",
    "CUME_DIST": "CUME_DIST",
    "PERCENT_RANK": "PERCENT_RANK",
    "PERCENTILE_CONT": "PERCENTILE_CONT",
    "PERCENTILE_DISC": "PERCENTILE_DISC",
    "RATIO_TO_REPORT": None,  # Handled specially → expr / SUM(expr) OVER()
    "FIRST": None,  # KEEP (DENSE_RANK FIRST ...), handled specially
    "LAST": None,   # KEEP (DENSE_RANK LAST ...), handled specially
    
    # ==========================================
    # NULL HANDLING FUNCTIONS
    # ==========================================
    "NULLIF": "NULLIF",
    "COALESCE": "COALESCE",
    "GREATEST": "GREATEST",
    "LEAST": "LEAST",
    "LNNVL": None,  # Handled specially → NOT(expr) OR expr IS NULL
    "NANVL": None,  # Handled specially → CASE WHEN isnan(expr) THEN alt ELSE expr
    
    # ==========================================
    # CONDITIONAL FUNCTIONS
    # ==========================================
    "CASE": "CASE",
    "IF": "IF",
    "IFF": "IF",  # Alias
    "IFNULL": "COALESCE",
    "ZEROIFNULL": "COALESCE",  # Handled specially
    "NULLIFZERO": "NULLIF",  # Handled specially
    
    # ==========================================
    # JSON FUNCTIONS (Oracle 12c+)
    # ==========================================
    "JSON_VALUE": "GET_JSON_OBJECT",  # Handled specially, different syntax
    "JSON_QUERY": "GET_JSON_OBJECT",  # Returns JSON fragment
    "JSON_TABLE": None,  # Handled specially → LATERAL VIEW + JSON functions
    "JSON_OBJECT": "TO_JSON",  # Handled specially
    "JSON_ARRAY": "TO_JSON",  # Handled specially
    "JSON_EXISTS": None,  # Handled specially → GET_JSON_OBJECT IS NOT NULL
    "JSON_ARRAYAGG": "TO_JSON",  # Handled specially → TO_JSON(COLLECT_LIST())
    "JSON_OBJECTAGG": "TO_JSON",  # Handled specially
    "JSON_SERIALIZE": "TO_JSON",
    "JSON_MERGEPATCH": None,  # No direct equivalent
    "IS_JSON": None,  # Handled specially → try parsing
    
    # ==========================================
    # XML FUNCTIONS
    # ==========================================
    "XMLTYPE": None,  # Handled specially
    "XMLELEMENT": None,  # Handled specially
    "XMLFOREST": None,  # Handled specially
    "XMLAGG": None,  # Handled specially
    "XMLPARSE": None,  # Handled specially
    "XMLROOT": None,  # Handled specially
    "XMLSERIALIZE": None,  # Handled specially
    "EXTRACT": "XPATH",  # When used with XMLType, → xpath()
    "EXTRACTVALUE": "XPATH_STRING",  # Handled specially
    "EXISTSNODE": None,  # Handled specially
    "XMLQUERY": "XPATH",  # Handled specially
    "XMLTABLE": None,  # Handled specially
    
    # ==========================================
    # CRYPTOGRAPHIC/HASH FUNCTIONS
    # ==========================================
    "SYS_GUID": "UUID",  # Handled specially → uuid()
    "ORA_HASH": "HASH",  # Handled specially → hash()
    "STANDARD_HASH": "SHA2",  # Handled specially, default SHA256
    "DBMS_CRYPTO.HASH": "SHA2",  # DBMS package function
    "DBMS_OBFUSCATION_TOOLKIT.MD5": "MD5",
    
    # ==========================================
    # MISCELLANEOUS FUNCTIONS
    # ==========================================
    "ROWNUM": "ROW_NUMBER",  # Handled specially
    "ROWID": None,  # No direct equivalent
    "USER": "CURRENT_USER",
    "CURRENT_USER": "CURRENT_USER",
    "SESSION_USER": "CURRENT_USER",
    "UID": None,  # No direct equivalent
    "USERENV": None,  # Handled specially → CURRENT_* functions
    "SYS_CONTEXT": None,  # Handled specially
    "ORA_ROWSCN": None,  # No equivalent
    "SCN_TO_TIMESTAMP": None,  # No equivalent
    "TIMESTAMP_TO_SCN": None,  # No equivalent
    "TREAT": None,  # Type casting in object types
    "SYS_TYPEID": None,  # Object type ID
    "DEREF": None,  # Dereference REF
    "REF": None,  # Create REF
    "VALUE": None,  # Object table value
    "EMPTY_BLOB": None,  # → empty binary literal
    "EMPTY_CLOB": None,  # → empty string
    "BFILENAME": None,  # No equivalent
    "SQLCODE": None,  # PL/SQL specific
    "SQLERRM": None,  # PL/SQL specific
    
    # ==========================================
    # SPATIAL FUNCTIONS (SDO_*)
    # ==========================================
    "SDO_GEOMETRY": None,  # Use H3 or other spatial libraries
    "SDO_RELATE": None,
    "SDO_WITHIN_DISTANCE": None,
    "SDO_DISTANCE": None,
}

# Oracle date format to Databricks date format mappings
# Comprehensive mapping for Oracle date/time format elements
DATE_FORMAT_MAPPINGS = {
    # Year formats
    "SYYYY": "yyyy",  # 4-digit year with sign (BC dates)
    "YYYY": "yyyy",   # 4-digit year
    "YYY": "yyy",     # Last 3 digits of year
    "YY": "yy",       # Last 2 digits of year
    "Y": "y",         # Last digit of year
    "IYYY": "YYYY",   # ISO 4-digit year
    "IYY": "YYY",     # ISO 3-digit year
    "IY": "YY",       # ISO 2-digit year
    "I": "Y",         # ISO last digit
    "RRRR": "yyyy",   # Round year (4 digits)
    "RR": "yy",       # Round year (2 digits)
    "YEAR": "yyyy",   # Spelled out year (approximation)
    
    # Quarter
    "Q": "Q",         # Quarter (1-4)
    
    # Month formats
    "MM": "MM",       # Month (01-12)
    "MON": "MMM",     # Abbreviated month (JAN, FEB)
    "MONTH": "MMMM",  # Full month name
    "RM": "MM",       # Roman numeral month → numeric
    
    # Week formats
    "WW": "ww",       # Week of year (1-52)
    "IW": "ww",       # ISO week of year
    "W": "W",         # Week of month
    
    # Day formats
    "DDD": "DDD",     # Day of year (1-366)
    "DD": "dd",       # Day of month (01-31)
    "D": "u",         # Day of week (1-7)
    "DY": "EEE",      # Abbreviated day (MON, TUE)
    "DAY": "EEEE",    # Full day name
    "J": "D",         # Julian day (approximation)
    
    # Hour formats
    "HH": "hh",       # Hour of day (01-12)
    "HH12": "hh",     # Hour of day (01-12)
    "HH24": "HH",     # Hour of day (00-23)
    
    # Minute/Second formats
    "MI": "mm",       # Minute (00-59)
    "SS": "ss",       # Second (00-59)
    "SSSSS": "SSSSS", # Seconds past midnight
    
    # Fractional seconds
    "FF": "SSS",      # Fractional seconds (default precision)
    "FF1": "S",       # 1 digit fractional
    "FF2": "SS",      # 2 digits fractional
    "FF3": "SSS",     # 3 digits (milliseconds)
    "FF4": "SSSS",    # 4 digits
    "FF5": "SSSSS",   # 5 digits
    "FF6": "SSSSSS",  # 6 digits (microseconds)
    "FF7": "SSSSSSS", # 7 digits
    "FF8": "SSSSSSSS", # 8 digits
    "FF9": "SSSSSSSSS", # 9 digits (nanoseconds)
    
    # AM/PM indicators
    "AM": "a",
    "PM": "a",
    "A.M.": "a",
    "P.M.": "a",
    
    # Era indicator
    "AD": "G",
    "BC": "G",
    "A.D.": "G",
    "B.C.": "G",
    
    # Time zone formats
    "TZH": "XXX",     # Time zone hour
    "TZM": "XXX",     # Time zone minute
    "TZR": "VV",      # Time zone region
    "TZD": "zzz",     # Daylight savings indicator
    
    # Punctuation and literals (usually pass through)
    "\"": "'",        # Quoted text delimiter
    
    # Special Oracle format modifiers (stripped or approximated)
    "FM": "",         # Fill mode (suppress padding) - stripped
    "FX": "",         # Format exact - stripped
    "TH": "",         # Ordinal suffix (1st, 2nd) - stripped
    "SP": "",         # Spelled out - stripped
    "THSP": "",       # Ordinal spelled out - stripped
    "SPTH": "",       # Spelled out ordinal - stripped
}

# Additional Oracle number format mappings (for TO_CHAR with numbers)
NUMBER_FORMAT_MAPPINGS = {
    "9": "#",         # Digit position (blank if not significant)
    "0": "0",         # Digit position (0 if not significant)
    ".": ".",         # Decimal point
    ",": ",",         # Grouping separator
    "D": ".",         # Local decimal separator
    "G": ",",         # Local grouping separator
    "$": "$",         # Dollar sign
    "L": "$",         # Local currency symbol → $
    "C": "USD",       # ISO currency symbol
    "MI": "",         # Trailing minus (handled specially)
    "S": "",          # Leading sign (handled specially)
    "PR": "",         # Angle brackets for negative (handled specially)
    "EEEE": "E0",     # Scientific notation
    "RN": "",         # Roman numerals (handled specially)
    "V": "",          # Multiply by 10^n (handled specially)
    "X": "",          # Hexadecimal (handled specially)
    "B": "",          # Blank for zero (handled specially)
}

# Oracle data type to Databricks data type mappings
DATA_TYPE_MAPPINGS = {
    "VARCHAR2": "STRING",
    "NVARCHAR2": "STRING",
    "CHAR": "STRING",
    "NCHAR": "STRING",
    "CLOB": "STRING",
    "NCLOB": "STRING",
    "LONG": "STRING",
    "NUMBER": "DECIMAL",
    "BINARY_FLOAT": "FLOAT",
    "BINARY_DOUBLE": "DOUBLE",
    "INTEGER": "INT",
    "INT": "INT",
    "SMALLINT": "SMALLINT",
    "FLOAT": "DOUBLE",
    "REAL": "FLOAT",
    "DATE": "TIMESTAMP",  # Oracle DATE includes time
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    "TIMESTAMP WITH LOCAL TIME ZONE": "TIMESTAMP",
    "INTERVAL YEAR TO MONTH": "STRING",  # No direct equivalent
    "INTERVAL DAY TO SECOND": "STRING",  # No direct equivalent
    "RAW": "BINARY",
    "LONG RAW": "BINARY",
    "BLOB": "BINARY",
    "BFILE": "STRING",  # Store path as string
    "ROWID": "STRING",
    "UROWID": "STRING",
    "BOOLEAN": "BOOLEAN",
    "XMLTYPE": "STRING",
    "SDO_GEOMETRY": "STRING",  # Serialize as string/JSON
}

# Oracle operators that need special handling
OPERATOR_MAPPINGS = {
    "||": "CONCAT",  # String concatenation
    "(+)": "LEFT OUTER JOIN",  # Oracle outer join syntax
}


def convert_oracle_date_format(oracle_format: str) -> str:
    """
    Convert Oracle date format string to Databricks/Spark format string.
    
    Args:
        oracle_format: Oracle date format string (e.g., 'YYYY-MM-DD HH24:MI:SS')
        
    Returns:
        Databricks compatible date format string
    """
    result = oracle_format
    
    # Remove format modifiers that don't have Spark equivalents
    modifiers_to_remove = ['FM', 'FX', 'TH', 'SP', 'THSP', 'SPTH']
    for mod in modifiers_to_remove:
        result = result.replace(mod, '')
    
    # Sort by length (longest first) to avoid partial replacements
    sorted_mappings = sorted(
        DATE_FORMAT_MAPPINGS.items(),
        key=lambda x: len(x[0]),
        reverse=True
    )
    
    for oracle_fmt, spark_fmt in sorted_mappings:
        # Case-insensitive replacement for format elements
        import re
        result = re.sub(re.escape(oracle_fmt), spark_fmt, result, flags=re.IGNORECASE)
    
    return result


def convert_oracle_number_format(oracle_format: str) -> str:
    """
    Convert Oracle number format string to Databricks/Spark format string.
    
    Args:
        oracle_format: Oracle number format string (e.g., '999,999.99')
        
    Returns:
        Databricks compatible format string (or None if special handling needed)
    """
    result = oracle_format.upper()
    
    # Handle special formats that need custom conversion
    if 'RN' in result or 'EEEE' in result or 'X' in result:
        return None  # Requires special handling
    
    for oracle_fmt, spark_fmt in NUMBER_FORMAT_MAPPINGS.items():
        result = result.replace(oracle_fmt, spark_fmt)
    
    return result


def get_databricks_data_type(oracle_type: str, precision: int = None, scale: int = None) -> str:
    """
    Convert Oracle data type to Databricks data type.
    
    Args:
        oracle_type: Oracle data type name
        precision: Optional precision for numeric types
        scale: Optional scale for numeric types
        
    Returns:
        Databricks compatible data type string
    """
    oracle_type_upper = oracle_type.upper().strip()
    
    # Handle NUMBER with precision/scale
    if oracle_type_upper.startswith("NUMBER"):
        if precision is not None and scale is not None:
            if scale == 0:
                if precision <= 9:
                    return "INT"
                elif precision <= 18:
                    return "BIGINT"
                else:
                    return f"DECIMAL({precision}, 0)"
            else:
                return f"DECIMAL({precision}, {scale})"
        elif precision is not None:
            return f"DECIMAL({precision}, 0)"
        else:
            return "DECIMAL(38, 10)"
    
    # Handle VARCHAR2 with length
    if oracle_type_upper.startswith("VARCHAR2") or oracle_type_upper.startswith("CHAR"):
        return "STRING"
    
    # Direct mapping
    base_type = oracle_type_upper.split("(")[0].strip()
    return DATA_TYPE_MAPPINGS.get(base_type, "STRING")


def get_function_mapping(oracle_function: str) -> tuple:
    """
    Get the Databricks equivalent for an Oracle function.
    
    Args:
        oracle_function: Oracle function name
        
    Returns:
        Tuple of (databricks_function, needs_special_handling)
    """
    func_upper = oracle_function.upper().strip()
    
    # Functions that need special transformation logic
    special_handling_functions = {
        'DECODE', 'NVL2', 'TO_NUMBER', 'LISTAGG', 'WM_CONCAT', 
        'MEDIAN', 'ROWNUM', 'SYSDATE', 'SYSTIMESTAMP',
        'REGEXP_SUBSTR', 'REGEXP_LIKE', 'REGEXP_COUNT',
        'JSON_VALUE', 'JSON_QUERY', 'JSON_TABLE', 'JSON_OBJECT',
        'JSON_ARRAY', 'JSON_EXISTS', 'JSON_ARRAYAGG', 'JSON_OBJECTAGG',
        'SYS_GUID', 'ORA_HASH', 'STANDARD_HASH',
        'USERENV', 'SYS_CONTEXT',
        'NUMTODSINTERVAL', 'NUMTOYMINTERVAL',
        'RATIO_TO_REPORT', 'LNNVL', 'NANVL',
        'APPROX_MEDIAN', 'APPROX_PERCENTILE',
        'COLLECT', 'BIN_TO_NUM', 'VSIZE', 'DUMP'
    }
    
    databricks_func = DIRECT_FUNCTION_MAPPINGS.get(func_upper)
    needs_special = func_upper in special_handling_functions
    
    return (databricks_func, needs_special)


# USERENV parameter mappings
USERENV_MAPPINGS = {
    'LANG': ('CURRENT_CATALOG', 'Use catalog name as language approximation'),
    'LANGUAGE': ('CURRENT_CATALOG', 'Use catalog name as language approximation'),
    'TERMINAL': ("'unknown'", 'No terminal concept in Databricks'),
    'SESSIONID': ('CURRENT_USER', 'Use current user as session approximation'),
    'SID': ('CURRENT_USER', 'Use current user as session approximation'),
    'INSTANCE': ("1", 'Single instance in Databricks'),
    'ENTRYID': ("0", 'No entry ID in Databricks'),
    'CURRENT_USER': ('CURRENT_USER', 'Direct equivalent'),
    'SESSION_USER': ('CURRENT_USER', 'Direct equivalent'),
    'CURRENT_SCHEMA': ('CURRENT_SCHEMA', 'Direct equivalent'),
    'CURRENT_CATALOG': ('CURRENT_CATALOG', 'Direct equivalent'),
    'DB_NAME': ('CURRENT_CATALOG', 'Catalog is similar to database'),
    'HOST': ("'databricks'", 'Databricks cluster host'),
    'IP_ADDRESS': ("'0.0.0.0'", 'No IP tracking in Databricks'),
    'ISDBA': ("false", 'No DBA concept in Databricks'),
    'NLS_CALENDAR': ("'GREGORIAN'", 'Default calendar'),
    'NLS_CURRENCY': ("'$'", 'Default currency'),
    'NLS_DATE_FORMAT': ("'yyyy-MM-dd'", 'Default date format'),
    'NLS_DATE_LANGUAGE': ("'AMERICAN'", 'Default language'),
    'NLS_SORT': ("'BINARY'", 'Default sort'),
    'NLS_TERRITORY': ("'AMERICA'", 'Default territory'),
    'OS_USER': ('CURRENT_USER', 'Use current user'),
    'CLIENT_INFO': ("''", 'No client info'),
    'MODULE': ("''", 'No module info'),
    'ACTION': ("''", 'No action info'),
    'CLIENT_IDENTIFIER': ('CURRENT_USER', 'Use current user'),
}

# SYS_CONTEXT namespace mappings
SYS_CONTEXT_MAPPINGS = {
    'USERENV': USERENV_MAPPINGS,
    # Add other namespaces as needed
}


def get_userenv_equivalent(parameter: str) -> tuple:
    """
    Get Databricks equivalent for USERENV parameter.
    
    Args:
        parameter: USERENV parameter name (e.g., 'CURRENT_USER')
        
    Returns:
        Tuple of (databricks_expression, comment)
    """
    param_upper = parameter.upper().strip().strip("'\"")
    return USERENV_MAPPINGS.get(param_upper, ("NULL", f"Unknown USERENV parameter: {parameter}"))


def get_sys_context_equivalent(namespace: str, parameter: str) -> tuple:
    """
    Get Databricks equivalent for SYS_CONTEXT.
    
    Args:
        namespace: Context namespace (e.g., 'USERENV')
        parameter: Context parameter name
        
    Returns:
        Tuple of (databricks_expression, comment)
    """
    ns_upper = namespace.upper().strip().strip("'\"")
    param_upper = parameter.upper().strip().strip("'\"")
    
    ns_mappings = SYS_CONTEXT_MAPPINGS.get(ns_upper, {})
    return ns_mappings.get(param_upper, ("NULL", f"Unknown SYS_CONTEXT: {namespace}.{parameter}"))

