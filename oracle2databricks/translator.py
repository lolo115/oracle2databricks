"""
Main Oracle to Databricks SQL translator module.

This module provides the primary interface for translating Oracle SQL
to Databricks SQL using sqlglot with custom transformations.
"""

import re
import traceback
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass, field
from enum import Enum

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .transformations import apply_all_transformations, apply_string_transformations
from .function_mappings import get_databricks_data_type
from .connect_by_converter import ConnectByConverter, has_connect_by
from .custom_rules import CustomRulesConfig, load_custom_rules


def strip_sql_comments(sql: str) -> str:
    """
    Remove SQL comments from the input string.
    
    Handles:
    - Single-line comments starting with --
    - Multi-line comments enclosed in /* */
    - Preserves string literals (doesn't strip -- or /* inside quotes)
    
    Args:
        sql: SQL string potentially containing comments
        
    Returns:
        SQL string with all comments removed
    """
    if not sql:
        return sql
    
    result = []
    i = 0
    length = len(sql)
    
    while i < length:
        # Check for string literals (single or double quotes)
        if sql[i] in ("'", '"'):
            quote_char = sql[i]
            result.append(sql[i])
            i += 1
            # Copy everything inside the string literal
            while i < length:
                if sql[i] == quote_char:
                    result.append(sql[i])
                    i += 1
                    # Check for escaped quote (doubled quote in Oracle)
                    if i < length and sql[i] == quote_char:
                        result.append(sql[i])
                        i += 1
                        continue
                    break
                else:
                    result.append(sql[i])
                    i += 1
        # Check for single-line comment --
        elif i + 1 < length and sql[i:i+2] == '--':
            # Skip until end of line
            while i < length and sql[i] != '\n':
                i += 1
            # Keep the newline to preserve line structure
            if i < length:
                result.append('\n')
                i += 1
        # Check for multi-line comment /* */
        elif i + 1 < length and sql[i:i+2] == '/*':
            i += 2  # Skip /*
            # Find the closing */
            while i + 1 < length and sql[i:i+2] != '*/':
                # Preserve newlines to maintain line structure
                if sql[i] == '\n':
                    result.append('\n')
                i += 1
            if i + 1 < length:
                i += 2  # Skip */
        else:
            result.append(sql[i])
            i += 1
    
    return ''.join(result)


class ConversionIssueType(Enum):
    """Types of conversion issues."""
    ERROR = "ERROR"
    WARNING = "WARNING"
    UNSUPPORTED = "UNSUPPORTED"
    PARTIAL = "PARTIAL"
    INFO = "INFO"


@dataclass
class ConversionIssue:
    """Represents a specific issue encountered during conversion."""
    issue_type: ConversionIssueType
    message: str
    details: Optional[str] = None
    line_number: Optional[int] = None
    oracle_construct: Optional[str] = None
    suggestion: Optional[str] = None
    
    def __str__(self) -> str:
        parts = [f"[{self.issue_type.value}] {self.message}"]
        if self.oracle_construct:
            parts.append(f"  Oracle construct: {self.oracle_construct}")
        if self.details:
            parts.append(f"  Details: {self.details}")
        if self.suggestion:
            parts.append(f"  Suggestion: {self.suggestion}")
        if self.line_number:
            parts.append(f"  Line: {self.line_number}")
        return "\n".join(parts)


@dataclass
class TranslationResult:
    """Result of a SQL translation operation."""
    
    original_sql: str
    translated_sql: str
    success: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    issues: List[ConversionIssue] = field(default_factory=list)
    unsupported_features: List[str] = field(default_factory=list)
    line_number: Optional[int] = None  # Starting line number in source file
    
    def __str__(self) -> str:
        if self.success:
            return self.translated_sql
        return f"-- Translation failed: {'; '.join(self.errors)}\n-- Original SQL:\n{self.original_sql}"
    
    def get_detailed_report(self) -> str:
        """Generate a detailed report of the translation result."""
        lines = []
        
        if not self.success:
            lines.append("=" * 60)
            lines.append("TRANSLATION FAILED")
            lines.append("=" * 60)
        
        if self.errors:
            lines.append("\n[ERRORS]")
            for error in self.errors:
                lines.append(f"  ✗ {error}")
        
        if self.unsupported_features:
            lines.append("\n[UNSUPPORTED FEATURES]")
            for feature in self.unsupported_features:
                lines.append(f"  ⚠ {feature}")
        
        if self.issues:
            lines.append("\n[DETAILED ISSUES]")
            for issue in self.issues:
                lines.append(f"  {issue}")
        
        if self.warnings:
            lines.append("\n[WARNINGS]")
            for warning in self.warnings:
                lines.append(f"  ⚠ {warning}")
        
        if not self.success:
            lines.append("\n[ORIGINAL SQL]")
            for i, line in enumerate(self.original_sql.split('\n'), 1):
                lines.append(f"  {i:3d} | {line}")
        
        return "\n".join(lines)


class OracleToDatabricksTranslator:
    """
    Translator for converting Oracle SQL to Databricks SQL.
    
    This class provides methods to translate individual SQL statements
    as well as entire SQL scripts containing multiple statements.
    
    Example:
        >>> translator = OracleToDatabricksTranslator()
        >>> result = translator.translate("SELECT SYSDATE FROM DUAL")
        >>> print(result.translated_sql)
        SELECT CURRENT_TIMESTAMP()
        
        # With custom rules
        >>> translator = OracleToDatabricksTranslator(config_file="custom_rules.json")
        >>> result = translator.translate("SELECT MY_CUSTOM_FUNC(a, b) FROM test")
    """
    
    def __init__(
        self,
        pretty: bool = True,
        identify: bool = False,
        config_file: Optional[str] = None,
        custom_rules_config: Optional[CustomRulesConfig] = None
    ):
        """
        Initialize the translator.
        
        Args:
            pretty: Whether to format output SQL with indentation
            identify: Whether to quote identifiers
            config_file: Optional path to a JSON file containing custom transformation rules
            custom_rules_config: Optional pre-loaded CustomRulesConfig object
        """
        self.pretty = pretty
        self.identify = identify
        self._dual_pattern = re.compile(r'\bFROM\s+DUAL\b', re.IGNORECASE)
        self._dual_only_pattern = re.compile(
            r'^\s*SELECT\s+(.+?)\s+FROM\s+DUAL\s*;?\s*$',
            re.IGNORECASE | re.DOTALL
        )
        
        # Load custom rules configuration
        self.custom_rules_config: Optional[CustomRulesConfig] = None
        if custom_rules_config is not None:
            self.custom_rules_config = custom_rules_config
        elif config_file is not None:
            self.custom_rules_config = load_custom_rules(config_file)
            if self.custom_rules_config:
                enabled_count = len(self.custom_rules_config.get_enabled_rules())
                total_count = len(self.custom_rules_config.rules)
                print(f"Loaded {enabled_count}/{total_count} custom transformation rules from: {config_file}")
    
    def translate(self, sql: str) -> TranslationResult:
        """
        Translate a single Oracle SQL statement to Databricks SQL.
        
        Args:
            sql: Oracle SQL statement to translate
            
        Returns:
            TranslationResult containing the translated SQL and any errors/warnings
        """
        errors = []
        warnings = []
        issues = []
        unsupported_features = []
        
        # Strip comments before processing
        sql_without_comments = strip_sql_comments(sql.strip())
        
        # Check if there's any SQL content after stripping comments
        if not sql_without_comments.strip():
            return TranslationResult(
                original_sql=sql,
                translated_sql="",
                success=True,  # Empty/comment-only is not a failure
                errors=[],
                warnings=[],
                issues=[],
                unsupported_features=[]
            )
        
        # Detect unsupported features before processing
        detected_unsupported = self._detect_unsupported_features(sql_without_comments)
        unsupported_features.extend(detected_unsupported)
        
        # Handle CONNECT BY hierarchical queries with special converter
        if has_connect_by(sql_without_comments):
            connect_by_result = self._convert_connect_by_query(sql_without_comments, sql)
            if connect_by_result is not None:
                return connect_by_result
        
        # Pre-process: Apply string-level transformations (including custom rules)
        processed_sql, applied_custom_rules = apply_string_transformations(
            sql_without_comments.strip(),
            self.custom_rules_config
        )
        
        # Track applied custom rules in warnings for transparency
        if applied_custom_rules:
            for rule_name in applied_custom_rules:
                if "ERROR" not in rule_name:
                    warnings.append(f"Applied custom rule: {rule_name}")
                else:
                    warnings.append(f"Custom rule error: {rule_name}")
        
        # Check for warnings added during pre-processing
        if processed_sql.startswith("-- WARNING:"):
            lines = processed_sql.split("\n")
            for line in lines:
                if line.startswith("-- WARNING:"):
                    warning_msg = line.replace("-- WARNING:", "").strip()
                    warnings.append(warning_msg)
                    issues.append(ConversionIssue(
                        issue_type=ConversionIssueType.WARNING,
                        message=warning_msg
                    ))
            processed_sql = "\n".join(
                line for line in lines if not line.startswith("-- WARNING:")
            )
        
        try:
            # Parse Oracle SQL
            parsed = sqlglot.parse_one(processed_sql, dialect="oracle")
            
            # Analyze parsed SQL for potential issues
            analysis_issues = self._analyze_sql_constructs(parsed, sql)
            issues.extend(analysis_issues)
            for issue in analysis_issues:
                if issue.issue_type == ConversionIssueType.UNSUPPORTED:
                    unsupported_features.append(issue.message)
                elif issue.issue_type == ConversionIssueType.WARNING:
                    warnings.append(issue.message)
            
            # Apply custom transformations
            transformed = apply_all_transformations(parsed)
            
            # Handle DUAL table removal
            transformed = self._remove_dual(transformed)
            
            # Generate Databricks SQL
            translated = transformed.sql(
                dialect="databricks",
                pretty=self.pretty,
                identify=self.identify
            )
            
            # Post-process
            translated = self._post_process(translated)
            
            # Check if translation looks incomplete or problematic
            post_issues = self._check_translation_quality(sql, translated)
            issues.extend(post_issues)
            for issue in post_issues:
                if issue.issue_type == ConversionIssueType.WARNING:
                    warnings.append(issue.message)
            
            return TranslationResult(
                original_sql=sql,
                translated_sql=translated,
                success=True,
                errors=errors,
                warnings=warnings,
                issues=issues,
                unsupported_features=unsupported_features
            )
            
        except ParseError as e:
            error_msg = f"Parse error: {str(e)}"
            errors.append(error_msg)
            issues.append(ConversionIssue(
                issue_type=ConversionIssueType.ERROR,
                message="Failed to parse Oracle SQL",
                details=str(e),
                suggestion="Check SQL syntax is valid Oracle SQL. Ensure all parentheses and quotes are balanced."
            ))
            return TranslationResult(
                original_sql=sql,
                translated_sql="",
                success=False,
                errors=errors,
                warnings=warnings,
                issues=issues,
                unsupported_features=unsupported_features
            )
        except Exception as e:
            error_msg = f"Translation error: {str(e)}"
            errors.append(error_msg)
            issues.append(ConversionIssue(
                issue_type=ConversionIssueType.ERROR,
                message="Translation failed unexpectedly",
                details=f"{type(e).__name__}: {str(e)}",
                suggestion="This may be a bug or an unsupported Oracle construct. Try simplifying the SQL."
            ))
            return TranslationResult(
                original_sql=sql,
                translated_sql="",
                success=False,
                errors=errors,
                warnings=warnings,
                issues=issues,
                unsupported_features=unsupported_features
            )
    
    def _detect_unsupported_features(self, sql: str) -> List[str]:
        """
        Detect Oracle features that are not supported or partially supported.
        
        Args:
            sql: Original SQL string
            
        Returns:
            List of unsupported feature descriptions
        """
        unsupported = []
        sql_upper = sql.upper()
        
        # Hierarchical queries
        if 'CONNECT BY' in sql_upper:
            unsupported.append(
                "CONNECT BY hierarchical queries - Convert to recursive CTE manually"
            )
        if 'START WITH' in sql_upper and 'CONNECT BY' in sql_upper:
            unsupported.append(
                "START WITH clause - Part of hierarchical query, needs manual conversion"
            )
        if 'LEVEL' in sql_upper and 'CONNECT BY' in sql_upper:
            unsupported.append(
                "LEVEL pseudo-column - Used in hierarchical queries, needs manual handling"
            )
        
        # MODEL clause
        if re.search(r'\bMODEL\s+', sql_upper):
            unsupported.append(
                "MODEL clause - No direct equivalent in Databricks, requires manual rewrite"
            )
        
        # PIVOT/UNPIVOT (partial support)
        if re.search(r'\bPIVOT\s*\(', sql_upper):
            unsupported.append(
                "PIVOT clause - Databricks has different syntax, may need adjustment"
            )
        if re.search(r'\bUNPIVOT\s*\(', sql_upper):
            unsupported.append(
                "UNPIVOT clause - Databricks has different syntax, may need adjustment"
            )
        
        # Flashback queries
        if 'AS OF' in sql_upper and ('TIMESTAMP' in sql_upper or 'SCN' in sql_upper):
            unsupported.append(
                "Flashback query (AS OF) - Use Delta Lake time travel syntax instead"
            )
        if 'VERSIONS BETWEEN' in sql_upper:
            unsupported.append(
                "VERSIONS BETWEEN - Use Delta Lake history instead"
            )
        
        # XMLType functions
        if 'XMLTYPE' in sql_upper or 'XMLELEMENT' in sql_upper or 'XMLFOREST' in sql_upper:
            unsupported.append(
                "XML functions - Use Databricks XML functions or from_xml/to_xml"
            )
        
        # Object types
        if re.search(r'\bTYPE\s+\w+\s+AS\s+OBJECT', sql_upper):
            unsupported.append(
                "Oracle object types - Use Databricks STRUCT types instead"
            )
        
        # TABLE() function for collections
        if re.search(r'\bTABLE\s*\(\s*\w+', sql_upper):
            unsupported.append(
                "TABLE() function for collections - Use EXPLODE() in Databricks"
            )
        
        # Autonomous transactions
        if 'PRAGMA AUTONOMOUS_TRANSACTION' in sql_upper:
            unsupported.append(
                "PRAGMA AUTONOMOUS_TRANSACTION - Not supported in Databricks"
            )
        
        # DBMS packages
        dbms_packages = re.findall(r'\bDBMS_\w+\.\w+', sql_upper)
        if dbms_packages:
            for pkg in set(dbms_packages):
                if pkg != 'DBMS_OUTPUT.PUT_LINE':
                    unsupported.append(
                        f"{pkg} - Oracle DBMS package not available in Databricks"
                    )
        
        # UTL packages
        utl_packages = re.findall(r'\bUTL_\w+\.\w+', sql_upper)
        if utl_packages:
            for pkg in set(utl_packages):
                unsupported.append(
                    f"{pkg} - Oracle UTL package not available, use Databricks alternatives"
                )
        
        # Sequences
        if '.NEXTVAL' in sql_upper or '.CURRVAL' in sql_upper:
            unsupported.append(
                "Oracle sequences (.NEXTVAL/.CURRVAL) - Use IDENTITY columns or custom sequence tables"
            )
        
        # ROWID
        if re.search(r'\bROWID\b', sql_upper):
            unsupported.append(
                "ROWID pseudo-column - No direct equivalent in Databricks"
            )
        
        # Analytic functions with unsupported syntax
        if 'KEEP' in sql_upper and ('FIRST' in sql_upper or 'LAST' in sql_upper):
            unsupported.append(
                "KEEP (DENSE_RANK FIRST/LAST) - Use window functions with FIRST_VALUE/LAST_VALUE"
            )
        
        # SAMPLE clause
        if re.search(r'\bSAMPLE\s*\(', sql_upper):
            unsupported.append(
                "SAMPLE clause - Use TABLESAMPLE in Databricks"
            )
        
        return unsupported
    
    def _analyze_sql_constructs(self, parsed: exp.Expression, original_sql: str) -> List[ConversionIssue]:
        """
        Analyze parsed SQL to identify constructs that may not convert properly.
        
        Args:
            parsed: Parsed sqlglot expression
            original_sql: Original SQL string
            
        Returns:
            List of ConversionIssue objects
        """
        issues = []
        
        # Check for ROWNUM usage
        for col in parsed.find_all(exp.Column):
            if col.name and col.name.upper() == 'ROWNUM':
                issues.append(ConversionIssue(
                    issue_type=ConversionIssueType.WARNING,
                    message="ROWNUM detected - semantics may differ from ROW_NUMBER()",
                    oracle_construct="ROWNUM",
                    suggestion="Review the conversion. ROWNUM in Oracle is evaluated before ORDER BY, "
                              "while ROW_NUMBER() is applied after. Consider using LIMIT/FETCH."
                ))
        
        # Check for DECODE (complex cases)
        for func in parsed.find_all(exp.Anonymous):
            if func.name and func.name.upper() == 'DECODE':
                args = list(func.args.get("expressions", []))
                if len(args) > 7:  # More than 3 condition pairs
                    issues.append(ConversionIssue(
                        issue_type=ConversionIssueType.INFO,
                        message="Complex DECODE with many conditions",
                        oracle_construct=f"DECODE with {(len(args)-1)//2} conditions",
                        suggestion="Consider using CASE expression for better readability"
                    ))
        
        # Check for NVL2
        for func in parsed.find_all(exp.Anonymous):
            if func.name and func.name.upper() == 'NVL2':
                issues.append(ConversionIssue(
                    issue_type=ConversionIssueType.INFO,
                    message="NVL2 function detected",
                    oracle_construct="NVL2(expr, val_if_not_null, val_if_null)",
                    suggestion="Converted to IF(expr IS NOT NULL, val_if_not_null, val_if_null)"
                ))
        
        # Check for date arithmetic
        original_upper = original_sql.upper()
        if re.search(r'\b\w+\s*[+-]\s*\d+\b', original_sql) and 'DATE' in original_upper:
            issues.append(ConversionIssue(
                issue_type=ConversionIssueType.WARNING,
                message="Date arithmetic detected - verify date operations",
                oracle_construct="date +/- number",
                suggestion="Oracle adds days by default. In Databricks, use DATE_ADD() or INTERVAL."
            ))
        
        # Check for LATERAL inline views and APPLY operators
        for lateral in parsed.find_all(exp.Lateral):
            lateral_args = lateral.args
            cross_apply = lateral_args.get('cross_apply')
            
            if cross_apply is True:
                # CROSS APPLY → INNER JOIN LATERAL
                issues.append(ConversionIssue(
                    issue_type=ConversionIssueType.INFO,
                    message="CROSS APPLY detected",
                    oracle_construct="CROSS APPLY (subquery)",
                    suggestion="Converted to INNER JOIN LATERAL - Databricks equivalent"
                ))
            elif cross_apply is False:
                # OUTER APPLY → LEFT JOIN LATERAL
                issues.append(ConversionIssue(
                    issue_type=ConversionIssueType.INFO,
                    message="OUTER APPLY detected",
                    oracle_construct="OUTER APPLY (subquery)",
                    suggestion="Converted to LEFT JOIN LATERAL - Databricks equivalent"
                ))
            else:
                # Plain LATERAL (comma join style)
                issues.append(ConversionIssue(
                    issue_type=ConversionIssueType.INFO,
                    message="LATERAL inline view detected",
                    oracle_construct="LATERAL (subquery)",
                    suggestion="LATERAL is supported in Databricks - correlated subquery in FROM clause"
                ))
        
        return issues
    
    def _convert_connect_by_query(self, sql: str, original_sql: str) -> Optional[TranslationResult]:
        """
        Convert Oracle CONNECT BY hierarchical query to Databricks recursive CTE.
        
        Args:
            sql: SQL without comments
            original_sql: Original SQL with comments
            
        Returns:
            TranslationResult if conversion successful, None to fall back to regular parsing
        """
        converter = ConnectByConverter()
        result = converter.convert(sql)
        
        if not result.success:
            # Fall back to regular parsing (will likely produce warning)
            return None
        
        # Build issues list
        issues = []
        warnings = []
        
        for warning in result.warnings:
            warnings.append(warning)
            issues.append(ConversionIssue(
                issue_type=ConversionIssueType.WARNING,
                message=warning,
                oracle_construct="CONNECT BY",
                suggestion="Review the converted recursive CTE"
            ))
        
        for note in result.notes:
            issues.append(ConversionIssue(
                issue_type=ConversionIssueType.INFO,
                message=note,
                oracle_construct="CONNECT BY",
                suggestion="Converted to recursive CTE"
            ))
        
        # Apply additional post-processing to the converted SQL (including custom rules)
        translated, _ = apply_string_transformations(
            result.converted_sql,
            self.custom_rules_config
        )
        translated = self._post_process(translated)
        
        return TranslationResult(
            original_sql=original_sql,
            translated_sql=translated,
            success=True,
            errors=[],
            warnings=warnings,
            issues=issues,
            unsupported_features=[]
        )
    
    def _check_translation_quality(self, original: str, translated: str) -> List[ConversionIssue]:
        """
        Check the quality of the translation and identify potential issues.
        
        Args:
            original: Original SQL
            translated: Translated SQL
            
        Returns:
            List of ConversionIssue objects
        """
        issues = []
        original_upper = original.upper()
        translated_upper = translated.upper()
        
        # Check if CONNECT BY survived (it shouldn't work properly)
        if 'CONNECT BY' in translated_upper:
            issues.append(ConversionIssue(
                issue_type=ConversionIssueType.WARNING,
                message="CONNECT BY was not converted - hierarchical query needs manual conversion",
                oracle_construct="CONNECT BY",
                suggestion="Convert to recursive CTE: WITH RECURSIVE cte AS (anchor UNION ALL recursive)"
            ))
        
        # Check if ROWNUM survived without conversion
        if 'ROWNUM' in translated_upper and 'ROWNUM' in original_upper:
            issues.append(ConversionIssue(
                issue_type=ConversionIssueType.WARNING,
                message="ROWNUM may not work as expected in Databricks",
                oracle_construct="ROWNUM",
                suggestion="Replace with LIMIT clause or ROW_NUMBER() window function"
            ))
        
        # Check for Oracle-specific functions that survived
        oracle_functions = ['DECODE', 'NVL2', 'USERENV', 'SYS_CONTEXT']
        for func in oracle_functions:
            if func in translated_upper and func in original_upper:
                issues.append(ConversionIssue(
                    issue_type=ConversionIssueType.PARTIAL,
                    message=f"{func} function may not be fully supported in Databricks",
                    oracle_construct=func,
                    suggestion=f"Verify {func} behavior or use Databricks equivalent"
                ))
        
        return issues
    
    def translate_script(self, script: str) -> List[TranslationResult]:
        """
        Translate an Oracle SQL script containing multiple statements.
        
        Args:
            script: Oracle SQL script with multiple statements
            
        Returns:
            List of TranslationResult for each statement
        """
        results = []
        
        # Split script into individual statements with line numbers
        statements_with_lines = self._split_statements_with_lines(script)
        
        for stmt, line_num in statements_with_lines:
            stmt = stmt.strip()
            if stmt and self._has_sql_content(stmt):
                result = self.translate(stmt)
                # Store line number in result for reporting
                result.line_number = line_num
                results.append(result)
        
        return results
    
    def _has_sql_content(self, stmt: str) -> bool:
        """
        Check if a statement contains actual SQL content (not just comments).
        
        Args:
            stmt: SQL statement to check
            
        Returns:
            True if statement contains SQL, False if only comments
        """
        # Strip all comments (single-line and multi-line)
        stripped = strip_sql_comments(stmt)
        
        # Check if any SQL content remains after stripping whitespace
        remaining = stripped.strip()
        return bool(remaining)
    
    def translate_file(self, input_path: str, output_path: Optional[str] = None) -> List[TranslationResult]:
        """
        Translate an Oracle SQL file to Databricks SQL.
        
        Args:
            input_path: Path to input Oracle SQL file
            output_path: Optional path to output Databricks SQL file
            
        Returns:
            List of TranslationResult for each statement
        """
        with open(input_path, 'r', encoding='utf-8') as f:
            script = f.read()
        
        results = self.translate_script(script)
        
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                for result in results:
                    if result.warnings:
                        for warning in result.warnings:
                            f.write(f"-- WARNING: {warning}\n")
                    if result.success:
                        f.write(result.translated_sql)
                        f.write(";\n\n")
                    else:
                        f.write(f"-- TRANSLATION FAILED:\n")
                        for error in result.errors:
                            f.write(f"-- {error}\n")
                        f.write(f"-- Original SQL:\n")
                        for line in result.original_sql.split("\n"):
                            f.write(f"-- {line}\n")
                        f.write("\n")
        
        return results
    
    def _remove_dual(self, expression: exp.Expression) -> exp.Expression:
        """
        Remove FROM DUAL clause as Databricks doesn't require it.
        
        Args:
            expression: Parsed SQL expression
            
        Returns:
            Expression with DUAL table removed
        """
        # Find and remove the FROM clause if it only contains DUAL
        if isinstance(expression, exp.Select):
            from_clause = expression.find(exp.From)
            if from_clause:
                # Check if the only table is DUAL
                tables = list(from_clause.find_all(exp.Table))
                if len(tables) == 1 and tables[0].name.upper() == "DUAL":
                    # Remove the FROM clause entirely
                    expression.set("from", None)
        
        return expression
    
    def _split_statements(self, script: str) -> List[str]:
        """
        Split a SQL script into individual statements.
        
        Handles:
        - Semicolon-delimited statements for regular SQL
        - PL/SQL blocks terminated by / (SQL*Plus terminator)
        - CREATE PROCEDURE/FUNCTION/PACKAGE blocks
        - Nested BEGIN/END in package bodies
        
        For PL/SQL blocks, the primary terminator is / on its own line.
        For blocks without /, we track BEGIN/END depth.
        
        Args:
            script: SQL script to split
            
        Returns:
            List of individual SQL statements
        """
        statements = []
        current_stmt = []
        in_plsql_block = False
        begin_count_total = 0
        plsql_type = None  # Track: PACKAGE, PACKAGE_BODY, PROCEDURE, ANONYMOUS
        plsql_name = None  # Track the name of the package/procedure
        
        lines = script.split('\n')
        
        for line in lines:
            stripped = line.strip()
            
            # Check for SQL*Plus terminator (/ on its own line)
            if stripped == '/':
                if current_stmt:
                    stmt = '\n'.join(current_stmt).strip()
                    if stmt:
                        statements.append(stmt)
                    current_stmt = []
                    in_plsql_block = False
                    begin_count_total = 0
                    plsql_type = None
                    plsql_name = None
                continue
            
            # Get the non-comment portion of the line for analysis
            line_without_comments = strip_sql_comments(stripped)
            upper_stripped = line_without_comments.upper().strip()
            
            # Skip if the line is only comments but keep them in the statement
            if not upper_stripped and stripped and (stripped.startswith('--') or stripped.startswith('/*')):
                current_stmt.append(line)
                continue
            
            # Skip empty lines but keep them if building a statement
            if not upper_stripped:
                if current_stmt:
                    current_stmt.append(line)
                continue
            
            # Detect start of PL/SQL block and its type
            if not in_plsql_block:
                # Extract object name for matching END statement
                pkg_body_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+BODY\s+(\w+)', upper_stripped)
                pkg_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+(\w+)', upper_stripped)
                proc_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION|TRIGGER)\s+(\w+)', upper_stripped)
                
                if pkg_body_match:
                    in_plsql_block = True
                    plsql_type = 'PACKAGE_BODY'
                    plsql_name = pkg_body_match.group(1)
                elif pkg_match and 'BODY' not in upper_stripped:
                    in_plsql_block = True
                    plsql_type = 'PACKAGE'
                    plsql_name = pkg_match.group(1)
                elif proc_match:
                    in_plsql_block = True
                    plsql_type = 'PROCEDURE'
                    plsql_name = proc_match.group(1)
                elif upper_stripped.startswith('DECLARE') or upper_stripped == 'BEGIN':
                    in_plsql_block = True
                    plsql_type = 'ANONYMOUS'
                    plsql_name = None
            
            current_stmt.append(line)
            
            if in_plsql_block:
                # Count BEGIN statements
                begin_count = len(re.findall(r'\bBEGIN\b', upper_stripped))
                begin_count_total += begin_count
                
                # Count END statements
                end_count = len(re.findall(r'\bEND\s*\w*\s*;', upper_stripped))
                begin_count_total -= end_count
                
                # Check for termination: END object_name; at proper depth
                # For package specs (no BEGIN), check END package_name;
                # For package bodies and procedures, need BEGIN depth = 0 AND END name;
                
                is_terminated = False
                
                if plsql_type == 'PACKAGE' and plsql_name:
                    # Package specs have no BEGIN, just END pkg_name;
                    if re.search(rf'\bEND\s+{plsql_name}\s*;\s*$', upper_stripped, re.IGNORECASE):
                        is_terminated = True
                elif plsql_name:
                    # Package body, procedure, function, trigger: END name; at depth 0
                    if begin_count_total <= 0 and re.search(rf'\bEND\s+{plsql_name}\s*;\s*$', upper_stripped, re.IGNORECASE):
                        is_terminated = True
                else:
                    # Anonymous block: END; at depth 0
                    if begin_count_total <= 0 and re.search(r'\bEND\s*;\s*$', upper_stripped):
                        is_terminated = True
                
                if is_terminated:
                    stmt = '\n'.join(current_stmt).strip()
                    if stmt:
                        statements.append(stmt)
                    current_stmt = []
                    in_plsql_block = False
                    begin_count_total = 0
                    plsql_type = None
                    plsql_name = None
            else:
                # Regular SQL statement - ends with semicolon
                if line_without_comments.strip().endswith(';'):
                    stmt = '\n'.join(current_stmt).strip()
                    stmt = stmt.rstrip(';').strip()
                    if stmt:
                        statements.append(stmt)
                    current_stmt = []
        
        # Handle any remaining statement
        if current_stmt:
            stmt = '\n'.join(current_stmt).strip()
            stmt = re.sub(r'\s*/\s*$', '', stmt)
            stmt = stmt.rstrip(';').strip()
            if stmt:
                statements.append(stmt)
        
        return [s for s in statements if s.strip()]
    
    def _split_statements_with_lines(self, script: str) -> List[Tuple[str, int]]:
        """
        Split a SQL script into individual statements with their starting line numbers.
        
        Uses the same logic as _split_statements but tracks line numbers.
        
        Args:
            script: SQL script to split
            
        Returns:
            List of tuples (statement, line_number) where line_number is 1-based
        """
        statements_with_lines = []
        current_stmt = []
        current_start_line = None
        in_plsql_block = False
        begin_count_total = 0
        plsql_type = None
        plsql_name = None
        
        lines = script.split('\n')
        
        for line_idx, line in enumerate(lines, 1):
            stripped = line.strip()
            
            # Check for SQL*Plus terminator (/ on its own line)
            if stripped == '/':
                if current_stmt:
                    stmt = '\n'.join(current_stmt).strip()
                    if stmt:
                        statements_with_lines.append((stmt, current_start_line or line_idx))
                    current_stmt = []
                    current_start_line = None
                    in_plsql_block = False
                    begin_count_total = 0
                    plsql_type = None
                    plsql_name = None
                continue
            
            line_without_comments = strip_sql_comments(stripped)
            upper_stripped = line_without_comments.upper().strip()
            
            # Keep comments in the statement
            if not upper_stripped and stripped and (stripped.startswith('--') or stripped.startswith('/*')):
                if current_stmt:
                    current_stmt.append(line)
                continue
            
            # Keep empty lines if building a statement
            if not upper_stripped:
                if current_stmt:
                    current_stmt.append(line)
                continue
            
            # Track start line of new statement
            if not current_stmt:
                current_start_line = line_idx
            
            # Detect start of PL/SQL block and its type
            if not in_plsql_block:
                pkg_body_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+BODY\s+(\w+)', upper_stripped)
                pkg_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+(\w+)', upper_stripped)
                proc_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION|TRIGGER)\s+(\w+)', upper_stripped)
                
                if pkg_body_match:
                    in_plsql_block = True
                    plsql_type = 'PACKAGE_BODY'
                    plsql_name = pkg_body_match.group(1)
                elif pkg_match and 'BODY' not in upper_stripped:
                    in_plsql_block = True
                    plsql_type = 'PACKAGE'
                    plsql_name = pkg_match.group(1)
                elif proc_match:
                    in_plsql_block = True
                    plsql_type = 'PROCEDURE'
                    plsql_name = proc_match.group(1)
                elif upper_stripped.startswith('DECLARE') or upper_stripped == 'BEGIN':
                    in_plsql_block = True
                    plsql_type = 'ANONYMOUS'
                    plsql_name = None
            
            current_stmt.append(line)
            
            if in_plsql_block:
                begin_count = len(re.findall(r'\bBEGIN\b', upper_stripped))
                begin_count_total += begin_count
                
                end_count = len(re.findall(r'\bEND\s*\w*\s*;', upper_stripped))
                begin_count_total -= end_count
                
                is_terminated = False
                
                if plsql_type == 'PACKAGE' and plsql_name:
                    if re.search(rf'\bEND\s+{plsql_name}\s*;\s*$', upper_stripped, re.IGNORECASE):
                        is_terminated = True
                elif plsql_name:
                    if begin_count_total <= 0 and re.search(rf'\bEND\s+{plsql_name}\s*;\s*$', upper_stripped, re.IGNORECASE):
                        is_terminated = True
                else:
                    if begin_count_total <= 0 and re.search(r'\bEND\s*;\s*$', upper_stripped):
                        is_terminated = True
                
                if is_terminated:
                    stmt = '\n'.join(current_stmt).strip()
                    if stmt:
                        statements_with_lines.append((stmt, current_start_line or line_idx))
                    current_stmt = []
                    current_start_line = None
                    in_plsql_block = False
                    begin_count_total = 0
                    plsql_type = None
                    plsql_name = None
            else:
                if line_without_comments.strip().endswith(';'):
                    stmt = '\n'.join(current_stmt).strip()
                    stmt = stmt.rstrip(';').strip()
                    if stmt:
                        statements_with_lines.append((stmt, current_start_line or line_idx))
                    current_stmt = []
                    current_start_line = None
        
        # Handle any remaining statement
        if current_stmt:
            stmt = '\n'.join(current_stmt).strip()
            stmt = re.sub(r'\s*/\s*$', '', stmt)
            stmt = stmt.rstrip(';').strip()
            if stmt:
                statements_with_lines.append((stmt, current_start_line or 1))
        
        return [(s, ln) for s, ln in statements_with_lines if s.strip()]
    
    def _post_process(self, sql: str) -> str:
        """
        Apply post-processing to translated SQL.
        
        Args:
            sql: Translated SQL string
            
        Returns:
            Post-processed SQL string
        """
        # Remove any remaining DUAL references
        sql = re.sub(r'\s+FROM\s+DUAL\b', '', sql, flags=re.IGNORECASE)
        
        # Clean up extra whitespace
        sql = re.sub(r'\n\s*\n', '\n', sql)
        
        return sql.strip()
    
    def get_data_type_mapping(self, oracle_type: str, 
                               precision: int = None, 
                               scale: int = None) -> str:
        """
        Get Databricks data type for an Oracle data type.
        
        Args:
            oracle_type: Oracle data type name
            precision: Optional precision for numeric types
            scale: Optional scale for numeric types
            
        Returns:
            Databricks data type string
        """
        return get_databricks_data_type(oracle_type, precision, scale)

