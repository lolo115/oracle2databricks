"""
Oracle to Databricks SQL Translator

A comprehensive tool for translating Oracle SQL and PL/SQL code
to Databricks SQL using the sqlglot framework.
"""

from .translator import OracleToDatabricksTranslator, strip_sql_comments
from .plsql_converter import PLSQLConverter
from .function_detector import FunctionDetector
from .connect_by_converter import ConnectByConverter, convert_connect_by, has_connect_by
from .report_generator import (
    ConversionReport,
    ReportGenerator,
    build_conversion_report,
    build_unified_conversion_report,
    print_conversion_report,
    analyze_translation_result,
)

__version__ = "0.1.0"
__all__ = [
    "OracleToDatabricksTranslator",
    "PLSQLConverter",
    "FunctionDetector",
    "ConnectByConverter",
    "convert_connect_by",
    "has_connect_by",
    "strip_sql_comments",
    "ConversionReport",
    "ReportGenerator",
    "build_conversion_report",
    "build_unified_conversion_report",
    "print_conversion_report",
    "analyze_translation_result",
]

