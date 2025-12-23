"""
Conversion Report Generator for Oracle to Databricks SQL Translator.

This module handles the generation and formatting of conversion reports,
including statistics, analysis, and recommendations.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import Counter

from .function_detector import FunctionDetector, get_line_number


@dataclass
class FileLocation:
    """Represents a location in a source file."""
    file: str
    line: int
    
    def __str__(self) -> str:
        return f"{self.file}:{self.line}"


@dataclass
class ConversionReport:
    """Comprehensive conversion report."""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Overall statistics
    total_statements: int = 0
    successful_statements: int = 0
    failed_statements: int = 0
    partial_statements: int = 0  # Success with warnings
    
    # SQL-specific statistics
    sql_total: int = 0
    sql_successful: int = 0
    sql_failed: int = 0
    sql_partial: int = 0
    
    # PL/SQL-specific statistics
    plsql_total: int = 0
    plsql_successful: int = 0
    plsql_failed: int = 0
    plsql_partial: int = 0
    
    # Function-level analysis
    functions_detected: Dict[str, int] = field(default_factory=dict)
    functions_converted: Dict[str, int] = field(default_factory=dict)
    functions_unsupported: Dict[str, int] = field(default_factory=dict)
    
    # Functions/packages with NO Databricks equivalent (mapped to None)
    functions_no_equivalent: Dict[str, int] = field(default_factory=dict)
    packages_no_equivalent: Dict[str, int] = field(default_factory=dict)
    
    # Unknown/custom functions (not standard Oracle or Databricks)
    unknown_functions: Dict[str, int] = field(default_factory=dict)
    
    # Detailed lists
    converted_items: List[Dict[str, Any]] = field(default_factory=list)
    failed_items: List[Dict[str, Any]] = field(default_factory=list)
    partial_items: List[Dict[str, Any]] = field(default_factory=list)
    
    # Items with no equivalent (details)
    no_equivalent_items: List[Dict[str, Any]] = field(default_factory=list)
    
    # Unsupported features
    unsupported_features: Dict[str, int] = field(default_factory=dict)
    
    # Location tracking for detailed reporting (file + line number)
    # Maps feature/function name to list of FileLocation objects
    unsupported_features_locations: Dict[str, List[FileLocation]] = field(default_factory=dict)
    unknown_functions_locations: Dict[str, List[FileLocation]] = field(default_factory=dict)
    functions_no_equivalent_locations: Dict[str, List[FileLocation]] = field(default_factory=dict)
    packages_no_equivalent_locations: Dict[str, List[FileLocation]] = field(default_factory=dict)
    
    # Legacy line-only tracking (for backward compatibility)
    unsupported_features_lines: Dict[str, List[int]] = field(default_factory=dict)
    unknown_functions_lines: Dict[str, List[int]] = field(default_factory=dict)
    functions_no_equivalent_lines: Dict[str, List[int]] = field(default_factory=dict)
    packages_no_equivalent_lines: Dict[str, List[int]] = field(default_factory=dict)
    
    # Warnings
    warnings: List[str] = field(default_factory=list)
    
    @property
    def conversion_rate(self) -> float:
        """Calculate overall conversion rate as percentage."""
        if self.total_statements == 0:
            return 0.0
        return (self.successful_statements / self.total_statements) * 100
    
    @property
    def success_with_warnings_rate(self) -> float:
        """Calculate rate including partial successes."""
        if self.total_statements == 0:
            return 0.0
        return ((self.successful_statements + self.partial_statements) / self.total_statements) * 100
    
    @property
    def sql_conversion_rate(self) -> float:
        """Calculate SQL conversion rate as percentage."""
        if self.sql_total == 0:
            return 0.0
        return (self.sql_successful / self.sql_total) * 100
    
    @property
    def sql_success_with_warnings_rate(self) -> float:
        """Calculate SQL rate including partial successes."""
        if self.sql_total == 0:
            return 0.0
        return ((self.sql_successful + self.sql_partial) / self.sql_total) * 100
    
    @property
    def plsql_conversion_rate(self) -> float:
        """Calculate PL/SQL conversion rate as percentage."""
        if self.plsql_total == 0:
            return 0.0
        return (self.plsql_successful / self.plsql_total) * 100
    
    @property
    def plsql_success_with_warnings_rate(self) -> float:
        """Calculate PL/SQL rate including partial successes."""
        if self.plsql_total == 0:
            return 0.0
        return ((self.plsql_successful + self.plsql_partial) / self.plsql_total) * 100
    
    @property
    def total_no_equivalent(self) -> int:
        """Total count of functions/packages with no equivalent."""
        return sum(self.functions_no_equivalent.values()) + sum(self.packages_no_equivalent.values())
    
    @property
    def total_unknown(self) -> int:
        """Total count of unknown/custom functions."""
        return sum(self.unknown_functions.values())
    
    @property
    def total_requiring_attention(self) -> int:
        """Total items requiring manual attention."""
        return self.total_no_equivalent + self.total_unknown
    
    @property
    def has_sql(self) -> bool:
        """Check if report contains SQL conversions."""
        return self.sql_total > 0
    
    @property
    def has_plsql(self) -> bool:
        """Check if report contains PL/SQL conversions."""
        return self.plsql_total > 0


class ReportGenerator:
    """
    Generator for conversion reports.
    
    Provides methods to build and format conversion reports from
    translation and conversion results.
    
    Example:
        >>> generator = ReportGenerator()
        >>> report = generator.build_report(results)
        >>> generator.print_report(report, format='text')
    """
    
    # Report width for text formatting
    REPORT_WIDTH = 100
    
    def __init__(self):
        """Initialize the report generator."""
        pass
    
    @staticmethod
    def analyze_translation_result(result, original_sql: str, line_number: int = None) -> Dict[str, Any]:
        """
        Analyze a single translation result for reporting.
        
        Args:
            result: Translation result object
            original_sql: Original SQL statement
            line_number: Optional starting line number in source file
            
        Returns:
            Dictionary with analysis details
        """
        # Detect functions/packages with no equivalent and unknown functions
        funcs_no_equiv, pkgs_no_equiv, unknown_funcs = FunctionDetector.detect_functions_no_equivalent(original_sql)
        
        analysis = {
            'original_sql': original_sql[:200] + '...' if len(original_sql) > 200 else original_sql,
            'success': result.success,
            'has_warnings': bool(result.warnings),
            'functions_detected': list(FunctionDetector.detect_oracle_functions(original_sql)),
            'unsupported_constructs': list(FunctionDetector.detect_unsupported_constructs(original_sql)),
            'functions_no_equivalent': list(funcs_no_equiv),
            'packages_no_equivalent': list(pkgs_no_equiv),
            'unknown_functions': list(unknown_funcs),
            'errors': result.errors if hasattr(result, 'errors') else [],
            'warnings': result.warnings if hasattr(result, 'warnings') else [],
        }
        
        # Add line number if provided
        if line_number is not None:
            analysis['line_number'] = line_number
        
        if result.success:
            analysis['translated_sql'] = result.translated_sql[:200] + '...' if len(result.translated_sql) > 200 else result.translated_sql
        
        if hasattr(result, 'unsupported_features'):
            analysis['unsupported_features'] = result.unsupported_features
        
        if hasattr(result, 'issues'):
            analysis['issues'] = [
                {
                    'type': issue.issue_type.value,
                    'message': issue.message,
                    'suggestion': issue.suggestion
                }
                for issue in result.issues
            ]
        
        return analysis
    
    def build_report(self, results: List, original_sqls: List[str] = None, 
                     statement_line_numbers: List[int] = None,
                     source_file: str = None) -> ConversionReport:
        """
        Build a comprehensive conversion report from translation results.
        
        Args:
            results: List of translation results
            original_sqls: Optional list of original SQL statements
            statement_line_numbers: Optional list of starting line numbers for each statement
            source_file: Optional source file name for location tracking
            
        Returns:
            ConversionReport with all statistics and details
        """
        report = ConversionReport()
        
        all_functions_detected = Counter()
        all_functions_converted = Counter()
        all_functions_unsupported = Counter()
        all_functions_no_equiv = Counter()
        all_packages_no_equiv = Counter()
        all_unknown_functions = Counter()
        all_unsupported_features = Counter()
        
        # Location tracking (file + line number)
        all_unsupported_features_locations: Dict[str, List[FileLocation]] = {}
        all_unknown_functions_locations: Dict[str, List[FileLocation]] = {}
        all_functions_no_equiv_locations: Dict[str, List[FileLocation]] = {}
        all_packages_no_equiv_locations: Dict[str, List[FileLocation]] = {}
        
        # Line number tracking (for backward compatibility)
        all_unsupported_features_lines: Dict[str, List[int]] = {}
        all_unknown_functions_lines: Dict[str, List[int]] = {}
        all_functions_no_equiv_lines: Dict[str, List[int]] = {}
        all_packages_no_equiv_lines: Dict[str, List[int]] = {}
        
        # Use filename or "unknown" if not provided
        file_name = source_file if source_file else "unknown"
        
        # Count only non-empty results (skip comment-only statements)
        counted_results = 0
        
        for i, result in enumerate(results):
            original_sql = original_sqls[i] if original_sqls and i < len(original_sqls) else (
                result.original_sql if hasattr(result, 'original_sql') else ""
            )
            
            # Get base line number for this statement
            base_line = statement_line_numbers[i] if statement_line_numbers and i < len(statement_line_numbers) else 1
            
            # Skip empty/comment-only results (they have empty translated_sql and are marked successful)
            translated_sql = getattr(result, 'translated_sql', '') or ''
            if result.success and not translated_sql.strip():
                continue
            
            counted_results += 1
            
            analysis = self.analyze_translation_result(result, original_sql, base_line)
            
            # Categorize result
            if result.success:
                if result.warnings:
                    report.partial_statements += 1
                    report.partial_items.append(analysis)
                else:
                    report.successful_statements += 1
                    report.converted_items.append(analysis)
            else:
                report.failed_statements += 1
                report.failed_items.append(analysis)
            
            # Track functions
            detected_funcs = FunctionDetector.detect_oracle_functions(original_sql)
            for func in detected_funcs:
                all_functions_detected[func] += 1
                
                # Check if this function was likely converted (success or partial)
                if result.success:
                    # Check if function appears in unsupported features
                    unsupported_in_result = getattr(result, 'unsupported_features', [])
                    func_unsupported = any(func.upper() in str(f).upper() for f in unsupported_in_result)
                    
                    if func_unsupported:
                        all_functions_unsupported[func] += 1
                    else:
                        all_functions_converted[func] += 1
                else:
                    all_functions_unsupported[func] += 1
            
            # Track functions and packages with no Databricks equivalent, plus unknown functions (with line numbers)
            funcs_no_equiv_lines, pkgs_no_equiv_lines, unknown_funcs_lines = \
                FunctionDetector.detect_functions_no_equivalent_with_lines(original_sql, base_line)
            
            for func, lines in funcs_no_equiv_lines.items():
                all_functions_no_equiv[func] += len(lines)
                if func not in all_functions_no_equiv_lines:
                    all_functions_no_equiv_lines[func] = []
                all_functions_no_equiv_lines[func].extend(lines)
                # Also track file locations
                if func not in all_functions_no_equiv_locations:
                    all_functions_no_equiv_locations[func] = []
                all_functions_no_equiv_locations[func].extend([FileLocation(file_name, ln) for ln in lines])
            
            for pkg, lines in pkgs_no_equiv_lines.items():
                all_packages_no_equiv[pkg] += len(lines)
                if pkg not in all_packages_no_equiv_lines:
                    all_packages_no_equiv_lines[pkg] = []
                all_packages_no_equiv_lines[pkg].extend(lines)
                # Also track file locations
                if pkg not in all_packages_no_equiv_locations:
                    all_packages_no_equiv_locations[pkg] = []
                all_packages_no_equiv_locations[pkg].extend([FileLocation(file_name, ln) for ln in lines])
            
            for func, lines in unknown_funcs_lines.items():
                all_unknown_functions[func] += len(lines)
                if func not in all_unknown_functions_lines:
                    all_unknown_functions_lines[func] = []
                all_unknown_functions_lines[func].extend(lines)
                # Also track file locations
                if func not in all_unknown_functions_locations:
                    all_unknown_functions_locations[func] = []
                all_unknown_functions_locations[func].extend([FileLocation(file_name, ln) for ln in lines])
            
            # Track statements with no-equivalent items or unknown functions
            if funcs_no_equiv_lines or pkgs_no_equiv_lines or unknown_funcs_lines:
                no_equiv_item = {
                    'original_sql': original_sql[:150] + '...' if len(original_sql) > 150 else original_sql,
                    'functions': list(funcs_no_equiv_lines.keys()),
                    'packages': list(pkgs_no_equiv_lines.keys()),
                    'unknown_functions': list(unknown_funcs_lines.keys()),
                    'line_numbers': {
                        **funcs_no_equiv_lines,
                        **pkgs_no_equiv_lines,
                        **unknown_funcs_lines,
                    },
                    'file': file_name,
                    'suggestions': {
                        **{f: FunctionDetector.get_equivalent_suggestion(f) for f in funcs_no_equiv_lines},
                        **{p: FunctionDetector.get_equivalent_suggestion(p) for p in pkgs_no_equiv_lines},
                        **{f: 'Custom/internal function - must be migrated manually' for f in unknown_funcs_lines},
                    }
                }
                report.no_equivalent_items.append(no_equiv_item)
            
            # Track unsupported constructs (with line numbers)
            unsupported_with_lines = FunctionDetector.detect_unsupported_constructs_with_lines(original_sql, base_line)
            for construct, lines in unsupported_with_lines.items():
                all_unsupported_features[construct] += len(lines)
                if construct not in all_unsupported_features_lines:
                    all_unsupported_features_lines[construct] = []
                all_unsupported_features_lines[construct].extend(lines)
                # Also track file locations
                if construct not in all_unsupported_features_locations:
                    all_unsupported_features_locations[construct] = []
                all_unsupported_features_locations[construct].extend([FileLocation(file_name, ln) for ln in lines])
            
            # Also add unsupported features from result (line number from base_line)
            if hasattr(result, 'unsupported_features'):
                for feature in result.unsupported_features:
                    all_unsupported_features[feature] += 1
                    if feature not in all_unsupported_features_lines:
                        all_unsupported_features_lines[feature] = []
                    all_unsupported_features_lines[feature].append(base_line)
                    # Also track file locations
                    if feature not in all_unsupported_features_locations:
                        all_unsupported_features_locations[feature] = []
                    all_unsupported_features_locations[feature].append(FileLocation(file_name, base_line))
            
            # Collect warnings
            if hasattr(result, 'warnings'):
                report.warnings.extend(result.warnings)
        
        report.total_statements = counted_results
        report.functions_detected = dict(all_functions_detected)
        report.functions_converted = dict(all_functions_converted)
        report.functions_unsupported = dict(all_functions_unsupported)
        report.functions_no_equivalent = dict(all_functions_no_equiv)
        report.packages_no_equivalent = dict(all_packages_no_equiv)
        report.unknown_functions = dict(all_unknown_functions)
        report.unsupported_features = dict(all_unsupported_features)
        
        # Assign line number tracking (for backward compatibility)
        report.unsupported_features_lines = all_unsupported_features_lines
        report.unknown_functions_lines = all_unknown_functions_lines
        report.functions_no_equivalent_lines = all_functions_no_equiv_lines
        report.packages_no_equivalent_lines = all_packages_no_equiv_lines
        
        # Assign file+line location tracking
        report.unsupported_features_locations = all_unsupported_features_locations
        report.unknown_functions_locations = all_unknown_functions_locations
        report.functions_no_equivalent_locations = all_functions_no_equiv_locations
        report.packages_no_equivalent_locations = all_packages_no_equiv_locations
        
        return report
    
    def build_unified_report(self, sql_results: List, plsql_results: List,
                              source_file: str = None) -> ConversionReport:
        """
        Build a unified conversion report from both SQL and PL/SQL results.
        
        Args:
            sql_results: List of SQL translation results
            plsql_results: List of PL/SQL conversion results
            source_file: Optional source file name for location tracking
            
        Returns:
            ConversionReport with combined statistics
        """
        report = ConversionReport()
        
        # Use filename or "unknown" if not provided
        file_name = source_file if source_file else "unknown"
        
        # Process SQL results
        for result in sql_results:
            original_sql = result.original_sql if hasattr(result, 'original_sql') else ""
            base_line = getattr(result, 'line_number', 1) or 1
            
            # Skip empty/comment-only results (they have empty translated_sql and are marked successful)
            translated_sql = getattr(result, 'translated_sql', '') or ''
            if result.success and not translated_sql.strip():
                continue
            
            report.sql_total += 1
            report.total_statements += 1
            
            analysis = self.analyze_translation_result(result, original_sql, base_line)
            
            if result.success:
                if result.warnings:
                    report.sql_partial += 1
                    report.partial_statements += 1
                    report.partial_items.append(analysis)
                else:
                    report.sql_successful += 1
                    report.successful_statements += 1
                    report.converted_items.append(analysis)
            else:
                report.sql_failed += 1
                report.failed_statements += 1
                report.failed_items.append(analysis)
            
            # Track functions
            detected_funcs = FunctionDetector.detect_oracle_functions(original_sql)
            for func in detected_funcs:
                report.functions_detected[func] = report.functions_detected.get(func, 0) + 1
                if result.success:
                    report.functions_converted[func] = report.functions_converted.get(func, 0) + 1
                else:
                    report.functions_unsupported[func] = report.functions_unsupported.get(func, 0) + 1
            
            # Track no-equivalent items with line numbers and file locations
            funcs_no_equiv_lines, pkgs_no_equiv_lines, unknown_funcs_lines = \
                FunctionDetector.detect_functions_no_equivalent_with_lines(original_sql, base_line)
            
            for func, lines in funcs_no_equiv_lines.items():
                report.functions_no_equivalent[func] = report.functions_no_equivalent.get(func, 0) + len(lines)
                if func not in report.functions_no_equivalent_lines:
                    report.functions_no_equivalent_lines[func] = []
                report.functions_no_equivalent_lines[func].extend(lines)
                # Also track file locations
                if func not in report.functions_no_equivalent_locations:
                    report.functions_no_equivalent_locations[func] = []
                report.functions_no_equivalent_locations[func].extend([FileLocation(file_name, ln) for ln in lines])
            
            for pkg, lines in pkgs_no_equiv_lines.items():
                report.packages_no_equivalent[pkg] = report.packages_no_equivalent.get(pkg, 0) + len(lines)
                if pkg not in report.packages_no_equivalent_lines:
                    report.packages_no_equivalent_lines[pkg] = []
                report.packages_no_equivalent_lines[pkg].extend(lines)
                # Also track file locations
                if pkg not in report.packages_no_equivalent_locations:
                    report.packages_no_equivalent_locations[pkg] = []
                report.packages_no_equivalent_locations[pkg].extend([FileLocation(file_name, ln) for ln in lines])
            
            for func, lines in unknown_funcs_lines.items():
                report.unknown_functions[func] = report.unknown_functions.get(func, 0) + len(lines)
                if func not in report.unknown_functions_lines:
                    report.unknown_functions_lines[func] = []
                report.unknown_functions_lines[func].extend(lines)
                # Also track file locations
                if func not in report.unknown_functions_locations:
                    report.unknown_functions_locations[func] = []
                report.unknown_functions_locations[func].extend([FileLocation(file_name, ln) for ln in lines])
            
            # Track unsupported constructs with line numbers and file locations
            unsupported_with_lines = FunctionDetector.detect_unsupported_constructs_with_lines(original_sql, base_line)
            for construct, lines in unsupported_with_lines.items():
                report.unsupported_features[construct] = report.unsupported_features.get(construct, 0) + len(lines)
                if construct not in report.unsupported_features_lines:
                    report.unsupported_features_lines[construct] = []
                report.unsupported_features_lines[construct].extend(lines)
                # Also track file locations
                if construct not in report.unsupported_features_locations:
                    report.unsupported_features_locations[construct] = []
                report.unsupported_features_locations[construct].extend([FileLocation(file_name, ln) for ln in lines])
            
            if hasattr(result, 'warnings'):
                report.warnings.extend(result.warnings)
        
        # Process PL/SQL results
        for result in plsql_results:
            original_code = result.original_code if hasattr(result, 'original_code') else ""
            
            # Skip empty/comment-only results
            converted_code = getattr(result, 'converted_code', '') or ''
            if result.success and not converted_code.strip():
                continue
            
            report.plsql_total += 1
            report.total_statements += 1
            
            # Base line for PL/SQL is 1 (could be enhanced to track actual positions)
            base_line = 1
            
            item = {
                'original_sql': f"{result.object_type.value}: {result.object_name}",
                'success': result.success,
                'has_warnings': bool(result.warnings),
                'functions_detected': list(FunctionDetector.detect_oracle_functions(original_code)),
                'unsupported_constructs': list(FunctionDetector.detect_unsupported_constructs(original_code)),
                'errors': result.errors,
                'warnings': result.warnings,
            }
            
            if result.success:
                if result.warnings or result.manual_review_required:
                    report.plsql_partial += 1
                    report.partial_statements += 1
                    report.partial_items.append(item)
                else:
                    report.plsql_successful += 1
                    report.successful_statements += 1
                    report.converted_items.append(item)
            else:
                report.plsql_failed += 1
                report.failed_statements += 1
                report.failed_items.append(item)
            
            # Track functions
            for func in item['functions_detected']:
                report.functions_detected[func] = report.functions_detected.get(func, 0) + 1
                if result.success:
                    report.functions_converted[func] = report.functions_converted.get(func, 0) + 1
                else:
                    report.functions_unsupported[func] = report.functions_unsupported.get(func, 0) + 1
            
            # Track no-equivalent items with line numbers and file locations
            funcs_no_equiv_lines, pkgs_no_equiv_lines, unknown_funcs_lines = \
                FunctionDetector.detect_functions_no_equivalent_with_lines(original_code, base_line)
            
            for func, lines in funcs_no_equiv_lines.items():
                report.functions_no_equivalent[func] = report.functions_no_equivalent.get(func, 0) + len(lines)
                if func not in report.functions_no_equivalent_lines:
                    report.functions_no_equivalent_lines[func] = []
                report.functions_no_equivalent_lines[func].extend(lines)
                # Also track file locations
                if func not in report.functions_no_equivalent_locations:
                    report.functions_no_equivalent_locations[func] = []
                report.functions_no_equivalent_locations[func].extend([FileLocation(file_name, ln) for ln in lines])
            
            for pkg, lines in pkgs_no_equiv_lines.items():
                report.packages_no_equivalent[pkg] = report.packages_no_equivalent.get(pkg, 0) + len(lines)
                if pkg not in report.packages_no_equivalent_lines:
                    report.packages_no_equivalent_lines[pkg] = []
                report.packages_no_equivalent_lines[pkg].extend(lines)
                # Also track file locations
                if pkg not in report.packages_no_equivalent_locations:
                    report.packages_no_equivalent_locations[pkg] = []
                report.packages_no_equivalent_locations[pkg].extend([FileLocation(file_name, ln) for ln in lines])
            
            for func, lines in unknown_funcs_lines.items():
                report.unknown_functions[func] = report.unknown_functions.get(func, 0) + len(lines)
                if func not in report.unknown_functions_lines:
                    report.unknown_functions_lines[func] = []
                report.unknown_functions_lines[func].extend(lines)
                # Also track file locations
                if func not in report.unknown_functions_locations:
                    report.unknown_functions_locations[func] = []
                report.unknown_functions_locations[func].extend([FileLocation(file_name, ln) for ln in lines])
            
            # Track unsupported constructs with line numbers and file locations
            unsupported_with_lines = FunctionDetector.detect_unsupported_constructs_with_lines(original_code, base_line)
            for construct, lines in unsupported_with_lines.items():
                report.unsupported_features[construct] = report.unsupported_features.get(construct, 0) + len(lines)
                if construct not in report.unsupported_features_lines:
                    report.unsupported_features_lines[construct] = []
                report.unsupported_features_lines[construct].extend(lines)
                # Also track file locations
                if construct not in report.unsupported_features_locations:
                    report.unsupported_features_locations[construct] = []
                report.unsupported_features_locations[construct].extend([FileLocation(file_name, ln) for ln in lines])
            
            report.warnings.extend(result.warnings)
            if hasattr(result, 'manual_review_required'):
                report.warnings.extend(result.manual_review_required)
        
        return report
    
    def print_report(self, report: ConversionReport, output_format: str = 'text', output_file: str = None):
        """
        Print the conversion report in the specified format.
        
        Args:
            report: ConversionReport to print
            output_format: 'text' or 'json'
            output_file: Optional file path to write the report
        """
        if output_format == 'json':
            self._print_json_report(report, output_file)
        else:
            self._print_text_report(report, output_file)
    
    def _print_json_report(self, report: ConversionReport, output_file: str = None):
        """Print report in JSON format."""
        report_dict = {
            'timestamp': report.timestamp,
            'summary': {
                'total_statements': report.total_statements,
                'successful': report.successful_statements,
                'failed': report.failed_statements,
                'partial': report.partial_statements,
                'conversion_rate': f"{report.conversion_rate:.1f}%",
                'success_with_warnings_rate': f"{report.success_with_warnings_rate:.1f}%"
            },
            'functions': {
                'detected': report.functions_detected,
                'converted': report.functions_converted,
                'unsupported': report.functions_unsupported,
                'no_equivalent': report.functions_no_equivalent,
                'unknown_custom': report.unknown_functions
            },
            'packages_no_equivalent': report.packages_no_equivalent,
            'unknown_functions': report.unknown_functions,
            'no_equivalent_details': [
                {
                    'sql': item['original_sql'],
                    'functions': item.get('functions', []),
                    'packages': item.get('packages', []),
                    'unknown_functions': item.get('unknown_functions', []),
                    'suggestions': item.get('suggestions', {})
                }
                for item in report.no_equivalent_items[:20]
            ],
            'unsupported_features': report.unsupported_features,
            'failed_items': report.failed_items[:20],  # Limit for readability
            'partial_items': report.partial_items[:20],
        }
        output = json.dumps(report_dict, indent=2)
        if output_file:
            with open(output_file, 'w') as f:
                f.write(output)
            print(f"Report written to: {output_file}")
        else:
            print(output)
    
    def _print_text_report(self, report: ConversionReport, output_file: str = None):
        """Print report in text format."""
        W = self.REPORT_WIDTH
        lines = []
        lines.append("")
        lines.append("╔" + "═" * W + "╗")
        lines.append("║" + " ORACLE TO DATABRICKS CONVERSION REPORT ".center(W) + "║")
        lines.append("╚" + "═" * W + "╝")
        lines.append(f"  Generated: {report.timestamp}")
        lines.append("")
        
        # Overall Summary
        lines.append("┌" + "─" * W + "┐")
        lines.append("│" + " CONVERSION SUMMARY ".center(W) + "│")
        lines.append("└" + "─" * W + "┘")
        
        # Calculate bar visualization for strict conversion rate (fully converted only)
        rate = report.conversion_rate
        bar_width = 50
        filled = int(bar_width * rate / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        
        # Calculate bar visualization for success with warnings rate
        rate_with_warnings = report.success_with_warnings_rate
        filled_warnings = int(bar_width * rate_with_warnings / 100)
        bar_warnings = "█" * filled_warnings + "░" * (bar_width - filled_warnings)
        
        lines.append(f"  Strict Conversion Rate:       [{bar}] {rate:.1f}%")
        lines.append(f"  Success (incl. warnings):     [{bar_warnings}] {rate_with_warnings:.1f}%")
        lines.append("")
        lines.append(f"  Total Items:                  {report.total_statements:>8}")
        lines.append(f"  ✓ Fully Converted:            {report.successful_statements:>8}  ({report.successful_statements/max(report.total_statements,1)*100:>5.1f}%)")
        lines.append(f"  ⚠ Converted (with warnings):  {report.partial_statements:>8}  ({report.partial_statements/max(report.total_statements,1)*100:>5.1f}%)")
        lines.append(f"  ✗ Failed:                     {report.failed_statements:>8}  ({report.failed_statements/max(report.total_statements,1)*100:>5.1f}%)")
        
        # SQL and PL/SQL breakdown (if both present)
        if report.has_sql or report.has_plsql:
            lines.append("")
            lines.append("┌" + "─" * W + "┐")
            lines.append("│" + " BREAKDOWN BY TYPE ".center(W) + "│")
            lines.append("└" + "─" * W + "┘")
            
            if report.has_sql:
                sql_bar_width = 30
                sql_rate = report.sql_conversion_rate
                sql_filled = int(sql_bar_width * sql_rate / 100)
                sql_bar = "█" * sql_filled + "░" * (sql_bar_width - sql_filled)
                sql_rate_warn = report.sql_success_with_warnings_rate
                
                lines.append(f"  SQL STATEMENTS")
                lines.append(f"    Total: {report.sql_total:>5}  |  Strict: [{sql_bar}] {sql_rate:>5.1f}%  |  With warnings: {sql_rate_warn:>5.1f}%")
                lines.append(f"    ✓ Success: {report.sql_successful:>5}  |  ⚠ Partial: {report.sql_partial:>5}  |  ✗ Failed: {report.sql_failed:>5}")
            
            if report.has_sql and report.has_plsql:
                lines.append("")
            
            if report.has_plsql:
                plsql_bar_width = 30
                plsql_rate = report.plsql_conversion_rate
                plsql_filled = int(plsql_bar_width * plsql_rate / 100)
                plsql_bar = "█" * plsql_filled + "░" * (plsql_bar_width - plsql_filled)
                plsql_rate_warn = report.plsql_success_with_warnings_rate
                
                lines.append(f"  PL/SQL OBJECTS")
                lines.append(f"    Total: {report.plsql_total:>5}  |  Strict: [{plsql_bar}] {plsql_rate:>5.1f}%  |  With warnings: {plsql_rate_warn:>5.1f}%")
                lines.append(f"    ✓ Success: {report.plsql_successful:>5}  |  ⚠ Partial: {report.plsql_partial:>5}  |  ✗ Failed: {report.plsql_failed:>5}")
        
        lines.append("")
        
        # Functions Analysis
        if report.functions_detected:
            lines.append("┌" + "─" * W + "┐")
            lines.append("│" + " ORACLE FUNCTIONS ANALYSIS ".center(W) + "│")
            lines.append("└" + "─" * W + "┘")
            lines.append(f"  {'Function':<30} {'Detected':>12} {'Converted':>12} {'Unsupported':>12} {'Rate':>10}")
            lines.append("  " + "─" * 80)
            
            # Sort by detection count
            sorted_funcs = sorted(report.functions_detected.items(), key=lambda x: -x[1])
            
            for func, detected_count in sorted_funcs[:15]:  # Top 15
                converted = report.functions_converted.get(func, 0)
                unsupported = report.functions_unsupported.get(func, 0)
                rate_pct = (converted / max(detected_count, 1)) * 100
                lines.append(f"  {func[:30]:<30} {detected_count:>12} {converted:>12} {unsupported:>12} {rate_pct:>9.1f}%")
            
            if len(sorted_funcs) > 15:
                lines.append(f"  ... and {len(sorted_funcs) - 15} more functions")
            
            # Function conversion summary
            total_func_occurrences = sum(report.functions_detected.values())
            converted_func_occurrences = sum(report.functions_converted.values())
            func_rate = (converted_func_occurrences / max(total_func_occurrences, 1)) * 100
            
            lines.append("")
            lines.append(f"  Function Conversion Rate: {converted_func_occurrences}/{total_func_occurrences} occurrences ({func_rate:.1f}%)")
            lines.append("")
        
        # Unsupported Features
        if report.unsupported_features:
            lines.append("┌" + "─" * W + "┐")
            lines.append("│" + " UNSUPPORTED FEATURES (In Databricks) DETECTED ".center(W) + "│")
            lines.append("└" + "─" * W + "┘")
            
            sorted_features = sorted(report.unsupported_features.items(), key=lambda x: -x[1])
            for feature, count in sorted_features[:10]:
                # Get file locations for this feature (prefer locations over lines)
                feature_locations = report.unsupported_features_locations.get(feature, [])
                if feature_locations:
                    # Group by file and show file:line format
                    unique_locs = []
                    seen = set()
                    for loc in feature_locations:
                        key = (loc.file, loc.line)
                        if key not in seen:
                            seen.add(key)
                            unique_locs.append(loc)
                    unique_locs.sort(key=lambda x: (x.file, x.line))
                    
                    if len(unique_locs) > 5:
                        locs_str = ', '.join(str(loc) for loc in unique_locs[:5]) + '...'
                    else:
                        locs_str = ', '.join(str(loc) for loc in unique_locs)
                    lines.append(f"  ⚠ {feature} ({count}x)")
                    lines.append(f"      └─ Locations: {locs_str}")
                else:
                    # Fallback to line numbers only
                    feature_lines = report.unsupported_features_lines.get(feature, [])
                    if feature_lines:
                        sorted_lines = sorted(set(feature_lines))
                        if len(sorted_lines) > 5:
                            lines_str = ', '.join(str(ln) for ln in sorted_lines[:5]) + '...'
                        else:
                            lines_str = ', '.join(str(ln) for ln in sorted_lines)
                        lines.append(f"  ⚠ {feature} ({count}x) [lines: {lines_str}]")
                    else:
                        lines.append(f"  ⚠ {feature} ({count}x)")
            
            if len(sorted_features) > 10:
                lines.append(f"  ... and {len(sorted_features) - 10} more unsupported features")
            
            lines.append("")
        
        # Functions/Packages with NO Databricks Equivalent
        if report.functions_no_equivalent or report.packages_no_equivalent:
            lines.append("┌" + "─" * W + "┐")
            lines.append("│" + " NO DATABRICKS EQUIVALENT (Requires Manual Work) ".center(W) + "│")
            lines.append("└" + "─" * W + "┘")
            
            # Functions with no equivalent
            if report.functions_no_equivalent:
                lines.append("  ORACLE FUNCTIONS WITH NO EQUIVALENT:")
                sorted_funcs = sorted(report.functions_no_equivalent.items(), key=lambda x: -x[1])
                for func, count in sorted_funcs[:8]:
                    suggestion = FunctionDetector.get_equivalent_suggestion(func)
                    # Get file locations (prefer locations over lines)
                    func_locations = report.functions_no_equivalent_locations.get(func, [])
                    if func_locations:
                        # Group by file and show file:line format
                        unique_locs = []
                        seen = set()
                        for loc in func_locations:
                            key = (loc.file, loc.line)
                            if key not in seen:
                                seen.add(key)
                                unique_locs.append(loc)
                        unique_locs.sort(key=lambda x: (x.file, x.line))
                        
                        if len(unique_locs) > 3:
                            locs_str = ', '.join(str(loc) for loc in unique_locs[:3]) + '...'
                        else:
                            locs_str = ', '.join(str(loc) for loc in unique_locs)
                        lines.append(f"    ✗ {func:<25} ({count:>3}x)  →  {suggestion}")
                        lines.append(f"        └─ {locs_str}")
                    else:
                        # Fallback to line numbers only
                        func_lines = report.functions_no_equivalent_lines.get(func, [])
                        if func_lines:
                            sorted_lines = sorted(set(func_lines))
                            if len(sorted_lines) > 5:
                                lines_str = ', '.join(str(ln) for ln in sorted_lines[:5]) + '...'
                            else:
                                lines_str = ', '.join(str(ln) for ln in sorted_lines)
                            lines.append(f"    ✗ {func:<25} ({count:>3}x) [lines: {lines_str}]  →  {suggestion}")
                        else:
                            lines.append(f"    ✗ {func:<30} ({count:>3}x)  →  {suggestion}")
                
                if len(sorted_funcs) > 8:
                    lines.append(f"    ... and {len(sorted_funcs) - 8} more functions")
                lines.append("")
            
            # Packages with no equivalent
            if report.packages_no_equivalent:
                lines.append("  PACKAGE CALLS:")
                sorted_pkgs = sorted(report.packages_no_equivalent.items(), key=lambda x: -x[1])
                for pkg, count in sorted_pkgs[:10]:
                    suggestion = FunctionDetector.get_equivalent_suggestion(pkg)
                    # Get file locations (prefer locations over lines)
                    pkg_locations = report.packages_no_equivalent_locations.get(pkg, [])
                    if pkg_locations:
                        # Group by file and show file:line format
                        unique_locs = []
                        seen = set()
                        for loc in pkg_locations:
                            key = (loc.file, loc.line)
                            if key not in seen:
                                seen.add(key)
                                unique_locs.append(loc)
                        unique_locs.sort(key=lambda x: (x.file, x.line))
                        
                        if len(unique_locs) > 3:
                            locs_str = ', '.join(str(loc) for loc in unique_locs[:3]) + '...'
                        else:
                            locs_str = ', '.join(str(loc) for loc in unique_locs)
                        lines.append(f"    ✗ {pkg:<35} ({count:>3}x)  →  {suggestion}")
                        lines.append(f"        └─ {locs_str}")
                    else:
                        # Fallback to line numbers only
                        pkg_lines = report.packages_no_equivalent_lines.get(pkg, [])
                        if pkg_lines:
                            sorted_lines = sorted(set(pkg_lines))
                            if len(sorted_lines) > 5:
                                lines_str = ', '.join(str(ln) for ln in sorted_lines[:5]) + '...'
                            else:
                                lines_str = ', '.join(str(ln) for ln in sorted_lines)
                            lines.append(f"    ✗ {pkg:<35} ({count:>3}x) [lines: {lines_str}]  →  {suggestion}")
                        else:
                            lines.append(f"    ✗ {pkg:<40} ({count:>3}x)  →  {suggestion}")
                
                if len(sorted_pkgs) > 10:
                    lines.append(f"    ... and {len(sorted_pkgs) - 10} more package calls")
            
            total_no_equiv = sum(report.functions_no_equivalent.values()) + sum(report.packages_no_equivalent.values())
            lines.append("")
            lines.append(f"  Total occurrences requiring manual work: {total_no_equiv}")
            lines.append("")
        
        # Unknown/Custom Functions (not standard Oracle or Databricks)
        if report.unknown_functions:
            lines.append("┌" + "─" * W + "┐")
            lines.append("│" + " UNKNOWN/CUSTOM FUNCTIONS DETECTED ".center(W) + "│")
            lines.append("│" + " (Not standard Oracle or Databricks - likely internal functions) ".center(W) + "│")
            lines.append("└" + "─" * W + "┘")
            
            sorted_unknown = sorted(report.unknown_functions.items(), key=lambda x: -x[1])
            # Display with file locations (single column for clarity)
            for func, count in sorted_unknown[:15]:
                # Get file locations (prefer locations over lines)
                func_locations = report.unknown_functions_locations.get(func, [])
                if func_locations:
                    # Group by file and show file:line format
                    unique_locs = []
                    seen = set()
                    for loc in func_locations:
                        key = (loc.file, loc.line)
                        if key not in seen:
                            seen.add(key)
                            unique_locs.append(loc)
                    unique_locs.sort(key=lambda x: (x.file, x.line))
                    
                    if len(unique_locs) > 5:
                        locs_str = ', '.join(str(loc) for loc in unique_locs[:5]) + '...'
                    else:
                        locs_str = ', '.join(str(loc) for loc in unique_locs)
                    lines.append(f"  ? {func:<35} ({count:>4}x)")
                    lines.append(f"      └─ {locs_str}")
                else:
                    # Fallback to line numbers only
                    func_lines = report.unknown_functions_lines.get(func, [])
                    if func_lines:
                        sorted_lines = sorted(set(func_lines))
                        if len(sorted_lines) > 5:
                            lines_str = ', '.join(str(ln) for ln in sorted_lines[:5]) + '...'
                        else:
                            lines_str = ', '.join(str(ln) for ln in sorted_lines)
                        lines.append(f"  ? {func:<35} ({count:>4}x) [lines: {lines_str}]")
                    else:
                        lines.append(f"  ? {func:<35} ({count:>4}x)")
            
            if len(sorted_unknown) > 15:
                lines.append(f"  ... and {len(sorted_unknown) - 15} more unknown functions")
            
            total_unknown = sum(report.unknown_functions.values())
            lines.append("")
            lines.append(f"  ⚠ These {total_unknown} function calls are NOT recognized. They are likely:")
            lines.append("    • Custom PL/SQL functions created in your Oracle database")
            lines.append("    • Package procedures from custom packages, or functions from third-party Oracle extensions")
            lines.append("    → Must be recreated as Databricks UDFs or SQL functions")
            lines.append("")
        
        # Failed Conversions Detail
        if report.failed_items:
            lines.append("┌" + "─" * W + "┐")
            lines.append("│" + " ✗ FAILED CONVERSIONS (Details) ".center(W) + "│")
            lines.append("└" + "─" * W + "┘")
            
            for i, item in enumerate(report.failed_items[:5], 1):  # First 5
                sql_preview = item['original_sql'][:100].replace('\n', ' ').strip()
                line_num = item.get('line_number')
                line_info = f" [line {line_num}]" if line_num else ""
                lines.append(f"  {i}.{line_info} {sql_preview}{'...' if len(item['original_sql']) > 100 else ''}")
                if item.get('errors'):
                    for error in item['errors'][:2]:
                        error_text = error[:150].replace('\n', ' ').strip()
                        lines.append(f"     ✗ Error: {error_text}")
                if item.get('unsupported_constructs'):
                    constructs = ', '.join(item['unsupported_constructs'][:5])
                    lines.append(f"     → Unsupported: {constructs}")
                lines.append("")
            
            if len(report.failed_items) > 5:
                lines.append(f"  ... and {len(report.failed_items) - 5} more failed conversions")
            lines.append("")
        
        # Partial Conversions (warnings)
        if report.partial_items:
            lines.append("┌" + "─" * W + "┐")
            lines.append("│" + " ⚠ PARTIAL CONVERSIONS (Require Review) ".center(W) + "│")
            lines.append("└" + "─" * W + "┘")
            
            for i, item in enumerate(report.partial_items[:5], 1):  # First 5
                sql_preview = item['original_sql'][:100].replace('\n', ' ').strip()
                line_num = item.get('line_number')
                line_info = f" [line {line_num}]" if line_num else ""
                lines.append(f"  {i}.{line_info} {sql_preview}{'...' if len(item['original_sql']) > 100 else ''}")
                if item.get('warnings'):
                    for warning in item['warnings'][:2]:
                        warning_text = warning[:150].replace('\n', ' ').strip()
                        lines.append(f"     ⚠ {warning_text}")
                lines.append("")
            
            if len(report.partial_items) > 5:
                lines.append(f"  ... and {len(report.partial_items) - 5} more partial conversions")
            lines.append("")
        
        # Successfully Converted Summary
        if report.converted_items:
            lines.append("┌" + "─" * W + "┐")
            lines.append("│" + " ✓ SUCCESSFULLY CONVERTED ".center(W) + "│")
            lines.append("└" + "─" * W + "┘")
            lines.append(f"  ✓ {report.successful_statements} statements converted without issues")
            
            # Show sample of converted functions
            if report.functions_converted:
                top_converted = sorted(report.functions_converted.items(), key=lambda x: -x[1])[:10]
                func_list = ', '.join(f"{f}({c})" for f, c in top_converted)
                lines.append(f"  Top converted functions: {func_list}")
            
            lines.append("")
        
        # Recommendations
        lines.append("┌" + "─" * W + "┐")
        lines.append("│" + " RECOMMENDATIONS ".center(W) + "│")
        lines.append("└" + "─" * W + "┘")
        
        rec_num = 1
        
        if report.failed_statements > 0:
            lines.append(f"  {rec_num}. Review failed conversions - may need manual intervention")
            rec_num += 1
        
        if report.partial_statements > 0:
            lines.append(f"  {rec_num}. Check partial conversions for semantic correctness")
            rec_num += 1
        
        if report.functions_no_equivalent or report.packages_no_equivalent:
            total_no_equiv = report.total_no_equivalent
            lines.append(f"  {rec_num}. {total_no_equiv} Oracle items have no Databricks equivalent → Implement custom UDFs or use alternative approaches")
            rec_num += 1
        
        if report.unknown_functions:
            total_unknown = report.total_unknown
            lines.append(f"  {rec_num}. {total_unknown} unknown/custom function calls detected → Identify source and recreate as Databricks SQL/UDFs")
            rec_num += 1
        
        if report.packages_no_equivalent:
            if any('DBMS_' in p for p in report.packages_no_equivalent):
                lines.append(f"  {rec_num}. Replace DBMS_* packages with Databricks alternatives (Workflows, dbutils, Python)")
                rec_num += 1
            if any('UTL_' in p for p in report.packages_no_equivalent):
                lines.append(f"  {rec_num}. Replace UTL_* packages with Python libraries (requests, os, io)")
                rec_num += 1
        
        if 'CONNECT BY' in report.unsupported_features or 'START WITH' in report.unsupported_features:
            lines.append(f"  {rec_num}. Convert hierarchical queries to recursive CTEs manually")
            rec_num += 1
        
        if any('Sequences' in f for f in report.unsupported_features):
            lines.append(f"  {rec_num}. Replace Oracle sequences with IDENTITY columns or BIGINT GENERATED ALWAYS AS IDENTITY")
            rec_num += 1
        
        # Overall assessment
        lines.append("")
        total_issues = report.total_no_equivalent + report.total_unknown
        if report.conversion_rate >= 95 and total_issues == 0:
            lines.append("  ✓ ASSESSMENT: Excellent conversion! Ready for testing.")
        elif report.conversion_rate >= 95 and total_issues < 10:
            lines.append("  ✓ ASSESSMENT: High conversion rate. Address custom functions before deployment.")
        elif report.conversion_rate >= 80:
            lines.append("  ⚠ ASSESSMENT: Good conversion rate. Review and address custom/unknown functions.")
        elif report.total_unknown > 0:
            lines.append("  ⚠ ASSESSMENT: Many custom functions detected - significant rework needed for full migration.")
        else:
            lines.append("  ⚠ ASSESSMENT: Significant manual work required for full migration.")
        
        lines.append("")
        
        output = '\n'.join(lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(output)
            print(f"Report written to: {output_file}")
        else:
            print(output)


# Convenience functions for backward compatibility
def build_conversion_report(results: List, original_sqls: List[str] = None,
                           source_file: str = None) -> ConversionReport:
    """Build a comprehensive conversion report from translation results."""
    generator = ReportGenerator()
    return generator.build_report(results, original_sqls, source_file=source_file)


def build_unified_conversion_report(sql_results: List, plsql_results: List,
                                    source_file: str = None) -> ConversionReport:
    """Build a unified conversion report from both SQL and PL/SQL results."""
    generator = ReportGenerator()
    return generator.build_unified_report(sql_results, plsql_results, source_file=source_file)


def print_conversion_report(report: ConversionReport, output_format: str = 'text', output_file: str = None):
    """Print the conversion report in the specified format."""
    generator = ReportGenerator()
    generator.print_report(report, output_format, output_file)


def analyze_translation_result(result, original_sql: str) -> Dict[str, Any]:
    """Analyze a single translation result for reporting."""
    return ReportGenerator.analyze_translation_result(result, original_sql)

