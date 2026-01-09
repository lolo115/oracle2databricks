#!/usr/bin/env python3
"""
Oracle to Databricks SQL Translator - Command Line Interface

Usage:
    python ora2databricks.py convert <input_file> [--output <output_file>] [--config <rules_file>] [--report]
    python ora2databricks.py batch <input_dir> <output_dir> [--config <rules_file>] [--recursive] [--report]
    python ora2databricks.py interactive
    python ora2databricks.py inline "SQL statement" [--config <rules_file>]
    python ora2databricks.py init-config [--output <config_file>]
    python ora2databricks.py validate-config <config_file>
"""

import argparse
import sys
import os
import re
import time
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
from enum import Enum

from oracle2databricks import (
    OracleToDatabricksTranslator,
    PLSQLConverter,
    strip_sql_comments,
    ConversionReport,
    ReportGenerator,
    build_conversion_report,
    build_unified_conversion_report,
    print_conversion_report,
    analyze_translation_result,
    load_custom_rules,
    save_sample_config,
    validate_config,
)
from oracle2databricks.function_detector import FunctionDetector


@dataclass
class BatchResult:
    """Result summary for batch processing."""
    input_file: str
    output_file: str
    success: bool
    statements_total: int = 0
    statements_success: int = 0
    sql_total: int = 0
    sql_success: int = 0
    plsql_total: int = 0
    plsql_success: int = 0
    script_type: str = "SQL"  # SQL, PL/SQL, or MIXED
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    # Store results for detailed report generation
    sql_results: List = field(default_factory=list)
    plsql_results: List = field(default_factory=list)


class ScriptType(Enum):
    """Types of Oracle scripts."""
    SQL = "SQL"
    PLSQL = "PL/SQL"
    MIXED = "MIXED"


def detect_script_type(content: str) -> ScriptType:
    """
    Detect if the script content is SQL, PL/SQL, or mixed.
    
    Args:
        content: The script content to analyze
        
    Returns:
        ScriptType indicating the type of script
    """
    # Strip comments before analyzing to avoid false positives from commented code
    content_without_comments = strip_sql_comments(content)
    content_upper = content_without_comments.upper()
    
    # PL/SQL indicators (strong indicators of procedural code)
    plsql_patterns = [
        r'\bCREATE\s+(OR\s+REPLACE\s+)?(PROCEDURE|FUNCTION|PACKAGE|TRIGGER)\b',
        r'\bCREATE\s+(OR\s+REPLACE\s+)?PACKAGE\s+BODY\b',
        r'\bDECLARE\b.*\bBEGIN\b',
        r'\bBEGIN\b.*\bEND\s*;',
        r'\bFOR\s+\w+\s+IN\b.*\bLOOP\b',
        r'\bWHILE\b.*\bLOOP\b',
        r'\bIF\b.*\bTHEN\b.*\bEND\s+IF\b',
        r'\bEXCEPTION\b.*\bWHEN\b',
        r'\bRAISE\b.*\b(EXCEPTION|APPLICATION_ERROR)\b',
        r'\bPRAGMA\b',
        r'\bCURSOR\b.*\bIS\b.*\bSELECT\b',
        r'\bOPEN\b.*\bFOR\b',
        r'\bFETCH\b.*\bINTO\b',
        r'\bBULK\s+COLLECT\b',
        r'\bFORALL\b',
        r'%TYPE\b',
        r'%ROWTYPE\b',
        r'\bDBMS_\w+\.\w+',
        r'\bUTL_\w+\.\w+',
    ]
    
    # SQL-only indicators (DML/DDL without procedural elements)
    sql_patterns = [
        r'^\s*SELECT\b',
        r'^\s*INSERT\b',
        r'^\s*UPDATE\b',
        r'^\s*DELETE\b',
        r'^\s*MERGE\b',
        r'^\s*CREATE\s+TABLE\b',
        r'^\s*CREATE\s+VIEW\b',
        r'^\s*CREATE\s+INDEX\b',
        r'^\s*ALTER\b',
        r'^\s*DROP\b',
        r'^\s*GRANT\b',
        r'^\s*REVOKE\b',
    ]
    
    # Count matches
    plsql_matches = 0
    for pattern in plsql_patterns:
        if re.search(pattern, content_upper, re.MULTILINE | re.DOTALL):
            plsql_matches += 1
    
    sql_only_matches = 0
    for pattern in sql_patterns:
        if re.search(pattern, content_upper, re.MULTILINE):
            sql_only_matches += 1
    
    # Decision logic
    has_plsql = plsql_matches >= 2 or re.search(
        r'\bCREATE\s+(OR\s+REPLACE\s+)?(PROCEDURE|FUNCTION|PACKAGE|TRIGGER)\b',
        content_upper
    )
    
    has_sql = sql_only_matches > 0 and not has_plsql
    
    # Check for mixed content
    if has_plsql and sql_only_matches > 0:
        # Check if there are standalone SQL statements outside PL/SQL blocks
        # Remove PL/SQL blocks and check for remaining SQL
        temp_content = re.sub(
            r'CREATE\s+(OR\s+REPLACE\s+)?(PROCEDURE|FUNCTION|PACKAGE|TRIGGER).*?END\s*;',
            '', content_upper, flags=re.DOTALL
        )
        for pattern in sql_patterns:
            if re.search(pattern, temp_content, re.MULTILINE):
                return ScriptType.MIXED
        return ScriptType.PLSQL
    
    if has_plsql:
        return ScriptType.PLSQL
    
    return ScriptType.SQL


def print_banner():
    """Print the application banner."""
    banner = """
╔═══════════════════════════════════════════════════════════════════╗
║           Oracle to Databricks SQL Translator                     ║
║          Laurent Leturgez - Powered by sqlglot                    ║
╚═══════════════════════════════════════════════════════════════════╝
"""
    print(banner)


def print_detailed_issues(result, statement_num: int = None, verbose: bool = True):
    """Print detailed issues for a translation result."""
    prefix = f"Statement {statement_num}: " if statement_num else ""
    
    if not result.success:
        print(f"\n{'='*60}")
        print(f"❌ {prefix}TRANSLATION FAILED")
        print('='*60)
        
        # Print errors
        if result.errors:
            print("\n[ERRORS]")
            for error in result.errors:
                print(f"  ✗ {error}")
        
        # Print detailed issues if available
        if hasattr(result, 'issues') and result.issues:
            print("\n[DETAILED ANALYSIS]")
            for issue in result.issues:
                print(f"  [{issue.issue_type.value}] {issue.message}")
                if issue.oracle_construct:
                    print(f"      Oracle construct: {issue.oracle_construct}")
                if issue.details:
                    print(f"      Details: {issue.details}")
                if issue.suggestion:
                    print(f"      💡 Suggestion: {issue.suggestion}")
        
        # Print original SQL with line numbers
        print("\n[ORIGINAL SQL]")
        for i, line in enumerate(result.original_sql.split('\n'), 1):
            print(f"  {i:3d} | {line}")
    
    elif verbose:
        # For successful translations, show unsupported features and warnings
        if hasattr(result, 'unsupported_features') and result.unsupported_features:
            print(f"\n⚠️  {prefix}Unsupported Features Detected:")
            for feature in result.unsupported_features:
                print(f"    • {feature}")
        
        if hasattr(result, 'issues') and result.issues:
            important_issues = [i for i in result.issues 
                             if i.issue_type.value in ('WARNING', 'UNSUPPORTED', 'PARTIAL')]
            if important_issues:
                print(f"\n📋 {prefix}Conversion Notes:")
                for issue in important_issues:
                    print(f"    [{issue.issue_type.value}] {issue.message}")
                    if issue.suggestion:
                        print(f"        💡 {issue.suggestion}")


def convert_file_unified(args):
    """
    Unified conversion function that auto-detects SQL vs PL/SQL content.
    
    This replaces the separate translate and convert-plsql commands.
    """
    config_file = getattr(args, 'config', None)
    translator = OracleToDatabricksTranslator(
        pretty=getattr(args, 'format', True),
        config_file=config_file
    )
    converter = PLSQLConverter()
    
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file '{input_path}' not found.")
        sys.exit(1)
    
    output_path = args.output if args.output else None
    verbose = getattr(args, 'verbose', False)
    show_report = getattr(args, 'report', False)
    report_format = getattr(args, 'report_format', 'text')
    report_output = getattr(args, 'report_output', None)
    
    # Read content and detect script type
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    script_type = detect_script_type(content)
    print(f"Converting: {input_path}")
    print(f"Detected script type: {script_type.value}")
    
    sql_results = []
    plsql_results = []
    
    if script_type == ScriptType.SQL:
        # Pure SQL conversion
        sql_results = translator.translate_file(str(input_path), output_path)
            
    elif script_type == ScriptType.PLSQL:
        # Pure PL/SQL conversion
        plsql_results = converter.convert_file(str(input_path), output_path)
            
    else:  # MIXED
        # Handle mixed content - extract and process both
        print("  Processing mixed SQL and PL/SQL content...")
        
        # Extract PL/SQL objects
        plsql_objects = converter._split_plsql_objects(content)
        if plsql_objects:
            print(f"  Found {len(plsql_objects)} PL/SQL objects")
            for obj in plsql_objects:
                result = converter.convert(obj)
                plsql_results.append(result)
        
        # Remove PL/SQL from content and process remaining SQL
        remaining_sql = content
        for obj in plsql_objects:
            remaining_sql = remaining_sql.replace(obj, '')
        
        # Process remaining SQL statements
        if remaining_sql.strip():
            statements = translator._split_statements(remaining_sql)
            statements = [s.strip() for s in statements if s.strip() and translator._has_sql_content(s)]
            if statements:
                print(f"  Found {len(statements)} SQL statements")
                for stmt in statements:
                    result = translator.translate(stmt)
                    sql_results.append(result)
    
    # Calculate totals
    sql_total = len(sql_results)
    sql_success = sum(1 for r in sql_results if r.success)
    sql_warnings = sum(1 for r in sql_results if r.success and r.warnings)
    
    plsql_total = len(plsql_results)
    plsql_success = sum(1 for r in plsql_results if r.success)
    plsql_warnings = sum(1 for r in plsql_results if r.success and (r.warnings or r.manual_review_required))
    
    total = sql_total + plsql_total
    success = sql_success + plsql_success
    
    # Print summary
    print(f"\nConversion complete:")
    if sql_total > 0:
        print(f"  SQL statements: {sql_success}/{sql_total} successful")
    if plsql_total > 0:
        print(f"  PL/SQL objects: {plsql_success}/{plsql_total} successful")
    print(f"  Total: {success}/{total} successful")
    
    # Write output
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            # Write SQL results
            if sql_results:
                f.write("-- SQL Statements\n")
                f.write("-- " + "=" * 50 + "\n\n")
                for i, result in enumerate(sql_results, 1):
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
            
            # Write PL/SQL results
            if plsql_results:
                f.write("\n-- PL/SQL Objects\n")
                f.write("-- " + "=" * 50 + "\n\n")
                for result in plsql_results:
                    f.write(f"-- {result.object_type.value}: {result.object_name}\n")
                    if result.warnings:
                        for warning in result.warnings:
                            f.write(f"-- WARNING: {warning}\n")
                    if result.manual_review_required:
                        for item in result.manual_review_required:
                            f.write(f"-- REVIEW: {item}\n")
                    f.write(result.converted_code)
                    f.write("\n\n")
        
        print(f"Output written to: {output_path}")
    else:
        # Print to stdout
        print("\n" + "=" * 60)
        print("CONVERTED CODE:")
        print("=" * 60)
        
        if sql_results:
            print("\n-- SQL Statements --")
            for i, result in enumerate(sql_results, 1):
                print(f"\n-- Statement {i}:")
                if result.warnings:
                    for warning in result.warnings:
                        print(f"-- WARNING: {warning}")
                if result.success:
                    print(result.translated_sql + ";")
                else:
                    print(f"-- TRANSLATION FAILED: {'; '.join(result.errors)}")
        
        if plsql_results:
            print("\n-- PL/SQL Objects --")
            for result in plsql_results:
                print(f"\n-- {result.object_type.value}: {result.object_name}")
                if result.warnings:
                    for warning in result.warnings:
                        print(f"-- WARNING: {warning}")
                print(result.converted_code)
    
    # Generate report if requested
    if show_report:
        report = build_unified_conversion_report(sql_results, plsql_results, source_file=str(input_path))
        print_conversion_report(report, output_format=report_format, output_file=report_output)
    
    return 0 if success == total else 1


def interactive_mode(args):
    """Run in interactive mode for quick translations."""
    print_banner()
    print("Interactive Mode - Enter Oracle SQL (end with ';' on a new line, 'quit' to exit)")
    print("-" * 60)
    
    translator = OracleToDatabricksTranslator(pretty=True)
    converter = PLSQLConverter()
    
    while True:
        print("\nEnter Oracle SQL/PL*SQL (or 'quit' to exit):")
        lines = []
        
        while True:
            try:
                line = input()
            except EOFError:
                print("\nGoodbye!")
                return 0
            
            if line.strip().lower() == 'quit':
                print("Goodbye!")
                return 0
            
            lines.append(line)
            
            # Check if statement is complete
            full_text = '\n'.join(lines)
            if (full_text.strip().endswith(';') or 
                full_text.strip().endswith('/') or
                (full_text.strip().upper().startswith('CREATE') and 
                 'END' in full_text.upper() and 
                 full_text.strip().endswith(';'))):
                break
        
        sql = '\n'.join(lines)
        
        # Detect if it's PL/SQL
        is_plsql = any(kw in sql.upper() for kw in [
            'CREATE PROCEDURE', 'CREATE FUNCTION', 'CREATE PACKAGE',
            'CREATE OR REPLACE PROCEDURE', 'CREATE OR REPLACE FUNCTION',
            'DECLARE', 'BEGIN'
        ])
        
        print("\n" + "-" * 40)
        print("DATABRICKS SQL:")
        print("-" * 40)
        
        if is_plsql:
            result = converter.convert(sql)
            if result.warnings:
                for warning in result.warnings:
                    print(f"-- WARNING: {warning}")
            print(result.converted_code)
            if result.manual_review_required:
                print("\n-- Manual review required:")
                for item in result.manual_review_required:
                    print(f"--   ⚠ {item}")
        else:
            result = translator.translate(sql.rstrip(';'))
            if result.warnings:
                for warning in result.warnings:
                    print(f"-- WARNING: {warning}")
            if result.success:
                print(result.translated_sql + ";")
            else:
                print(f"-- Translation failed: {'; '.join(result.errors)}")


def init_config(args):
    """Generate a sample custom rules configuration file."""
    output_path = args.output
    
    try:
        # Ensure the directory exists
        output_dir = Path(output_path).parent
        if output_dir and not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
        
        save_sample_config(output_path)
        print(f"\n✓ Configuration file created: {output_path}")
        print("\nThis file contains example custom transformation rules.")
        print("Edit the file to add your own in-house function conversions.")
        print("\nNote: User configuration files in extra_config/ are git-ignored.")
        print("      The sample template (custom_rules.sample.json) is tracked in git.")
        print("\nUsage:")
        print(f"  python cli.py convert input.sql -o output.sql --config {output_path}")
        return 0
    except Exception as e:
        print(f"Error creating configuration file: {e}")
        return 1


def validate_config_cmd(args):
    """Validate a custom rules configuration file."""
    config_path = args.config_file
    
    print(f"Validating configuration file: {config_path}")
    
    is_valid, errors = validate_config(config_path)
    
    if is_valid:
        # Load and display the rules
        try:
            config = load_custom_rules(config_path)
            enabled_rules = config.get_enabled_rules()
            disabled_rules = [r for r in config.rules if not r.enabled]
            
            print(f"\n✓ Configuration is valid!")
            print(f"\nRules summary:")
            print(f"  Total rules:    {len(config.rules)}")
            print(f"  Enabled rules:  {len(enabled_rules)}")
            print(f"  Disabled rules: {len(disabled_rules)}")
            
            if enabled_rules:
                print(f"\nEnabled rules (by priority):")
                for rule in enabled_rules:
                    print(f"  [{rule.priority:3d}] {rule.name}")
                    if rule.description:
                        print(f"        {rule.description}")
            
            print(f"\nSettings:")
            print(f"  Apply before default: {config.apply_before_default}")
            print(f"  Continue on error:    {config.continue_on_error}")
            
            return 0
        except Exception as e:
            print(f"\n✗ Error loading configuration: {e}")
            return 1
    else:
        print(f"\n✗ Configuration is invalid!")
        print("\nErrors:")
        for error in errors:
            print(f"  • {error}")
        return 1


def translate_inline(args):
    """Translate inline SQL from command line argument."""
    config_file = getattr(args, 'config', None)
    translator = OracleToDatabricksTranslator(pretty=args.format, config_file=config_file)
    verbose = getattr(args, 'verbose', False)
    
    result = translator.translate(args.sql)
    
    if result.success:
        # Print warnings first
        if result.warnings:
            for warning in result.warnings:
                print(f"-- WARNING: {warning}", file=sys.stderr)
        
        # Print translated SQL
        print(result.translated_sql + ";")
        
        # Print unsupported features if any
        if hasattr(result, 'unsupported_features') and result.unsupported_features:
            print("\n-- ⚠️  Unsupported features detected:", file=sys.stderr)
            for feature in result.unsupported_features:
                print(f"--   • {feature}", file=sys.stderr)
        
        # Print detailed issues in verbose mode
        if verbose and hasattr(result, 'issues') and result.issues:
            print("\n-- 📋 Conversion notes:", file=sys.stderr)
            for issue in result.issues:
                print(f"--   [{issue.issue_type.value}] {issue.message}", file=sys.stderr)
                if issue.suggestion:
                    print(f"--       💡 {issue.suggestion}", file=sys.stderr)
        
        return 0
    else:
        # Translation failed - print detailed error information
        print("-- ❌ TRANSLATION FAILED", file=sys.stderr)
        print("-- " + "=" * 50, file=sys.stderr)
        
        # Print errors
        if result.errors:
            print("-- [ERRORS]", file=sys.stderr)
            for error in result.errors:
                print(f"--   ✗ {error}", file=sys.stderr)
        
        # Print detailed issues
        if hasattr(result, 'issues') and result.issues:
            print("--", file=sys.stderr)
            print("-- [DETAILED ANALYSIS]", file=sys.stderr)
            for issue in result.issues:
                print(f"--   [{issue.issue_type.value}] {issue.message}", file=sys.stderr)
                if issue.oracle_construct:
                    print(f"--       Oracle construct: {issue.oracle_construct}", file=sys.stderr)
                if issue.details:
                    print(f"--       Details: {issue.details}", file=sys.stderr)
                if issue.suggestion:
                    print(f"--       💡 Suggestion: {issue.suggestion}", file=sys.stderr)
        
        # Print unsupported features
        if hasattr(result, 'unsupported_features') and result.unsupported_features:
            print("--", file=sys.stderr)
            print("-- [UNSUPPORTED FEATURES]", file=sys.stderr)
            for feature in result.unsupported_features:
                print(f"--   ⚠ {feature}", file=sys.stderr)
        
        # Print original SQL
        print("--", file=sys.stderr)
        print("-- [ORIGINAL SQL]", file=sys.stderr)
        for i, line in enumerate(result.original_sql.split('\n'), 1):
            print(f"--   {i:3d} | {line}", file=sys.stderr)
        
        return 1


def process_single_file(
    input_path: Path,
    output_path: Path,
    translator: OracleToDatabricksTranslator,
    converter: PLSQLConverter
) -> BatchResult:
    """Process a single file for batch operation using unified detection."""
    result = BatchResult(
        input_file=str(input_path),
        output_file=str(output_path),
        success=True
    )
    
    try:
        # Read content and detect script type
        with open(input_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        script_type = detect_script_type(content)
        result.script_type = script_type.value
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        sql_results = []
        plsql_results = []
        
        if script_type == ScriptType.SQL:
            # Pure SQL conversion
            sql_results = translator.translate_file(str(input_path), str(output_path))
            
        elif script_type == ScriptType.PLSQL:
            # Pure PL/SQL conversion
            plsql_results = converter.convert_file(str(input_path), str(output_path))
            
        else:  # MIXED
            # Handle mixed content
            # Extract and convert PL/SQL objects
            plsql_objects = converter._split_plsql_objects(content)
            for obj in plsql_objects:
                conv_result = converter.convert(obj)
                plsql_results.append(conv_result)
            
            # Remove PL/SQL from content and process remaining SQL
            remaining_sql = content
            for obj in plsql_objects:
                remaining_sql = remaining_sql.replace(obj, '')
            
            if remaining_sql.strip():
                statements = translator._split_statements(remaining_sql)
                statements = [s.strip() for s in statements if s.strip() and translator._has_sql_content(s)]
                for stmt in statements:
                    trans_result = translator.translate(stmt)
                    sql_results.append(trans_result)
            
            # Write combined output
            with open(output_path, 'w', encoding='utf-8') as f:
                if sql_results:
                    f.write("-- SQL Statements\n\n")
                    for r in sql_results:
                        if r.success:
                            f.write(r.translated_sql + ";\n\n")
                if plsql_results:
                    f.write("\n-- PL/SQL Objects\n\n")
                    for r in plsql_results:
                        f.write(f"-- {r.object_type.value}: {r.object_name}\n")
                        f.write(r.converted_code + "\n\n")
        
        # Calculate results
        result.sql_total = len(sql_results)
        result.sql_success = sum(1 for r in sql_results if r.success)
        result.plsql_total = len(plsql_results)
        result.plsql_success = sum(1 for r in plsql_results if r.success)
        
        result.statements_total = result.sql_total + result.plsql_total
        result.statements_success = result.sql_success + result.plsql_success
        
        # Store results for detailed report generation
        result.sql_results = sql_results
        result.plsql_results = plsql_results
        
        # Collect warnings and errors
        for r in sql_results:
            result.warnings.extend(r.warnings)
            result.errors.extend(r.errors)
        
        for r in plsql_results:
            result.warnings.extend(r.warnings)
            result.errors.extend(r.errors)
            if hasattr(r, 'manual_review_required'):
                result.warnings.extend(r.manual_review_required)
        
        result.success = result.statements_success == result.statements_total
        
    except Exception as e:
        result.success = False
        result.errors.append(str(e))
    
    return result


def batch_translate(args):
    """Batch translate all SQL files in a directory."""
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    
    if not input_dir.exists():
        print(f"Error: Input directory '{input_dir}' not found.")
        sys.exit(1)
    
    if not input_dir.is_dir():
        print(f"Error: '{input_dir}' is not a directory.")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all SQL files
    sql_extensions = {'.sql', '.pls', '.pks', '.pkb', '.plb', '.prc', '.fnc', '.trg'}
    
    if args.recursive:
        sql_files = [
            f for f in input_dir.rglob('*')
            if f.is_file() and f.suffix.lower() in sql_extensions
        ]
    else:
        sql_files = [
            f for f in input_dir.iterdir()
            if f.is_file() and f.suffix.lower() in sql_extensions
        ]
    
    if not sql_files:
        print(f"No SQL files found in '{input_dir}'")
        return 0
    
    print_banner()
    print(f"Batch Translation")
    print(f"  Input directory:  {input_dir}")
    print(f"  Output directory: {output_dir}")
    print(f"  Files to process: {len(sql_files)}")
    print(f"  Recursive:        {args.recursive}")
    print("-" * 60)
    
    # Initialize translator and converter
    config_file = getattr(args, 'config', None)
    translator = OracleToDatabricksTranslator(pretty=True, config_file=config_file)
    converter = PLSQLConverter()
    
    # Process files
    results: List[BatchResult] = []
    
    print("Processing files...")
    for input_file in sql_files:
        # Calculate relative path for output
        rel_path = input_file.relative_to(input_dir)
        output_file = output_dir / rel_path.with_suffix('.sql')
        
        result = process_single_file(input_file, output_file, translator, converter)
        results.append(result)
        
        status = "✓" if result.success else "✗"
        print(f"  {status} {input_file.name} ({result.statements_success}/{result.statements_total} statements)")
    
    # Print summary
    print("\n" + "=" * 60)
    print("BATCH SUMMARY")
    print("=" * 60)
    
    total_files = len(results)
    successful_files = sum(1 for r in results if r.success)
    total_statements = sum(r.statements_total for r in results)
    successful_statements = sum(r.statements_success for r in results)
    
    # SQL and PL/SQL breakdown
    total_sql = sum(r.sql_total for r in results)
    success_sql = sum(r.sql_success for r in results)
    total_plsql = sum(r.plsql_total for r in results)
    success_plsql = sum(r.plsql_success for r in results)
    
    # Count file types
    sql_files_count = sum(1 for r in results if r.script_type == "SQL")
    plsql_files_count = sum(1 for r in results if r.script_type == "PL/SQL")
    mixed_files_count = sum(1 for r in results if r.script_type == "MIXED")
    
    print(f"  Files processed:      {total_files}")
    print(f"  Files successful:     {successful_files}")
    print(f"  Files with errors:    {total_files - successful_files}")
    print(f"  File types:           SQL: {sql_files_count}, PL/SQL: {plsql_files_count}, Mixed: {mixed_files_count}")
    print(f"  Total items:          {total_statements}")
    print(f"  Items converted:      {successful_statements}")
    if total_sql > 0:
        print(f"    SQL statements:     {success_sql}/{total_sql}")
    if total_plsql > 0:
        print(f"    PL/SQL objects:     {success_plsql}/{total_plsql}")
    
    show_report = getattr(args, 'report', False)
    report_format = getattr(args, 'report_format', 'text')
    report_output = getattr(args, 'report_output', None)
    
    # List failed files
    failed = [r for r in results if not r.success]
    if failed and not show_report:
        print("\n" + "-" * 40)
        print("FAILED FILES:")
        for r in failed:
            print(f"  • {r.input_file}")
            for err in r.errors[:3]:  # Show first 3 errors
                print(f"      Error: {err}")
    
    # Collect unique warnings
    all_warnings = set()
    for r in results:
        all_warnings.update(r.warnings)
    
    if all_warnings and not show_report:
        print("\n" + "-" * 40)
        print("WARNINGS (may require manual review):")
        for warning in sorted(all_warnings)[:10]:  # Show first 10 unique warnings
            print(f"  ⚠ {warning}")
        if len(all_warnings) > 10:
            print(f"  ... and {len(all_warnings) - 10} more warnings")
    
    print("\n" + "=" * 60)
    print(f"Output written to: {output_dir}")
    
    # Generate and print conversion report if requested
    if show_report:
        # Build a batch report with SQL/PL/SQL breakdown using proper analysis
        from oracle2databricks.report_generator import FileLocation
        
        report = ConversionReport()
        
        # Track SQL and PL/SQL separately (calculate partials properly)
        sql_with_warnings = sum(1 for r in results if r.sql_total > 0 and r.warnings)
        plsql_with_warnings = sum(1 for r in results if r.plsql_total > 0 and r.warnings)
        
        report.sql_total = total_sql
        report.sql_successful = success_sql - sql_with_warnings
        report.sql_partial = sql_with_warnings
        report.sql_failed = total_sql - success_sql
        
        report.plsql_total = total_plsql
        report.plsql_successful = success_plsql - plsql_with_warnings
        report.plsql_partial = plsql_with_warnings
        report.plsql_failed = total_plsql - success_plsql
        
        # Calculate totals from the breakdown
        report.total_statements = total_statements
        report.successful_statements = report.sql_successful + report.plsql_successful
        report.partial_statements = report.sql_partial + report.plsql_partial
        report.failed_statements = report.sql_failed + report.plsql_failed
        
        for batch_result in results:
            file_name = batch_result.input_file
            
            # Track file-level info
            item = {
                'original_sql': f"File: {batch_result.input_file} ({batch_result.script_type})",
                'success': batch_result.success,
                'has_warnings': bool(batch_result.warnings),
                'functions_detected': [],
                'unsupported_constructs': [],
                'errors': batch_result.errors,
                'warnings': batch_result.warnings,
            }
            
            if batch_result.success:
                if batch_result.warnings:
                    report.partial_items.append(item)
                else:
                    report.converted_items.append(item)
            else:
                report.failed_items.append(item)
            
            # Add warnings
            report.warnings.extend(batch_result.warnings)
            
            # Analyze SQL results with file and line tracking
            for sql_result in batch_result.sql_results:
                original_sql = getattr(sql_result, 'original_sql', '')
                base_line = getattr(sql_result, 'line_number', 1) or 1
                
                # Track functions
                detected_funcs = FunctionDetector.detect_oracle_functions(original_sql)
                for func in detected_funcs:
                    report.functions_detected[func] = report.functions_detected.get(func, 0) + 1
                    if sql_result.success:
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
                    if func not in report.functions_no_equivalent_locations:
                        report.functions_no_equivalent_locations[func] = []
                    report.functions_no_equivalent_locations[func].extend([FileLocation(file_name, ln) for ln in lines])
                
                for pkg, lines in pkgs_no_equiv_lines.items():
                    report.packages_no_equivalent[pkg] = report.packages_no_equivalent.get(pkg, 0) + len(lines)
                    if pkg not in report.packages_no_equivalent_lines:
                        report.packages_no_equivalent_lines[pkg] = []
                    report.packages_no_equivalent_lines[pkg].extend(lines)
                    if pkg not in report.packages_no_equivalent_locations:
                        report.packages_no_equivalent_locations[pkg] = []
                    report.packages_no_equivalent_locations[pkg].extend([FileLocation(file_name, ln) for ln in lines])
                
                for func, lines in unknown_funcs_lines.items():
                    report.unknown_functions[func] = report.unknown_functions.get(func, 0) + len(lines)
                    if func not in report.unknown_functions_lines:
                        report.unknown_functions_lines[func] = []
                    report.unknown_functions_lines[func].extend(lines)
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
                    if construct not in report.unsupported_features_locations:
                        report.unsupported_features_locations[construct] = []
                    report.unsupported_features_locations[construct].extend([FileLocation(file_name, ln) for ln in lines])
            
            # Analyze PL/SQL results with file and line tracking
            for plsql_result in batch_result.plsql_results:
                original_code = getattr(plsql_result, 'original_code', '')
                base_line = 1
                
                # Track functions
                detected_funcs = FunctionDetector.detect_oracle_functions(original_code)
                for func in detected_funcs:
                    report.functions_detected[func] = report.functions_detected.get(func, 0) + 1
                    if plsql_result.success:
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
                    if func not in report.functions_no_equivalent_locations:
                        report.functions_no_equivalent_locations[func] = []
                    report.functions_no_equivalent_locations[func].extend([FileLocation(file_name, ln) for ln in lines])
                
                for pkg, lines in pkgs_no_equiv_lines.items():
                    report.packages_no_equivalent[pkg] = report.packages_no_equivalent.get(pkg, 0) + len(lines)
                    if pkg not in report.packages_no_equivalent_lines:
                        report.packages_no_equivalent_lines[pkg] = []
                    report.packages_no_equivalent_lines[pkg].extend(lines)
                    if pkg not in report.packages_no_equivalent_locations:
                        report.packages_no_equivalent_locations[pkg] = []
                    report.packages_no_equivalent_locations[pkg].extend([FileLocation(file_name, ln) for ln in lines])
                
                for func, lines in unknown_funcs_lines.items():
                    report.unknown_functions[func] = report.unknown_functions.get(func, 0) + len(lines)
                    if func not in report.unknown_functions_lines:
                        report.unknown_functions_lines[func] = []
                    report.unknown_functions_lines[func].extend(lines)
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
                    if construct not in report.unsupported_features_locations:
                        report.unsupported_features_locations[construct] = []
                    report.unsupported_features_locations[construct].extend([FileLocation(file_name, ln) for ln in lines])
        
        print_conversion_report(report, output_format=report_format, output_file=report_output)
    
    return 0 if successful_files == total_files else 1


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Oracle to Databricks SQL Translator\nLaurent Leturgez",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert any Oracle file (auto-detects SQL vs PL/SQL)
  python ora2databricks.py convert input.sql --output output.sql

  # Convert with custom transformation rules
  python ora2databricks.py convert input.sql -o output.sql --config extra_config/custom_rules.json

  # Convert with verbose output
  python ora2databricks.py convert input.sql -o output.sql --verbose

  # Convert with detailed conversion report
  python ora2databricks.py convert input.sql -o output.sql --report

  # Convert with JSON report saved to file
  python ora2databricks.py convert input.sql -o out.sql --report --report-format json --report-output report.json

  # Batch convert a directory with custom rules
  python ora2databricks.py batch ./oracle_scripts ./databricks_scripts --recursive --config extra_config/custom_rules.json

  # Batch convert with report
  python ora2databricks.py batch ./oracle_scripts ./databricks_scripts -r --report

  # Interactive mode
  python ora2databricks.py interactive

  # Quick inline translation
  python ora2databricks.py inline "SELECT SYSDATE FROM DUAL"

  # Quick inline translation with custom rules
  python ora2databricks.py inline "SELECT MY_CUSTOM_FUNC(a, b) FROM test" --config extra_config/custom_rules.json

  # Generate a custom rules configuration file (in extra_config/ folder)
  python ora2databricks.py init-config
  python ora2databricks.py init-config --output extra_config/my_project_rules.json

  # Validate a custom rules configuration file
  python ora2databricks.py validate-config extra_config/custom_rules.json
"""
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Unified Convert command (auto-detects SQL vs PL/SQL)
    convert_parser = subparsers.add_parser(
        'convert',
        help='Convert Oracle SQL/PL/SQL file to Databricks (auto-detects type)'
    )
    convert_parser.add_argument('input_file', help='Input Oracle SQL or PL/SQL file')
    convert_parser.add_argument(
        '--output', '-o',
        help='Output file path (prints to stdout if not specified)'
    )
    convert_parser.add_argument(
        '--config', '-c',
        help='Path to JSON file containing custom transformation rules'
    )
    convert_parser.add_argument(
        '--format', '-f',
        action='store_true',
        default=True,
        help='Format output SQL with indentation (default: True)'
    )
    convert_parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        default=False,
        help='Show detailed conversion notes and suggestions'
    )
    convert_parser.add_argument(
        '--report', '-R',
        action='store_true',
        default=False,
        help='Generate a detailed conversion report after conversion'
    )
    convert_parser.add_argument(
        '--report-format',
        choices=['text', 'json'],
        default='text',
        help='Format for the conversion report (default: text)'
    )
    convert_parser.add_argument(
        '--report-output',
        help='Write report to file instead of stdout'
    )
    convert_parser.set_defaults(func=convert_file_unified)
    
    # Batch convert directory
    batch_parser = subparsers.add_parser(
        'batch',
        help='Batch convert all Oracle SQL/PL/SQL files in a directory'
    )
    batch_parser.add_argument('input_dir', help='Input directory containing Oracle SQL files')
    batch_parser.add_argument('output_dir', help='Output directory for Databricks SQL files')
    batch_parser.add_argument(
        '--config', '-c',
        help='Path to JSON file containing custom transformation rules'
    )
    batch_parser.add_argument(
        '--recursive', '-r',
        action='store_true',
        default=False,
        help='Recursively process subdirectories'
    )
    batch_parser.add_argument(
        '--report', '-R',
        action='store_true',
        default=False,
        help='Generate a detailed conversion report after batch processing'
    )
    batch_parser.add_argument(
        '--report-format',
        choices=['text', 'json'],
        default='text',
        help='Format for the conversion report (default: text)'
    )
    batch_parser.add_argument(
        '--report-output',
        help='Write report to file instead of stdout'
    )
    batch_parser.set_defaults(func=batch_translate)
    
    # Interactive mode
    interactive_parser = subparsers.add_parser(
        'interactive',
        help='Run in interactive mode for quick translations'
    )
    interactive_parser.set_defaults(func=interactive_mode)
    
    # Inline translation
    inline_parser = subparsers.add_parser(
        'inline',
        help='Translate a single SQL statement from command line'
    )
    inline_parser.add_argument('sql', help='Oracle SQL statement to translate')
    inline_parser.add_argument(
        '--config', '-c',
        help='Path to JSON file containing custom transformation rules'
    )
    inline_parser.add_argument(
        '--format', '-f',
        action='store_true',
        default=True,
        help='Format output SQL'
    )
    inline_parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        default=False,
        help='Show detailed conversion notes and suggestions'
    )
    inline_parser.set_defaults(func=translate_inline)
    
    # Generate sample config file
    init_config_parser = subparsers.add_parser(
        'init-config',
        help='Generate a sample custom_rules.json configuration file'
    )
    init_config_parser.add_argument(
        '--output', '-o',
        default='extra_config/custom_rules.json',
        help='Output path for the configuration file (default: extra_config/custom_rules.json)'
    )
    init_config_parser.set_defaults(func=init_config)
    
    # Validate config file
    validate_config_parser = subparsers.add_parser(
        'validate-config',
        help='Validate a custom rules configuration file'
    )
    validate_config_parser.add_argument(
        'config_file',
        help='Path to the configuration file to validate'
    )
    validate_config_parser.set_defaults(func=validate_config_cmd)
    
    args = parser.parse_args()
    
    if args.command is None:
        print_banner()
        parser.print_help()
        return 0
    
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())

