"""
Custom transformation rules module for Oracle to Databricks conversion.

This module allows users to define custom regex-based transformation rules
in a JSON configuration file to handle in-house Oracle functions and
specific conversion patterns that are not covered by the default mappings.

Example JSON configuration:
{
  "custom_rules": [
    {
      "name": "Convert MY_CUSTOM_FUNC to Databricks",
      "description": "Converts in-house MY_CUSTOM_FUNC(x, y) to CONCAT(x, y)",
      "pattern": "MY_CUSTOM_FUNC\\s*\\(\\s*([^,]+)\\s*,\\s*([^)]+)\\s*\\)",
      "replacement": "CONCAT(\\1, \\2)",
      "flags": ["IGNORECASE"],
      "enabled": true,
      "priority": 100
    }
  ],
  "settings": {
    "apply_before_default": true,
    "continue_on_error": true
  }
}
"""

import json
import re
import os
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path


@dataclass
class CustomRule:
    """Represents a single custom transformation rule."""
    name: str
    pattern: str
    replacement: str
    description: str = ""
    flags: List[str] = field(default_factory=list)
    enabled: bool = True
    priority: int = 100  # Higher priority rules are applied first
    
    def __post_init__(self):
        """Compile the regex pattern with the specified flags."""
        self._compiled_pattern = None
        self._compile_pattern()
    
    def _compile_pattern(self) -> None:
        """Compile the regex pattern with the specified flags."""
        regex_flags = 0
        flag_mapping = {
            'IGNORECASE': re.IGNORECASE,
            'I': re.IGNORECASE,
            'MULTILINE': re.MULTILINE,
            'M': re.MULTILINE,
            'DOTALL': re.DOTALL,
            'S': re.DOTALL,
            'VERBOSE': re.VERBOSE,
            'X': re.VERBOSE,
        }
        
        for flag in self.flags:
            flag_upper = flag.upper()
            if flag_upper in flag_mapping:
                regex_flags |= flag_mapping[flag_upper]
        
        try:
            self._compiled_pattern = re.compile(self.pattern, regex_flags)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern in rule '{self.name}': {e}")
    
    @property
    def compiled_pattern(self) -> re.Pattern:
        """Get the compiled regex pattern."""
        if self._compiled_pattern is None:
            self._compile_pattern()
        return self._compiled_pattern
    
    def apply(self, sql: str) -> Tuple[str, bool]:
        """
        Apply this rule to the given SQL string.
        
        Args:
            sql: The SQL string to transform
            
        Returns:
            Tuple of (transformed_sql, was_modified)
        """
        if not self.enabled:
            return sql, False
        
        new_sql = self.compiled_pattern.sub(self.replacement, sql)
        was_modified = new_sql != sql
        return new_sql, was_modified


@dataclass
class CustomRulesConfig:
    """Configuration container for custom transformation rules."""
    rules: List[CustomRule] = field(default_factory=list)
    apply_before_default: bool = True  # Apply custom rules before default transformations
    continue_on_error: bool = True  # Continue processing if a rule fails
    source_file: Optional[str] = None  # Path to the config file (for reference)
    
    def get_enabled_rules(self) -> List[CustomRule]:
        """Get all enabled rules sorted by priority (highest first)."""
        return sorted(
            [r for r in self.rules if r.enabled],
            key=lambda r: -r.priority  # Negative for descending order
        )
    
    def apply_all(self, sql: str) -> Tuple[str, List[str]]:
        """
        Apply all enabled rules to the given SQL string.
        
        Args:
            sql: The SQL string to transform
            
        Returns:
            Tuple of (transformed_sql, list_of_applied_rule_names)
        """
        applied_rules = []
        current_sql = sql
        
        for rule in self.get_enabled_rules():
            try:
                new_sql, was_modified = rule.apply(current_sql)
                if was_modified:
                    applied_rules.append(rule.name)
                    current_sql = new_sql
            except Exception as e:
                if not self.continue_on_error:
                    raise RuntimeError(f"Error applying rule '{rule.name}': {e}")
                # Log the error but continue
                applied_rules.append(f"{rule.name} (ERROR: {e})")
        
        return current_sql, applied_rules


def load_custom_rules(config_path: str) -> CustomRulesConfig:
    """
    Load custom transformation rules from a JSON configuration file.
    
    Args:
        config_path: Path to the JSON configuration file
        
    Returns:
        CustomRulesConfig object containing the loaded rules
        
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        ValueError: If the configuration file is invalid
    """
    path = Path(config_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Custom rules configuration file not found: {config_path}")
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file '{config_path}': {e}")
    
    return parse_config(config_data, source_file=str(path))


def parse_config(config_data: Dict[str, Any], source_file: Optional[str] = None) -> CustomRulesConfig:
    """
    Parse a configuration dictionary into a CustomRulesConfig object.
    
    Args:
        config_data: Dictionary containing the configuration
        source_file: Optional path to the source file (for reference)
        
    Returns:
        CustomRulesConfig object
    """
    # Parse settings
    settings = config_data.get('settings', {})
    apply_before_default = settings.get('apply_before_default', True)
    continue_on_error = settings.get('continue_on_error', True)
    
    # Parse rules
    rules_data = config_data.get('custom_rules', [])
    rules = []
    
    for i, rule_data in enumerate(rules_data):
        # Validate required fields
        if 'pattern' not in rule_data:
            raise ValueError(f"Rule {i+1} is missing required 'pattern' field")
        if 'replacement' not in rule_data:
            raise ValueError(f"Rule {i+1} is missing required 'replacement' field")
        
        rule = CustomRule(
            name=rule_data.get('name', f'Rule_{i+1}'),
            pattern=rule_data['pattern'],
            replacement=rule_data['replacement'],
            description=rule_data.get('description', ''),
            flags=rule_data.get('flags', []),
            enabled=rule_data.get('enabled', True),
            priority=rule_data.get('priority', 100),
        )
        rules.append(rule)
    
    return CustomRulesConfig(
        rules=rules,
        apply_before_default=apply_before_default,
        continue_on_error=continue_on_error,
        source_file=source_file,
    )


def create_sample_config() -> Dict[str, Any]:
    """
    Create a sample configuration dictionary with example rules.
    
    Returns:
        Dictionary that can be saved as a JSON configuration file
    """
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "description": "Custom transformation rules for Oracle to Databricks SQL conversion",
        "settings": {
            "apply_before_default": True,
            "continue_on_error": True
        },
        "custom_rules": [
            {
                "name": "Convert MY_COMPANY_CONCAT",
                "description": "Converts in-house MY_COMPANY_CONCAT(a, b, c) to CONCAT(a, b, c)",
                "pattern": r"MY_COMPANY_CONCAT\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)",
                "replacement": r"CONCAT(\1, \2, \3)",
                "flags": ["IGNORECASE"],
                "enabled": True,
                "priority": 100
            },
            {
                "name": "Convert CUSTOM_DATE_FORMAT",
                "description": "Converts CUSTOM_DATE_FORMAT(date_col) to DATE_FORMAT(date_col, 'yyyy-MM-dd')",
                "pattern": r"CUSTOM_DATE_FORMAT\s*\(\s*([^)]+)\s*\)",
                "replacement": r"DATE_FORMAT(\1, 'yyyy-MM-dd')",
                "flags": ["IGNORECASE"],
                "enabled": True,
                "priority": 100
            },
            {
                "name": "Convert GET_EMPLOYEE_NAME",
                "description": "Example: Replace in-house function with a Databricks UDF call",
                "pattern": r"GET_EMPLOYEE_NAME\s*\(\s*([^)]+)\s*\)",
                "replacement": r"my_catalog.hr_schema.get_employee_name(\1)",
                "flags": ["IGNORECASE"],
                "enabled": False,
                "priority": 90
            },
            {
                "name": "Replace schema prefix",
                "description": "Replace old Oracle schema with Databricks catalog.schema",
                "pattern": r"\bOLD_SCHEMA\.",
                "replacement": "my_catalog.new_schema.",
                "flags": ["IGNORECASE"],
                "enabled": False,
                "priority": 50
            },
            {
                "name": "Convert CUSTOM_NVL3",
                "description": "Converts in-house CUSTOM_NVL3(a, b, c) - returns b if a is not null, else c",
                "pattern": r"CUSTOM_NVL3\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)",
                "replacement": r"IF(\1 IS NOT NULL, \2, \3)",
                "flags": ["IGNORECASE"],
                "enabled": True,
                "priority": 100
            },
            {
                "name": "Convert PKG_UTILS.FORMAT_AMOUNT",
                "description": "Converts package function to Databricks FORMAT_NUMBER",
                "pattern": r"PKG_UTILS\.FORMAT_AMOUNT\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)",
                "replacement": r"FORMAT_NUMBER(CAST(\1 AS DECIMAL(18, \2)), \2)",
                "flags": ["IGNORECASE"],
                "enabled": True,
                "priority": 100
            },
            {
                "name": "Convert LOG_ERROR procedure call",
                "description": "Converts Oracle error logging procedure to Databricks logging",
                "pattern": r"PKG_LOGGING\.LOG_ERROR\s*\(\s*'([^']+)'\s*,\s*([^)]+)\s*\)",
                "replacement": r"-- [LOGGING] Error in \1: Use Databricks logging framework\n-- Original: PKG_LOGGING.LOG_ERROR('\1', \2)",
                "flags": ["IGNORECASE"],
                "enabled": True,
                "priority": 80
            }
        ]
    }


def save_sample_config(output_path: str) -> None:
    """
    Save a sample configuration file to the specified path.
    
    Args:
        output_path: Path where the sample config should be saved
    """
    sample = create_sample_config()
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(sample, f, indent=2, ensure_ascii=False)
    
    print(f"Sample configuration saved to: {output_path}")


def validate_config(config_path: str) -> Tuple[bool, List[str]]:
    """
    Validate a custom rules configuration file.
    
    Args:
        config_path: Path to the configuration file to validate
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    try:
        config = load_custom_rules(config_path)
        
        # Test each rule's pattern
        for rule in config.rules:
            test_sql = "SELECT * FROM test"
            try:
                rule.apply(test_sql)
            except Exception as e:
                errors.append(f"Rule '{rule.name}': Pattern error - {e}")
        
        return len(errors) == 0, errors
        
    except FileNotFoundError as e:
        return False, [str(e)]
    except ValueError as e:
        return False, [str(e)]
    except Exception as e:
        return False, [f"Unexpected error: {e}"]


def apply_custom_rules(sql: str, config: Optional[CustomRulesConfig]) -> Tuple[str, List[str]]:
    """
    Apply custom rules to SQL string if config is provided.
    
    Args:
        sql: The SQL string to transform
        config: Optional CustomRulesConfig object
        
    Returns:
        Tuple of (transformed_sql, list_of_applied_rule_names)
    """
    if config is None:
        return sql, []
    
    return config.apply_all(sql)
