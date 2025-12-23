"""
Custom transformations for Oracle to Databricks SQL conversion.

This module contains custom transformation functions for handling
Oracle-specific SQL constructs that require special processing.
"""

import re
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass

import sqlglot
from sqlglot import exp, transforms
from sqlglot.dialects import oracle, databricks

from .function_mappings import (
    convert_oracle_date_format,
    convert_oracle_number_format,
    get_databricks_data_type,
    get_userenv_equivalent,
    get_sys_context_equivalent,
    DIRECT_FUNCTION_MAPPINGS,
)


@dataclass
class JoinCondition:
    """Represents a join condition extracted from Oracle (+) syntax."""
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str  # 'LEFT' or 'RIGHT'
    original_condition: str


class OracleOuterJoinConverter:
    """
    Converts Oracle (+) outer join syntax to standard SQL JOIN syntax.
    
    Oracle uses (+) to denote the "optional" side of an outer join:
    - `a.col = b.col(+)` → LEFT OUTER JOIN (rows from a preserved, b is optional)
    - `a.col(+) = b.col` → RIGHT OUTER JOIN (rows from b preserved, a is optional)
    
    Example:
        Oracle:
            SELECT * FROM employees e, departments d
            WHERE e.department_id = d.department_id(+)
        
        Standard SQL:
            SELECT * FROM employees e
            LEFT OUTER JOIN departments d ON e.department_id = d.department_id
    """
    
    # Pattern to match Oracle (+) in join conditions
    # Matches: table.column(+) or alias.column(+)
    PLUS_PATTERN = re.compile(
        r'(\w+)\.(\w+)\s*\(\+\)',
        re.IGNORECASE
    )
    
    # Pattern to match a complete join condition with (+)
    # Matches conditions like: a.col = b.col(+) or a.col(+) = b.col
    JOIN_CONDITION_PATTERN = re.compile(
        r'(\w+)\.(\w+)\s*(\(\+\))?\s*=\s*(\w+)\.(\w+)\s*(\(\+\))?',
        re.IGNORECASE
    )
    
    def __init__(self):
        self.tables: Dict[str, str] = {}  # alias -> table_name
        self.join_conditions: List[JoinCondition] = []
        self.other_conditions: List[str] = []
    
    def convert(self, sql: str) -> str:
        """
        Convert Oracle (+) outer join syntax to standard SQL.
        
        Args:
            sql: Oracle SQL statement
            
        Returns:
            SQL with standard JOIN syntax
        """
        # Check if there's any (+) in the SQL
        if '(+)' not in sql:
            return sql
        
        # Reset state
        self.tables = {}
        self.join_conditions = []
        self.other_conditions = []
        
        try:
            return self._convert_query(sql)
        except Exception as e:
            # If conversion fails, return original with a warning comment
            return f"-- WARNING: Could not convert Oracle (+) outer join: {e}\n{sql}"
    
    def _convert_query(self, sql: str) -> str:
        """Convert a single query with (+) syntax."""
        
        # First, check if this is an INSERT...SELECT, UPDATE, DELETE, or MERGE statement
        # that contains (+) in a subquery or WHERE clause
        dml_prefix = ""
        dml_match = None
        
        # Handle INSERT INTO ... SELECT
        insert_match = re.match(
            r'(INSERT\s+INTO\s+[\w.]+\s*(?:\([^)]+\))?\s*)(SELECT\s+.*)',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        if insert_match:
            dml_prefix = insert_match.group(1)
            sql = insert_match.group(2)
            dml_match = insert_match
        
        # Handle UPDATE ... SET ... WHERE
        if not dml_match:
            update_match = re.match(
                r'(UPDATE\s+[\w.]+(?:\s+[\w]+)?\s+SET\s+.*?)(WHERE\s+.*)',
                sql,
                re.IGNORECASE | re.DOTALL
            )
            if update_match and '(+)' in update_match.group(2):
                # (+) is in the WHERE clause of UPDATE - need to convert subqueries
                dml_prefix = update_match.group(1)
                # For UPDATE, we need to handle subqueries in WHERE differently
                # For now, let the main logic handle it
                dml_match = None  # Reset, let it fall through
        
        # Handle DELETE FROM ... WHERE
        if not dml_match:
            delete_match = re.match(
                r'(DELETE\s+FROM\s+[\w.]+\s*)(WHERE\s+.*)',
                sql,
                re.IGNORECASE | re.DOTALL
            )
            if delete_match and '(+)' in delete_match.group(2):
                # (+) is in the WHERE clause - typically in a subquery
                dml_match = None  # Let it fall through
        
        # Extract the main parts of the query
        select_match = re.search(
            r'(SELECT\s+(?:DISTINCT\s+)?)(.*?)\s+(FROM\s+)(.*?)\s+(WHERE\s+)(.*?)(?:\s+(GROUP\s+BY|ORDER\s+BY|HAVING|UNION|INTERSECT|MINUS|FETCH|LIMIT|$))',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        
        if not select_match:
            # Try without GROUP BY/ORDER BY etc.
            select_match = re.search(
                r'(SELECT\s+(?:DISTINCT\s+)?)(.*?)\s+(FROM\s+)(.*?)\s+(WHERE\s+)(.*?)$',
                sql,
                re.IGNORECASE | re.DOTALL
            )
        
        if not select_match:
            # If still no match, return original with prefix
            return dml_prefix + sql if dml_prefix else sql
        
        select_keyword = select_match.group(1)
        select_columns = select_match.group(2)
        from_keyword = select_match.group(3)
        from_clause = select_match.group(4)
        where_keyword = select_match.group(5)
        where_clause = select_match.group(6)
        remaining = select_match.group(7) if select_match.lastindex >= 7 else ""
        
        # Get the part after WHERE clause if there's more
        remaining_sql = ""
        if remaining:
            remaining_start = select_match.end(6)
            remaining_sql = sql[remaining_start:]
        
        # Parse tables from FROM clause
        self._parse_from_clause(from_clause)
        
        # Parse WHERE clause conditions
        self._parse_where_clause(where_clause)
        
        # If no join conditions found, return original
        if not self.join_conditions:
            return dml_prefix + sql if dml_prefix else sql
        
        # Build new FROM clause with JOINs
        new_from_clause = self._build_join_clause()
        
        # Build new WHERE clause (without join conditions)
        new_where_clause = self._build_where_clause()
        
        # Reconstruct the query
        result = f"{select_keyword}{select_columns} {from_keyword}{new_from_clause}"
        
        if new_where_clause:
            result += f" {where_keyword}{new_where_clause}"
        
        if remaining_sql:
            result += remaining_sql
        
        # Prepend any DML prefix (INSERT INTO ... for INSERT...SELECT)
        return dml_prefix + result if dml_prefix else result
    
    def _parse_from_clause(self, from_clause: str) -> None:
        """Parse tables and aliases from FROM clause."""
        # Remove leading/trailing whitespace
        from_clause = from_clause.strip()
        
        # Split by comma (handling potential subqueries)
        tables = self._split_tables(from_clause)
        
        for table_spec in tables:
            table_spec = table_spec.strip()
            if not table_spec:
                continue
            
            # Match: table_name [AS] alias or just table_name
            match = re.match(
                r'(\w+(?:\.\w+)?)\s+(?:AS\s+)?(\w+)|(\w+(?:\.\w+)?)',
                table_spec,
                re.IGNORECASE
            )
            
            if match:
                if match.group(1) and match.group(2):
                    # table_name alias
                    table_name = match.group(1)
                    alias = match.group(2)
                else:
                    # just table_name (use as its own alias)
                    table_name = match.group(3)
                    alias = table_name.split('.')[-1]  # Use last part if schema.table
                
                self.tables[alias.upper()] = table_name
    
    def _split_tables(self, from_clause: str) -> List[str]:
        """Split FROM clause by commas, handling parentheses."""
        tables = []
        current = []
        depth = 0
        
        for char in from_clause:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                tables.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        
        if current:
            tables.append(''.join(current).strip())
        
        return tables
    
    def _parse_where_clause(self, where_clause: str) -> None:
        """Parse WHERE clause to extract join conditions with (+)."""
        # Split by AND (simple approach - may need enhancement for complex cases)
        conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
        
        for condition in conditions:
            condition = condition.strip()
            if not condition:
                continue
            
            # Check if this condition has (+)
            if '(+)' in condition:
                join_cond = self._parse_join_condition(condition)
                if join_cond:
                    self.join_conditions.append(join_cond)
                else:
                    self.other_conditions.append(condition)
            else:
                self.other_conditions.append(condition)
    
    def _parse_join_condition(self, condition: str) -> Optional[JoinCondition]:
        """Parse a single join condition with (+)."""
        match = self.JOIN_CONDITION_PATTERN.search(condition)
        
        if not match:
            return None
        
        left_table = match.group(1)
        left_column = match.group(2)
        left_plus = match.group(3)  # (+) or None
        right_table = match.group(4)
        right_column = match.group(5)
        right_plus = match.group(6)  # (+) or None
        
        # Determine join type
        # If right side has (+), it's a LEFT JOIN (left table is preserved)
        # If left side has (+), it's a RIGHT JOIN (right table is preserved)
        if right_plus:
            join_type = 'LEFT'
        elif left_plus:
            join_type = 'RIGHT'
        else:
            return None  # No (+) found
        
        return JoinCondition(
            left_table=left_table,
            left_column=left_column,
            right_table=right_table,
            right_column=right_column,
            join_type=join_type,
            original_condition=condition
        )
    
    def _build_join_clause(self) -> str:
        """Build the new FROM clause with explicit JOINs."""
        if not self.join_conditions:
            return ", ".join(f"{name} {alias}" for alias, name in self.tables.items())
        
        # Group join conditions by the "outer" table (the one being joined)
        # For LEFT JOIN: right_table is the outer table
        # For RIGHT JOIN: left_table is the outer table
        
        # Find the "base" table (first table that appears on the left of joins)
        joined_tables: Set[str] = set()
        base_table = None
        
        # Determine base table and join order
        join_order: List[Tuple[str, str, List[JoinCondition]]] = []
        
        for jc in self.join_conditions:
            if jc.join_type == 'LEFT':
                outer_table = jc.right_table.upper()
                inner_table = jc.left_table.upper()
            else:  # RIGHT
                outer_table = jc.left_table.upper()
                inner_table = jc.right_table.upper()
            
            if base_table is None:
                base_table = inner_table
            
            # Group by outer table
            found = False
            for i, (ot, jt, conds) in enumerate(join_order):
                if ot == outer_table:
                    conds.append(jc)
                    found = True
                    break
            
            if not found:
                join_order.append((outer_table, jc.join_type, [jc]))
            
            joined_tables.add(outer_table)
        
        # Start with base table
        if base_table and base_table in self.tables:
            result = f"{self.tables[base_table]} {base_table}"
        else:
            # Fallback: use first table
            first_alias = next(iter(self.tables))
            result = f"{self.tables[first_alias]} {first_alias}"
            base_table = first_alias
        
        # Add joins
        for outer_table, join_type, conditions in join_order:
            if outer_table in self.tables:
                table_name = self.tables[outer_table]
                
                # Build ON clause
                on_conditions = []
                for jc in conditions:
                    # Remove (+) from condition
                    clean_cond = f"{jc.left_table}.{jc.left_column} = {jc.right_table}.{jc.right_column}"
                    on_conditions.append(clean_cond)
                
                on_clause = " AND ".join(on_conditions)
                result += f"\n{join_type} OUTER JOIN {table_name} {outer_table} ON {on_clause}"
        
        # Add any tables not in joins (CROSS JOIN or inner join)
        for alias, table_name in self.tables.items():
            if alias != base_table and alias not in joined_tables:
                result += f", {table_name} {alias}"
        
        return result
    
    def _build_where_clause(self) -> str:
        """Build the new WHERE clause without join conditions."""
        if not self.other_conditions:
            return ""
        
        return " AND ".join(self.other_conditions)


class OracleTransformations:
    """
    Collection of transformation methods for Oracle to Databricks conversion.
    """
    
    @staticmethod
    def transform_decode(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle DECODE function to Databricks CASE expression.
        
        Oracle: DECODE(expr, search1, result1, search2, result2, ..., default)
        Databricks: CASE WHEN expr = search1 THEN result1 WHEN expr = search2 THEN result2 ... ELSE default END
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "DECODE":
            args = list(expression.args.get("expressions", []))
            if len(args) < 3:
                return expression
            
            expr = args[0]
            pairs = args[1:]
            
            # Build CASE expression
            ifs = []
            else_val = None
            
            i = 0
            while i < len(pairs) - 1:
                search_val = pairs[i]
                result_val = pairs[i + 1]
                
                # Create condition: expr = search_val
                condition = exp.EQ(this=expr.copy(), expression=search_val)
                ifs.append(exp.If(this=condition, true=result_val))
                i += 2
            
            # If odd number of remaining args, last one is default
            if i < len(pairs):
                else_val = pairs[i]
            
            case_expr = exp.Case(ifs=ifs, default=else_val)
            return case_expr
        
        return expression
    
    @staticmethod
    def transform_nvl2(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle NVL2 to Databricks IF/CASE expression.
        
        Oracle: NVL2(expr, val_if_not_null, val_if_null)
        Databricks: IF(expr IS NOT NULL, val_if_not_null, val_if_null)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "NVL2":
            args = list(expression.args.get("expressions", []))
            if len(args) != 3:
                return expression
            
            expr, val_not_null, val_null = args
            
            # Create: IF(expr IS NOT NULL, val_not_null, val_null)
            condition = exp.Not(this=exp.Is(this=expr, expression=exp.Null()))
            return exp.If(this=condition, true=val_not_null, false=val_null)
        
        return expression
    
    @staticmethod
    def transform_to_number(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle TO_NUMBER to Databricks CAST.
        
        Oracle: TO_NUMBER(expr)
        Databricks: CAST(expr AS DECIMAL)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TO_NUMBER":
            args = list(expression.args.get("expressions", []))
            if args:
                return exp.Cast(this=args[0], to=exp.DataType.build("DECIMAL"))
        
        return expression
    
    @staticmethod
    def transform_rownum(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle ROWNUM to ROW_NUMBER() OVER().
        
        Note: This is a simplified transformation. In practice,
        ROWNUM semantics can be complex and may need manual review.
        """
        if isinstance(expression, exp.Column) and expression.name.upper() == "ROWNUM":
            return exp.Anonymous(
                this="ROW_NUMBER",
                expressions=[],
            )
        return expression
    
    @staticmethod
    def transform_sysdate(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle SYSDATE to Databricks CURRENT_TIMESTAMP().
        """
        if isinstance(expression, exp.Column) and expression.name.upper() == "SYSDATE":
            return exp.CurrentTimestamp()
        return expression
    
    @staticmethod
    def transform_systimestamp(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle SYSTIMESTAMP to Databricks CURRENT_TIMESTAMP().
        """
        if isinstance(expression, exp.Column) and expression.name.upper() == "SYSTIMESTAMP":
            return exp.CurrentTimestamp()
        return expression
    
    @staticmethod
    def transform_listagg(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle LISTAGG to Databricks ARRAY_JOIN + COLLECT_LIST.
        
        Oracle: LISTAGG(expr, delimiter) WITHIN GROUP (ORDER BY ...)
        Databricks: ARRAY_JOIN(COLLECT_LIST(expr), delimiter)
        
        Note: ORDER BY within LISTAGG is not directly supported in Databricks.
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "LISTAGG":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                expr = args[0]
                delimiter = args[1] if len(args) > 1 else exp.Literal.string(",")
                
                # COLLECT_LIST(expr)
                collect = exp.Anonymous(this="COLLECT_LIST", expressions=[expr])
                # ARRAY_JOIN(COLLECT_LIST(expr), delimiter)
                return exp.Anonymous(this="ARRAY_JOIN", expressions=[collect, delimiter])
        
        return expression
    
    @staticmethod
    def transform_concat_operator(sql: str) -> str:
        """
        Transform Oracle string concatenation operator || to CONCAT function.
        
        This is a string-level transformation as sqlglot handles this automatically
        for most cases, but we keep it for edge cases.
        """
        # This is typically handled by sqlglot, but included for completeness
        return sql
    
    @staticmethod
    def transform_outer_join_syntax(sql: str) -> str:
        """
        Transform Oracle (+) outer join syntax to standard SQL.
        
        Oracle: SELECT * FROM a, b WHERE a.id = b.id(+)
        Standard: SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id
        
        This handles:
        - a.col = b.col(+) → LEFT OUTER JOIN
        - a.col(+) = b.col → RIGHT OUTER JOIN
        - Multiple join conditions
        - Mixed join types
        """
        if '(+)' not in sql:
            return sql
        
        converter = OracleOuterJoinConverter()
        return converter.convert(sql)
    
    @staticmethod
    def remove_oracle_hints(sql: str) -> str:
        """
        Remove Oracle optimizer hints as they're not applicable in Databricks.
        
        Oracle: SELECT /*+ PARALLEL(4) */ * FROM table
        Databricks: SELECT * FROM table
        """
        # Remove Oracle hints like /*+ ... */
        pattern = r'/\*\+[^*]*\*/'
        return re.sub(pattern, '', sql)
    
    @staticmethod
    def transform_connect_by(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle CONNECT BY hierarchical queries.
        
        This is a complex transformation that converts to Databricks recursive CTEs.
        Basic implementation - complex hierarchies may need manual review.
        """
        # This is a placeholder - full CONNECT BY transformation is complex
        # and typically needs manual review
        return expression
    
    @staticmethod
    def transform_merge_statement(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle MERGE to Databricks MERGE.
        
        Databricks supports MERGE INTO syntax similar to Oracle,
        but some clauses may need adjustment.
        """
        # sqlglot handles basic MERGE transformation
        return expression
    
    @staticmethod
    def transform_sequences(sql: str) -> str:
        """
        Transform Oracle sequence references.
        
        Oracle: sequence_name.NEXTVAL, sequence_name.CURRVAL
        Note: Databricks doesn't have sequences; suggest alternatives.
        """
        # Add comment for manual review
        if ".NEXTVAL" in sql.upper() or ".CURRVAL" in sql.upper():
            sql = f"-- WARNING: Oracle sequences detected. Consider using Databricks IDENTITY columns or custom sequence implementation.\n{sql}"
        return sql
    
    @staticmethod
    def transform_regexp_substr(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle REGEXP_SUBSTR to Databricks REGEXP_EXTRACT.
        
        Oracle: REGEXP_SUBSTR(source, pattern, position, occurrence, match_param, subexpr)
        Databricks: REGEXP_EXTRACT(source, pattern, group_index)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "REGEXP_SUBSTR":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                source = args[0]
                pattern = args[1]
                # Default group index is 0 (entire match)
                group_idx = exp.Literal.number(0)
                
                # If subexpr (6th arg) is provided, use it as group index
                if len(args) >= 6:
                    group_idx = args[5]
                
                return exp.RegexpExtract(this=source, expression=pattern, group=group_idx)
        return expression
    
    @staticmethod
    def transform_regexp_like(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle REGEXP_LIKE to Databricks RLIKE.
        
        Oracle: REGEXP_LIKE(source, pattern, match_param)
        Databricks: source RLIKE pattern
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "REGEXP_LIKE":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                source = args[0]
                pattern = args[1]
                # Note: match_param (case sensitivity, etc.) is not directly supported
                return exp.RegexpLike(this=source, expression=pattern)
        return expression
    
    @staticmethod
    def transform_regexp_count(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle REGEXP_COUNT to Databricks equivalent.
        
        Oracle: REGEXP_COUNT(source, pattern, position, match_param)
        Databricks: SIZE(REGEXP_EXTRACT_ALL(source, pattern))
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "REGEXP_COUNT":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                source = args[0]
                pattern = args[1]
                # Use REGEXP_EXTRACT_ALL and count results
                extract_all = exp.Anonymous(
                    this="REGEXP_EXTRACT_ALL",
                    expressions=[source, pattern]
                )
                return exp.Anonymous(this="SIZE", expressions=[extract_all])
        return expression
    
    @staticmethod
    def transform_wm_concat(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle WM_CONCAT (deprecated) to Databricks ARRAY_JOIN + COLLECT_LIST.
        
        Oracle: WM_CONCAT(expr)
        Databricks: ARRAY_JOIN(COLLECT_LIST(expr), ',')
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "WM_CONCAT":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                expr = args[0]
                collect = exp.Anonymous(this="COLLECT_LIST", expressions=[expr])
                delimiter = exp.Literal.string(",")
                return exp.Anonymous(this="ARRAY_JOIN", expressions=[collect, delimiter])
        return expression
    
    @staticmethod
    def transform_median(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle MEDIAN to Databricks PERCENTILE.
        
        Oracle: MEDIAN(expr)
        Databricks: PERCENTILE(expr, 0.5)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "MEDIAN":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                expr = args[0]
                return exp.Anonymous(
                    this="PERCENTILE",
                    expressions=[expr, exp.Literal.number(0.5)]
                )
        return expression
    
    @staticmethod
    def transform_sys_guid(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle SYS_GUID to Databricks UUID.
        
        Oracle: SYS_GUID()
        Databricks: UUID()
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "SYS_GUID":
            return exp.Anonymous(this="UUID", expressions=[])
        return expression
    
    @staticmethod
    def transform_ora_hash(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle ORA_HASH to Databricks HASH.
        
        Oracle: ORA_HASH(expr, max_bucket, seed)
        Databricks: HASH(expr) or MOD(HASH(expr), max_bucket)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "ORA_HASH":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                expr = args[0]
                hash_expr = exp.Anonymous(this="HASH", expressions=[expr])
                
                # If max_bucket is specified, apply modulo
                if len(args) >= 2:
                    max_bucket = args[1]
                    # Add 1 to max_bucket as Oracle is inclusive
                    bucket_plus_one = exp.Add(this=max_bucket, expression=exp.Literal.number(1))
                    return exp.Anonymous(this="MOD", expressions=[hash_expr, bucket_plus_one])
                
                return hash_expr
        return expression
    
    @staticmethod
    def transform_standard_hash(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle STANDARD_HASH to Databricks SHA2.
        
        Oracle: STANDARD_HASH(expr, 'SHA256')
        Databricks: SHA2(expr, 256)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "STANDARD_HASH":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                expr = args[0]
                # Default to SHA256, map algorithm if provided
                bits = exp.Literal.number(256)
                
                if len(args) >= 2:
                    algo = str(args[1]).upper().strip("'\"")
                    algo_bits = {
                        'SHA1': 1,  # Special case for SHA1
                        'SHA256': 256,
                        'SHA384': 384,
                        'SHA512': 512,
                        'MD5': 0  # Use MD5 function instead
                    }
                    if algo == 'MD5':
                        return exp.Anonymous(this="MD5", expressions=[expr])
                    elif algo == 'SHA1':
                        return exp.Anonymous(this="SHA1", expressions=[expr])
                    bits = exp.Literal.number(algo_bits.get(algo, 256))
                
                return exp.Anonymous(this="SHA2", expressions=[expr, bits])
        return expression
    
    @staticmethod
    def transform_userenv(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle USERENV to Databricks equivalents.
        
        Oracle: USERENV('CURRENT_USER')
        Databricks: CURRENT_USER()
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "USERENV":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                param = str(args[0]).upper().strip("'\"")
                equiv, _ = get_userenv_equivalent(param)
                
                # Return appropriate expression based on equivalent
                if equiv == 'CURRENT_USER':
                    return exp.Anonymous(this="CURRENT_USER", expressions=[])
                elif equiv == 'CURRENT_SCHEMA':
                    return exp.Anonymous(this="CURRENT_SCHEMA", expressions=[])
                elif equiv == 'CURRENT_CATALOG':
                    return exp.Anonymous(this="CURRENT_CATALOG", expressions=[])
                elif equiv.startswith("'") or equiv.startswith('"'):
                    # Literal value
                    return exp.Literal.string(equiv.strip("'\""))
                else:
                    return exp.Literal.string(equiv)
        return expression
    
    @staticmethod
    def transform_sys_context(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle SYS_CONTEXT to Databricks equivalents.
        
        Oracle: SYS_CONTEXT('USERENV', 'CURRENT_USER')
        Databricks: CURRENT_USER()
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "SYS_CONTEXT":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                namespace = str(args[0]).upper().strip("'\"")
                param = str(args[1]).upper().strip("'\"")
                
                equiv, _ = get_sys_context_equivalent(namespace, param)
                
                if equiv == 'CURRENT_USER':
                    return exp.Anonymous(this="CURRENT_USER", expressions=[])
                elif equiv == 'CURRENT_SCHEMA':
                    return exp.Anonymous(this="CURRENT_SCHEMA", expressions=[])
                elif equiv == 'CURRENT_CATALOG':
                    return exp.Anonymous(this="CURRENT_CATALOG", expressions=[])
                elif equiv.startswith("'") or equiv.startswith('"'):
                    return exp.Literal.string(equiv.strip("'\""))
                else:
                    return exp.Literal.string(equiv)
        return expression
    
    @staticmethod
    def transform_lnnvl(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle LNNVL to Databricks equivalent.
        
        Oracle: LNNVL(condition) - returns TRUE if condition is FALSE or NULL
        Databricks: NOT(condition) OR condition IS NULL
        
        Note: LNNVL is typically used in WHERE clauses.
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "LNNVL":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                condition = args[0]
                # NOT(condition) OR condition IS NULL
                not_cond = exp.Not(this=condition.copy())
                is_null = exp.Is(this=condition.copy(), expression=exp.Null())
                return exp.Or(this=not_cond, expression=is_null)
        return expression
    
    @staticmethod
    def transform_nanvl(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle NANVL to Databricks equivalent.
        
        Oracle: NANVL(expr, alternative) - returns alternative if expr is NaN
        Databricks: IF(ISNAN(expr), alternative, expr)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "NANVL":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                expr = args[0]
                alternative = args[1]
                # IF(ISNAN(expr), alternative, expr)
                isnan = exp.Anonymous(this="ISNAN", expressions=[expr.copy()])
                return exp.If(this=isnan, true=alternative, false=expr)
        return expression
    
    @staticmethod
    def transform_numtodsinterval(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle NUMTODSINTERVAL to Databricks INTERVAL.
        
        Oracle: NUMTODSINTERVAL(n, 'DAY'|'HOUR'|'MINUTE'|'SECOND')
        Databricks: INTERVAL n DAY/HOUR/MINUTE/SECOND
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "NUMTODSINTERVAL":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                n = args[0]
                unit = str(args[1]).upper().strip("'\"")
                
                # Map units
                unit_map = {
                    'DAY': 'DAY',
                    'HOUR': 'HOUR',
                    'MINUTE': 'MINUTE',
                    'SECOND': 'SECOND'
                }
                spark_unit = unit_map.get(unit, 'DAY')
                
                return exp.Interval(this=n, unit=exp.Var(this=spark_unit))
        return expression
    
    @staticmethod
    def transform_numtoyminterval(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle NUMTOYMINTERVAL to Databricks INTERVAL.
        
        Oracle: NUMTOYMINTERVAL(n, 'YEAR'|'MONTH')
        Databricks: INTERVAL n YEAR/MONTH
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "NUMTOYMINTERVAL":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                n = args[0]
                unit = str(args[1]).upper().strip("'\"")
                
                unit_map = {'YEAR': 'YEAR', 'MONTH': 'MONTH'}
                spark_unit = unit_map.get(unit, 'MONTH')
                
                return exp.Interval(this=n, unit=exp.Var(this=spark_unit))
        return expression
    
    @staticmethod
    def transform_collect(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle COLLECT to Databricks COLLECT_LIST.
        
        Oracle: COLLECT(expr)
        Databricks: COLLECT_LIST(expr)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "COLLECT":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                return exp.Anonymous(this="COLLECT_LIST", expressions=args)
        return expression
    
    @staticmethod
    def transform_approx_median(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle APPROX_MEDIAN to Databricks APPROX_PERCENTILE.
        
        Oracle: APPROX_MEDIAN(expr)
        Databricks: APPROX_PERCENTILE(expr, 0.5)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "APPROX_MEDIAN":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                expr = args[0]
                return exp.Anonymous(
                    this="APPROX_PERCENTILE",
                    expressions=[expr, exp.Literal.number(0.5)]
                )
        return expression
    
    @staticmethod
    def transform_ratio_to_report(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle RATIO_TO_REPORT to Databricks equivalent.
        
        Oracle: RATIO_TO_REPORT(expr) OVER (partition_clause)
        Databricks: expr / SUM(expr) OVER (partition_clause)
        
        Note: This is complex and may not capture all cases.
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "RATIO_TO_REPORT":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                expr = args[0]
                # Simple case: expr / SUM(expr) OVER ()
                sum_expr = exp.Sum(this=expr.copy())
                window = exp.Window(this=sum_expr)
                return exp.Div(this=expr, expression=window)
        return expression
    
    @staticmethod
    def transform_json_value(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle JSON_VALUE to Databricks GET_JSON_OBJECT or :.
        
        Oracle: JSON_VALUE(json_doc, '$.path')
        Databricks: GET_JSON_OBJECT(json_doc, '$.path') or json_doc:path
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "JSON_VALUE":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                json_doc = args[0]
                path = args[1]
                return exp.Anonymous(
                    this="GET_JSON_OBJECT",
                    expressions=[json_doc, path]
                )
        return expression
    
    @staticmethod
    def transform_json_query(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle JSON_QUERY to Databricks GET_JSON_OBJECT.
        
        Oracle: JSON_QUERY(json_doc, '$.path')
        Databricks: GET_JSON_OBJECT(json_doc, '$.path')
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "JSON_QUERY":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                json_doc = args[0]
                path = args[1]
                return exp.Anonymous(
                    this="GET_JSON_OBJECT",
                    expressions=[json_doc, path]
                )
        return expression
    
    @staticmethod
    def transform_json_exists(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle JSON_EXISTS to Databricks equivalent.
        
        Oracle: JSON_EXISTS(json_doc, '$.path')
        Databricks: GET_JSON_OBJECT(json_doc, '$.path') IS NOT NULL
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "JSON_EXISTS":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                json_doc = args[0]
                path = args[1]
                get_json = exp.Anonymous(
                    this="GET_JSON_OBJECT",
                    expressions=[json_doc, path]
                )
                return exp.Not(this=exp.Is(this=get_json, expression=exp.Null()))
        return expression
    
    @staticmethod
    def transform_json_object(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle JSON_OBJECT to Databricks equivalent.
        
        Oracle: JSON_OBJECT('key1' VALUE val1, 'key2' VALUE val2)
        Databricks: TO_JSON(NAMED_STRUCT('key1', val1, 'key2', val2))
        
        Note: Simplified transformation - may need manual adjustment.
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "JSON_OBJECT":
            # This is complex; return a placeholder that signals need for review
            return exp.Anonymous(
                this="TO_JSON",
                expressions=[exp.Anonymous(
                    this="NAMED_STRUCT",
                    expressions=list(expression.args.get("expressions", []))
                )]
            )
        return expression
    
    @staticmethod
    def transform_json_array(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle JSON_ARRAY to Databricks equivalent.
        
        Oracle: JSON_ARRAY(val1, val2, val3)
        Databricks: TO_JSON(ARRAY(val1, val2, val3))
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "JSON_ARRAY":
            args = list(expression.args.get("expressions", []))
            array_expr = exp.Array(expressions=args)
            return exp.Anonymous(this="TO_JSON", expressions=[array_expr])
        return expression
    
    @staticmethod
    def transform_json_arrayagg(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle JSON_ARRAYAGG to Databricks equivalent.
        
        Oracle: JSON_ARRAYAGG(expr)
        Databricks: TO_JSON(COLLECT_LIST(expr))
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "JSON_ARRAYAGG":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                collect = exp.Anonymous(this="COLLECT_LIST", expressions=[args[0]])
                return exp.Anonymous(this="TO_JSON", expressions=[collect])
        return expression
    
    @staticmethod
    def transform_vsize(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle VSIZE to Databricks LENGTH (byte length).
        
        Oracle: VSIZE(expr) - returns byte size
        Databricks: LENGTH(expr) - approximation (character length)
        
        Note: For exact byte length, use OCTET_LENGTH if available.
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "VSIZE":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                # Try OCTET_LENGTH for better accuracy
                return exp.Anonymous(this="OCTET_LENGTH", expressions=[args[0]])
        return expression
    
    @staticmethod  
    def transform_to_char(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle TO_CHAR with format conversion.
        
        Oracle: TO_CHAR(date_expr, 'YYYY-MM-DD') or TO_CHAR(number_expr, '999,999.99')
        Databricks: DATE_FORMAT(date_expr, 'yyyy-MM-dd') or FORMAT_NUMBER(number_expr, format)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TO_CHAR":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                expr = args[0]
                fmt = args[1]
                
                # Get the format string value
                fmt_str = str(fmt).strip("'\"")
                
                # Check if it looks like a date format or number format
                date_indicators = ['YYYY', 'YY', 'MM', 'DD', 'HH', 'MI', 'SS', 'MON', 'DAY']
                is_date_format = any(ind in fmt_str.upper() for ind in date_indicators)
                
                if is_date_format:
                    # Convert date format
                    spark_fmt = convert_oracle_date_format(fmt_str)
                    return exp.Anonymous(
                        this="DATE_FORMAT",
                        expressions=[expr, exp.Literal.string(spark_fmt)]
                    )
                else:
                    # Number format - use FORMAT_NUMBER
                    spark_fmt = convert_oracle_number_format(fmt_str)
                    if spark_fmt:
                        return exp.Anonymous(
                            this="FORMAT_NUMBER",
                            expressions=[expr, exp.Literal.string(spark_fmt)]
                        )
            elif len(args) == 1:
                # Simple TO_CHAR without format - use CAST
                return exp.Cast(this=args[0], to=exp.DataType.build("STRING"))
        return expression
    
    @staticmethod
    def transform_to_date(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle TO_DATE with format conversion.
        
        Oracle: TO_DATE(string_expr, 'YYYY-MM-DD')
        Databricks: TO_DATE(string_expr, 'yyyy-MM-dd')
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TO_DATE":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                expr = args[0]
                fmt = args[1]
                
                # Convert the format string
                fmt_str = str(fmt).strip("'\"")
                spark_fmt = convert_oracle_date_format(fmt_str)
                
                return exp.Anonymous(
                    this="TO_DATE",
                    expressions=[expr, exp.Literal.string(spark_fmt)]
                )
        return expression
    
    @staticmethod
    def transform_to_timestamp(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle TO_TIMESTAMP with format conversion.
        
        Oracle: TO_TIMESTAMP(string_expr, 'YYYY-MM-DD HH24:MI:SS')
        Databricks: TO_TIMESTAMP(string_expr, 'yyyy-MM-dd HH:mm:ss')
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TO_TIMESTAMP":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 2:
                expr = args[0]
                fmt = args[1]
                
                # Convert the format string
                fmt_str = str(fmt).strip("'\"")
                spark_fmt = convert_oracle_date_format(fmt_str)
                
                return exp.Anonymous(
                    this="TO_TIMESTAMP",
                    expressions=[expr, exp.Literal.string(spark_fmt)]
                )
        return expression
    
    @staticmethod
    def transform_rawtohex(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle RAWTOHEX to Databricks HEX.
        
        Oracle: RAWTOHEX(raw_expr)
        Databricks: HEX(raw_expr)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "RAWTOHEX":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                return exp.Anonymous(this="HEX", expressions=[args[0]])
        return expression
    
    @staticmethod
    def transform_hextoraw(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle HEXTORAW to Databricks UNHEX.
        
        Oracle: HEXTORAW(hex_string)
        Databricks: UNHEX(hex_string)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "HEXTORAW":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                return exp.Anonymous(this="UNHEX", expressions=[args[0]])
        return expression
    
    @staticmethod
    def transform_add_months(expression: exp.Expression) -> exp.Expression:
        """
        Ensure ADD_MONTHS is properly handled.
        
        Oracle: ADD_MONTHS(date, n)
        Databricks: ADD_MONTHS(date, n) - same syntax
        """
        # Databricks supports ADD_MONTHS with same syntax
        return expression
    
    @staticmethod
    def transform_trunc_date(expression: exp.Expression) -> exp.Expression:
        """
        Transform Oracle TRUNC for dates to Databricks DATE_TRUNC.
        
        Oracle: TRUNC(date_expr, 'MM') or TRUNC(date_expr)
        Databricks: DATE_TRUNC('MONTH', date_expr) or DATE(date_expr)
        """
        if isinstance(expression, exp.Anonymous) and expression.name.upper() == "TRUNC":
            args = list(expression.args.get("expressions", []))
            if len(args) >= 1:
                expr = args[0]
                
                if len(args) >= 2:
                    fmt = str(args[1]).upper().strip("'\"")
                    
                    # Map Oracle truncation formats to Databricks
                    format_map = {
                        'YYYY': 'YEAR', 'YY': 'YEAR', 'YEAR': 'YEAR', 'Y': 'YEAR',
                        'Q': 'QUARTER',
                        'MM': 'MONTH', 'MON': 'MONTH', 'MONTH': 'MONTH',
                        'WW': 'WEEK', 'IW': 'WEEK', 'W': 'WEEK',
                        'DD': 'DAY', 'DDD': 'DAY', 'D': 'DAY', 'DAY': 'DAY', 'DY': 'DAY',
                        'HH': 'HOUR', 'HH24': 'HOUR', 'HH12': 'HOUR',
                        'MI': 'MINUTE',
                        'SS': 'SECOND'
                    }
                    
                    spark_fmt = format_map.get(fmt, 'DAY')
                    return exp.Anonymous(
                        this="DATE_TRUNC",
                        expressions=[exp.Literal.string(spark_fmt), expr]
                    )
                else:
                    # No format means truncate to day
                    return exp.Anonymous(this="DATE", expressions=[expr])
        return expression


def apply_all_transformations(expression: exp.Expression) -> exp.Expression:
    """
    Apply all custom Oracle transformations to an expression.
    
    Args:
        expression: sqlglot expression to transform
        
    Returns:
        Transformed expression
    """
    transformations = [
        # Core Oracle function transformations
        OracleTransformations.transform_decode,
        OracleTransformations.transform_nvl2,
        OracleTransformations.transform_to_number,
        OracleTransformations.transform_sysdate,
        OracleTransformations.transform_systimestamp,
        OracleTransformations.transform_listagg,
        
        # Regular expression functions
        OracleTransformations.transform_regexp_substr,
        OracleTransformations.transform_regexp_like,
        OracleTransformations.transform_regexp_count,
        
        # Aggregate and analytic functions
        OracleTransformations.transform_wm_concat,
        OracleTransformations.transform_median,
        OracleTransformations.transform_collect,
        OracleTransformations.transform_approx_median,
        OracleTransformations.transform_ratio_to_report,
        
        # Hash and GUID functions
        OracleTransformations.transform_sys_guid,
        OracleTransformations.transform_ora_hash,
        OracleTransformations.transform_standard_hash,
        
        # Context and environment functions
        OracleTransformations.transform_userenv,
        OracleTransformations.transform_sys_context,
        
        # NULL handling functions
        OracleTransformations.transform_lnnvl,
        OracleTransformations.transform_nanvl,
        
        # Interval functions
        OracleTransformations.transform_numtodsinterval,
        OracleTransformations.transform_numtoyminterval,
        
        # JSON functions
        OracleTransformations.transform_json_value,
        OracleTransformations.transform_json_query,
        OracleTransformations.transform_json_exists,
        OracleTransformations.transform_json_object,
        OracleTransformations.transform_json_array,
        OracleTransformations.transform_json_arrayagg,
        
        # Conversion functions
        OracleTransformations.transform_vsize,
        OracleTransformations.transform_to_char,
        OracleTransformations.transform_to_date,
        OracleTransformations.transform_to_timestamp,
        OracleTransformations.transform_rawtohex,
        OracleTransformations.transform_hextoraw,
        
        # Date functions
        OracleTransformations.transform_trunc_date,
    ]
    
    for transform in transformations:
        expression = expression.transform(transform)
    
    return expression


def apply_string_transformations(sql: str) -> str:
    """
    Apply string-level transformations that can't be done at AST level.
    
    Args:
        sql: SQL string to transform
        
    Returns:
        Transformed SQL string
    """
    sql = OracleTransformations.remove_oracle_hints(sql)
    sql = OracleTransformations.transform_sequences(sql)
    sql = OracleTransformations.transform_outer_join_syntax(sql)
    return sql

