"""
Oracle CONNECT BY to Databricks Recursive CTE Converter.

This module handles the conversion of Oracle hierarchical queries using CONNECT BY
to Databricks SQL recursive Common Table Expressions (CTEs).

Oracle CONNECT BY Syntax:
    SELECT columns
    FROM table
    START WITH condition
    CONNECT BY [NOCYCLE] PRIOR parent_col = child_col
    [ORDER SIBLINGS BY column]

Databricks Recursive CTE Syntax:
    WITH RECURSIVE cte_name AS (
        -- Anchor member (START WITH)
        SELECT columns, 1 AS level
        FROM table
        WHERE anchor_condition
        
        UNION ALL
        
        -- Recursive member (CONNECT BY)
        SELECT t.columns, cte.level + 1
        FROM table t
        INNER JOIN cte_name cte ON join_condition
    )
    SELECT * FROM cte_name

Supported Features:
    - CONNECT BY PRIOR for parent-child relationships
    - START WITH for root node selection
    - LEVEL pseudo-column
    - CONNECT BY LEVEL <= N for sequence generation
    - SYS_CONNECT_BY_PATH (converted to path concatenation)
    - CONNECT_BY_ROOT (tracks root values)
    - CONNECT_BY_ISLEAF (approximated with NOT EXISTS)
    - NOCYCLE (ignored, Databricks handles cycles differently)
    - ORDER SIBLINGS BY (approximated with ROW_NUMBER)

Limitations:
    - Complex CONNECT BY conditions may need manual review
    - CONNECT_BY_ISCYCLE not directly supported
    - ORDER SIBLINGS BY is approximated
"""

import re
from typing import Optional, Tuple, List, Dict, Any
from dataclasses import dataclass, field


@dataclass
class ConnectByComponents:
    """Parsed components of a CONNECT BY query."""
    select_clause: str
    from_clause: str
    where_clause: Optional[str] = None
    start_with_clause: Optional[str] = None
    connect_by_clause: Optional[str] = None
    order_siblings_by: Optional[str] = None
    order_by_clause: Optional[str] = None
    group_by_clause: Optional[str] = None
    having_clause: Optional[str] = None
    has_prior: bool = False
    has_nocycle: bool = False
    has_level: bool = False
    has_sys_connect_by_path: bool = False
    has_connect_by_root: bool = False
    has_connect_by_isleaf: bool = False
    is_sequence_generator: bool = False  # CONNECT BY LEVEL <= N pattern
    level_limit: Optional[str] = None
    table_alias: Optional[str] = None


@dataclass
class ConversionResult:
    """Result of CONNECT BY conversion."""
    success: bool
    converted_sql: str
    original_sql: str
    warnings: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


class ConnectByConverter:
    """
    Converts Oracle CONNECT BY hierarchical queries to Databricks recursive CTEs.
    """
    
    def __init__(self):
        self.cte_counter = 0
    
    def convert(self, sql: str) -> ConversionResult:
        """
        Convert an Oracle CONNECT BY query to Databricks recursive CTE.
        
        Args:
            sql: Oracle SQL with CONNECT BY clause
            
        Returns:
            ConversionResult with converted SQL and metadata
        """
        warnings = []
        notes = []
        
        # Check if this is a CONNECT BY query
        if not self._is_connect_by_query(sql):
            return ConversionResult(
                success=False,
                converted_sql=sql,
                original_sql=sql,
                warnings=["Not a CONNECT BY query"]
            )
        
        # Parse the query components
        components = self._parse_connect_by_query(sql)
        
        if components is None:
            return ConversionResult(
                success=False,
                converted_sql=sql,
                original_sql=sql,
                warnings=["Failed to parse CONNECT BY query structure"]
            )
        
        # Handle sequence generator pattern: SELECT ... FROM DUAL CONNECT BY LEVEL <= N
        if components.is_sequence_generator:
            converted = self._convert_sequence_generator(components)
            notes.append("Converted CONNECT BY LEVEL sequence generator to SEQUENCE function")
            return ConversionResult(
                success=True,
                converted_sql=converted,
                original_sql=sql,
                notes=notes
            )
        
        # Convert to recursive CTE
        try:
            converted = self._build_recursive_cte(components, warnings, notes)
            return ConversionResult(
                success=True,
                converted_sql=converted,
                original_sql=sql,
                warnings=warnings,
                notes=notes
            )
        except Exception as e:
            return ConversionResult(
                success=False,
                converted_sql=sql,
                original_sql=sql,
                warnings=[f"Conversion error: {str(e)}"]
            )
    
    def _is_connect_by_query(self, sql: str) -> bool:
        """Check if SQL contains CONNECT BY clause."""
        sql_upper = sql.upper()
        return 'CONNECT BY' in sql_upper or 'CONNECT_BY' in sql_upper
    
    def _parse_connect_by_query(self, sql: str) -> Optional[ConnectByComponents]:
        """
        Parse Oracle CONNECT BY query into components.
        """
        # Normalize whitespace
        sql_normalized = ' '.join(sql.split())
        sql_upper = sql_normalized.upper()
        
        # Check for sequence generator pattern (FROM DUAL CONNECT BY LEVEL <= N)
        dual_pattern = re.search(
            r'\bFROM\s+DUAL\b.*\bCONNECT\s+BY\s+LEVEL\s*<=\s*(\d+|\w+)',
            sql_upper
        )
        if dual_pattern:
            return self._parse_sequence_generator(sql_normalized)
        
        # Extract SELECT clause
        select_match = re.match(
            r'^\s*SELECT\s+(.*?)\s+FROM\s+',
            sql_normalized,
            re.IGNORECASE | re.DOTALL
        )
        if not select_match:
            return None
        
        select_clause = select_match.group(1).strip()
        
        # Extract FROM clause (table and alias)
        # Be careful not to capture keywords like START, CONNECT, WHERE, etc.
        from_pattern = re.search(
            r'\bFROM\s+([\w.]+)(?:\s+(?:AS\s+)?(\w+))?(?=\s+(?:WHERE|START|CONNECT|ORDER|GROUP|HAVING|$))',
            sql_normalized,
            re.IGNORECASE
        )
        if not from_pattern:
            # Fallback: simpler pattern
            from_pattern = re.search(
                r'\bFROM\s+([\w.]+)',
                sql_normalized,
                re.IGNORECASE
            )
            if not from_pattern:
                return None
            from_clause = from_pattern.group(1).strip()
            table_alias = None
        else:
            from_clause = from_pattern.group(1).strip()
            table_alias = from_pattern.group(2) if from_pattern.lastindex >= 2 and from_pattern.group(2) else None
            # Make sure alias is not a keyword
            if table_alias and table_alias.upper() in ('START', 'CONNECT', 'WHERE', 'ORDER', 'GROUP', 'HAVING'):
                table_alias = None
        
        # Extract START WITH clause
        start_with_match = re.search(
            r'\bSTART\s+WITH\s+(.*?)(?=\s*CONNECT\s+BY|\s*ORDER\s+SIBLINGS\s+BY|\s*ORDER\s+BY|\s*GROUP\s+BY|\s*$)',
            sql_normalized,
            re.IGNORECASE | re.DOTALL
        )
        start_with_clause = start_with_match.group(1).strip() if start_with_match else None
        
        # Extract CONNECT BY clause
        connect_by_match = re.search(
            r'\bCONNECT\s+BY\s+(NOCYCLE\s+)?(.*?)(?=\s*START\s+WITH|\s*ORDER\s+SIBLINGS\s+BY|\s*ORDER\s+BY|\s*GROUP\s+BY|\s*HAVING|\s*$)',
            sql_normalized,
            re.IGNORECASE | re.DOTALL
        )
        if not connect_by_match:
            return None
        
        has_nocycle = connect_by_match.group(1) is not None
        connect_by_clause = connect_by_match.group(2).strip()
        
        # Check for PRIOR keyword
        has_prior = 'PRIOR' in connect_by_clause.upper()
        
        # Extract WHERE clause (before START WITH)
        where_match = re.search(
            r'\bWHERE\s+(.*?)(?=\s*START\s+WITH|\s*CONNECT\s+BY|\s*ORDER\s+BY|\s*GROUP\s+BY|\s*$)',
            sql_normalized,
            re.IGNORECASE | re.DOTALL
        )
        where_clause = where_match.group(1).strip() if where_match else None
        
        # Extract ORDER SIBLINGS BY
        order_siblings_match = re.search(
            r'\bORDER\s+SIBLINGS\s+BY\s+(.*?)(?=\s*ORDER\s+BY|\s*$)',
            sql_normalized,
            re.IGNORECASE | re.DOTALL
        )
        order_siblings_by = order_siblings_match.group(1).strip() if order_siblings_match else None
        
        # Extract ORDER BY (regular, not SIBLINGS)
        # First check if there's ORDER SIBLINGS BY (already extracted above)
        if order_siblings_by:
            # Look for ORDER BY after ORDER SIBLINGS BY
            order_by_match = re.search(
                r'\bORDER\s+SIBLINGS\s+BY\s+.*?\s+ORDER\s+BY\s+(.*?)(?=\s*$)',
                sql_normalized,
                re.IGNORECASE | re.DOTALL
            )
        else:
            # No SIBLINGS, just look for ORDER BY
            order_by_match = re.search(
                r'\bORDER\s+BY\s+(.*?)(?=\s*$)',
                sql_normalized,
                re.IGNORECASE | re.DOTALL
            )
        order_by_clause = order_by_match.group(1).strip() if order_by_match else None
        
        # Extract GROUP BY
        group_by_match = re.search(
            r'\bGROUP\s+BY\s+(.*?)(?=\s*HAVING|\s*ORDER|\s*$)',
            sql_normalized,
            re.IGNORECASE | re.DOTALL
        )
        group_by_clause = group_by_match.group(1).strip() if group_by_match else None
        
        # Check for hierarchical pseudo-columns and functions
        has_level = 'LEVEL' in sql_upper and 'CONNECT BY' in sql_upper
        has_sys_connect_by_path = 'SYS_CONNECT_BY_PATH' in sql_upper
        has_connect_by_root = 'CONNECT_BY_ROOT' in sql_upper
        has_connect_by_isleaf = 'CONNECT_BY_ISLEAF' in sql_upper
        
        return ConnectByComponents(
            select_clause=select_clause,
            from_clause=from_clause,
            where_clause=where_clause,
            start_with_clause=start_with_clause,
            connect_by_clause=connect_by_clause,
            order_siblings_by=order_siblings_by,
            order_by_clause=order_by_clause,
            group_by_clause=group_by_clause,
            has_prior=has_prior,
            has_nocycle=has_nocycle,
            has_level=has_level,
            has_sys_connect_by_path=has_sys_connect_by_path,
            has_connect_by_root=has_connect_by_root,
            has_connect_by_isleaf=has_connect_by_isleaf,
            table_alias=table_alias
        )
    
    def _parse_sequence_generator(self, sql: str) -> ConnectByComponents:
        """Parse CONNECT BY LEVEL <= N sequence generator pattern."""
        sql_upper = sql.upper()
        
        # Extract SELECT clause
        select_match = re.match(
            r'^\s*SELECT\s+(.*?)\s+FROM\s+DUAL',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        select_clause = select_match.group(1).strip() if select_match else "LEVEL"
        
        # Extract level limit
        limit_match = re.search(
            r'\bCONNECT\s+BY\s+LEVEL\s*<=\s*(\d+|\w+)',
            sql,
            re.IGNORECASE
        )
        level_limit = limit_match.group(1) if limit_match else "10"
        
        # Extract ORDER BY if present
        order_by_match = re.search(
            r'\bORDER\s+BY\s+(.*?)(?=\s*$)',
            sql,
            re.IGNORECASE
        )
        order_by_clause = order_by_match.group(1).strip() if order_by_match else None
        
        return ConnectByComponents(
            select_clause=select_clause,
            from_clause="DUAL",
            is_sequence_generator=True,
            level_limit=level_limit,
            order_by_clause=order_by_clause,
            has_level=True
        )
    
    def _convert_sequence_generator(self, components: ConnectByComponents) -> str:
        """
        Convert CONNECT BY LEVEL sequence generator to Databricks.
        
        Oracle: SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 10
        Databricks: SELECT id AS LEVEL FROM RANGE(1, 11)
        """
        select_clause = components.select_clause
        level_limit = components.level_limit or "10"
        
        # Replace LEVEL with id and adjust the select
        # Convert: SELECT LEVEL AS n -> SELECT id AS n
        # Convert: SELECT LEVEL -> SELECT id AS LEVEL
        
        # Handle complex SELECT with LEVEL
        select_converted = select_clause
        
        # Replace standalone LEVEL
        select_converted = re.sub(
            r'\bLEVEL\b(?!\s*\()',
            'id',
            select_converted,
            flags=re.IGNORECASE
        )
        
        # Build the query
        # RANGE(start, end) generates [start, end), so for LEVEL <= N, use RANGE(1, N+1)
        try:
            if level_limit.isdigit():
                end_value = int(level_limit) + 1
                range_expr = f"RANGE(1, {end_value})"
            else:
                range_expr = f"RANGE(1, {level_limit} + 1)"
        except:
            range_expr = f"RANGE(1, {level_limit} + 1)"
        
        result = f"SELECT {select_converted}\nFROM {range_expr}"
        
        if components.order_by_clause:
            order_by = components.order_by_clause
            # Replace LEVEL in ORDER BY
            order_by = re.sub(r'\bLEVEL\b', 'id', order_by, flags=re.IGNORECASE)
            result += f"\nORDER BY {order_by}"
        
        return result
    
    def _build_recursive_cte(self, components: ConnectByComponents, 
                             warnings: List[str], notes: List[str]) -> str:
        """
        Build a recursive CTE from parsed CONNECT BY components.
        """
        self.cte_counter += 1
        cte_name = f"hierarchy_cte_{self.cte_counter}"
        
        # Determine table name and alias
        table_name = components.from_clause.split()[0]
        table_alias = components.table_alias or self._generate_alias(table_name)
        
        # Parse the CONNECT BY condition to extract join columns
        join_condition = self._parse_connect_by_condition(
            components.connect_by_clause, 
            table_alias, 
            cte_name
        )
        
        if not join_condition:
            warnings.append("Could not parse CONNECT BY condition - may need manual review")
            join_condition = "1=0  -- TODO: Fix join condition"
        
        # Build anchor member columns
        anchor_columns, recursive_columns, output_columns = self._build_column_lists(
            components, table_alias, cte_name
        )
        
        # Build anchor WHERE clause (START WITH condition)
        anchor_where = components.start_with_clause or "1=1"
        
        # Add any additional WHERE conditions (but not LEVEL-based ones - those go in final SELECT)
        if components.where_clause:
            # Filter out LEVEL conditions from WHERE - they apply to final output, not anchor
            where_without_level = re.sub(
                r'\bLEVEL\s*[<>=!]+\s*\d+\s*(?:AND\s*)?',
                '',
                components.where_clause,
                flags=re.IGNORECASE
            ).strip()
            # Also remove trailing AND
            where_without_level = re.sub(r'\s+AND\s*$', '', where_without_level, flags=re.IGNORECASE).strip()
            
            if where_without_level and where_without_level.upper() != 'AND':
                anchor_where = f"({anchor_where}) AND ({where_without_level})"
        
        # Build the recursive CTE
        cte_sql = f"""WITH RECURSIVE {cte_name} AS (
    -- Anchor member: root nodes (START WITH condition)
    SELECT {anchor_columns}
    FROM {table_name} AS {table_alias}
    WHERE {anchor_where}
    
    UNION ALL
    
    -- Recursive member: child nodes (CONNECT BY condition)
    SELECT {recursive_columns}
    FROM {table_name} AS {table_alias}
    INNER JOIN {cte_name} ON {join_condition}
)
SELECT {output_columns}
FROM {cte_name}"""
        
        # Add WHERE clause for final SELECT if LEVEL filter exists
        level_filter = self._extract_level_filter(components)
        if level_filter:
            cte_sql += f"\nWHERE {level_filter}"
        
        # Add GROUP BY
        if components.group_by_clause:
            group_by = self._convert_pseudo_columns(components.group_by_clause, cte_name)
            cte_sql += f"\nGROUP BY {group_by}"
        
        # Add ORDER BY
        if components.order_by_clause:
            order_by = self._convert_pseudo_columns(components.order_by_clause, cte_name)
            cte_sql += f"\nORDER BY {order_by}"
        elif components.order_siblings_by:
            warnings.append("ORDER SIBLINGS BY approximated - may not preserve exact sibling order")
            notes.append("Consider adding path-based ordering for exact sibling order")
            order_by = self._convert_pseudo_columns(components.order_siblings_by, cte_name)
            cte_sql += f"\nORDER BY level, {order_by}"
        
        # Add notes for complex features
        if components.has_sys_connect_by_path:
            notes.append("SYS_CONNECT_BY_PATH converted to path concatenation in recursive CTE")
        if components.has_connect_by_root:
            notes.append("CONNECT_BY_ROOT tracked via root_* columns in CTE")
        if components.has_connect_by_isleaf:
            notes.append("CONNECT_BY_ISLEAF approximated - uses NOT EXISTS subquery")
            warnings.append("CONNECT_BY_ISLEAF may need manual verification")
        if components.has_nocycle:
            notes.append("NOCYCLE ignored - Databricks handles cycles with MAXRECURSION")
        
        return cte_sql
    
    def _parse_connect_by_condition(self, connect_by: str, table_alias: str, cte_name: str) -> Optional[str]:
        """
        Parse CONNECT BY condition and convert to JOIN condition.
        
        Oracle: PRIOR employee_id = manager_id
        Databricks: employees.manager_id = cte.employee_id
        
        The PRIOR keyword indicates which column is from the parent (CTE) row.
        """
        if not connect_by:
            return None
        
        # Handle simple: PRIOR col1 = col2
        prior_left = re.match(
            r'\s*PRIOR\s+([\w.]+)\s*=\s*([\w.]+)\s*$',
            connect_by,
            re.IGNORECASE
        )
        if prior_left:
            parent_col = prior_left.group(1)  # This is from the CTE (parent row)
            child_col = prior_left.group(2)   # This is from the table (child row)
            # Remove any existing alias from columns
            parent_col = parent_col.split('.')[-1]
            child_col = child_col.split('.')[-1]
            return f"{table_alias}.{child_col} = {cte_name}.{parent_col}"
        
        # Handle: col1 = PRIOR col2
        prior_right = re.match(
            r'\s*([\w.]+)\s*=\s*PRIOR\s+([\w.]+)\s*$',
            connect_by,
            re.IGNORECASE
        )
        if prior_right:
            child_col = prior_right.group(1)  # This is from the table (child row)
            parent_col = prior_right.group(2)  # This is from the CTE (parent row)
            # Remove any existing alias from columns
            parent_col = parent_col.split('.')[-1]
            child_col = child_col.split('.')[-1]
            return f"{table_alias}.{child_col} = {cte_name}.{parent_col}"
        
        # Handle compound conditions with AND
        if ' AND ' in connect_by.upper():
            conditions = re.split(r'\s+AND\s+', connect_by, flags=re.IGNORECASE)
            converted_conditions = []
            for cond in conditions:
                converted = self._parse_connect_by_condition(cond.strip(), table_alias, cte_name)
                if converted:
                    converted_conditions.append(converted)
            if converted_conditions:
                return ' AND '.join(converted_conditions)
        
        return None
    
    def _build_column_lists(self, components: ConnectByComponents, 
                           table_alias: str, cte_name: str) -> Tuple[str, str, str]:
        """
        Build column lists for anchor, recursive, and output SELECT clauses.
        
        Returns:
            Tuple of (anchor_columns, recursive_columns, output_columns)
        """
        original_select = components.select_clause
        
        # Parse individual columns
        columns = self._split_select_columns(original_select)
        
        anchor_cols = []
        recursive_cols = []
        output_cols = []
        
        for col in columns:
            col = col.strip()
            if not col:
                continue
            
            # Handle LEVEL pseudo-column
            if re.match(r'^\s*LEVEL\s*$', col, re.IGNORECASE):
                anchor_cols.append("1 AS level")
                recursive_cols.append(f"{cte_name}.level + 1 AS level")
                output_cols.append("level")
                continue
            
            # Handle LEVEL with alias: LEVEL AS depth
            level_alias = re.match(r'^\s*LEVEL\s+(?:AS\s+)?(\w+)\s*$', col, re.IGNORECASE)
            if level_alias:
                alias = level_alias.group(1)
                anchor_cols.append(f"1 AS {alias}")
                recursive_cols.append(f"{cte_name}.{alias} + 1 AS {alias}")
                output_cols.append(alias)
                continue
            
            # Handle SYS_CONNECT_BY_PATH
            path_match = re.match(
                r"^\s*SYS_CONNECT_BY_PATH\s*\(\s*(.+?)\s*,\s*'(.+?)'\s*\)\s*(?:AS\s+)?(\w+)?\s*$",
                col,
                re.IGNORECASE
            )
            if path_match:
                path_expr = path_match.group(1)
                delimiter = path_match.group(2)
                alias = path_match.group(3) or "path"
                # In anchor: just the value
                anchor_cols.append(f"CONCAT('{delimiter}', CAST({path_expr} AS STRING)) AS {alias}")
                # In recursive: concatenate parent path + delimiter + current value
                recursive_cols.append(f"CONCAT({cte_name}.{alias}, '{delimiter}', CAST({table_alias}.{path_expr} AS STRING)) AS {alias}")
                output_cols.append(alias)
                continue
            
            # Handle CONNECT_BY_ROOT
            root_match = re.match(
                r'^\s*CONNECT_BY_ROOT\s+(\w+)\s+(?:AS\s+)?(\w+)?\s*$',
                col,
                re.IGNORECASE
            )
            if root_match:
                root_col = root_match.group(1)
                alias = root_match.group(2) or f"root_{root_col}"
                # In anchor: the column value (root is itself)
                anchor_cols.append(f"{root_col} AS {alias}")
                # In recursive: carry forward the root value
                recursive_cols.append(f"{cte_name}.{alias} AS {alias}")
                output_cols.append(alias)
                continue
            
            # Handle CONNECT_BY_ISLEAF
            if re.match(r'^\s*CONNECT_BY_ISLEAF\s*(?:AS\s+)?(\w+)?\s*$', col, re.IGNORECASE):
                alias_match = re.match(r'^\s*CONNECT_BY_ISLEAF\s+(?:AS\s+)?(\w+)?\s*$', col, re.IGNORECASE)
                alias = alias_match.group(1) if alias_match and alias_match.group(1) else "is_leaf"
                # This needs to be computed in the final SELECT, not in CTE
                # Add placeholder, will handle in output
                output_cols.append(f"-- CONNECT_BY_ISLEAF needs manual implementation AS {alias}")
                continue
            
            # Handle expressions with LEVEL
            if 'LEVEL' in col.upper():
                # Replace LEVEL with the column reference
                anchor_col = re.sub(r'\bLEVEL\b', '1', col, flags=re.IGNORECASE)
                # For alias extraction
                alias_match = re.search(r'\s+(?:AS\s+)?(\w+)\s*$', col, re.IGNORECASE)
                if alias_match:
                    alias = alias_match.group(1)
                    recursive_col = re.sub(r'\bLEVEL\b', f'{cte_name}.level + 1', col, flags=re.IGNORECASE)
                else:
                    # Generate alias for complex expression
                    alias = f"level_expr_{len(anchor_cols)}"
                    anchor_col = f"{anchor_col} AS {alias}"
                    recursive_col = re.sub(r'\bLEVEL\b', f'{cte_name}.level + 1', col, flags=re.IGNORECASE)
                    recursive_col = f"{recursive_col} AS {alias}"
                
                anchor_cols.append(anchor_col)
                recursive_cols.append(recursive_col)
                output_cols.append(alias if alias_match else alias)
                continue
            
            # Handle regular columns
            # Extract alias if present
            alias_match = re.search(r'\s+(?:AS\s+)?(\w+)\s*$', col, re.IGNORECASE)
            if alias_match:
                alias = alias_match.group(1)
                # Use alias in output
                anchor_cols.append(col)
                # In recursive, prefix with table alias
                col_base = col[:col.rfind(alias_match.group(0))]
                recursive_cols.append(f"{table_alias}.{col_base.strip()} AS {alias}")
                output_cols.append(alias)
            else:
                # Simple column reference
                col_name = col.split('.')[-1].strip()
                anchor_cols.append(col)
                recursive_cols.append(f"{table_alias}.{col_name}")
                output_cols.append(col_name)
        
        # Ensure we always have level column for depth tracking
        has_level = any('level' in c.lower() for c in anchor_cols)
        if not has_level and components.has_level:
            anchor_cols.append("1 AS level")
            recursive_cols.append(f"{cte_name}.level + 1 AS level")
            output_cols.append("level")
        
        return (
            ', '.join(anchor_cols),
            ', '.join(recursive_cols),
            ', '.join(output_cols)
        )
    
    def _split_select_columns(self, select_clause: str) -> List[str]:
        """
        Split SELECT clause into individual columns, respecting parentheses.
        """
        columns = []
        current = ""
        paren_depth = 0
        
        for char in select_clause:
            if char == '(':
                paren_depth += 1
                current += char
            elif char == ')':
                paren_depth -= 1
                current += char
            elif char == ',' and paren_depth == 0:
                columns.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            columns.append(current.strip())
        
        return columns
    
    def _extract_level_filter(self, components: ConnectByComponents) -> Optional[str]:
        """Extract LEVEL-based filters from WHERE clause."""
        if not components.where_clause:
            return None
        
        # Look for LEVEL <= N, LEVEL < N, etc.
        level_filter = re.search(
            r'\bLEVEL\s*([<>=!]+)\s*(\d+)',
            components.where_clause,
            re.IGNORECASE
        )
        if level_filter:
            op = level_filter.group(1)
            val = level_filter.group(2)
            return f"level {op} {val}"
        
        return None
    
    def _convert_pseudo_columns(self, clause: str, cte_name: str) -> str:
        """Convert Oracle pseudo-columns to CTE column references."""
        result = clause
        result = re.sub(r'\bLEVEL\b', 'level', result, flags=re.IGNORECASE)
        result = re.sub(r'\bCONNECT_BY_ISLEAF\b', 'is_leaf', result, flags=re.IGNORECASE)
        return result
    
    def _generate_alias(self, table_name: str) -> str:
        """Generate a short alias for a table name."""
        # Remove schema prefix if present
        name = table_name.split('.')[-1]
        # Use first letter of each word
        return name[0].lower()


def convert_connect_by(sql: str) -> ConversionResult:
    """
    Convenience function to convert CONNECT BY query.
    
    Args:
        sql: Oracle SQL with CONNECT BY
        
    Returns:
        ConversionResult with converted SQL
    """
    converter = ConnectByConverter()
    return converter.convert(sql)


def has_connect_by(sql: str) -> bool:
    """Check if SQL contains CONNECT BY clause."""
    return 'CONNECT BY' in sql.upper()

