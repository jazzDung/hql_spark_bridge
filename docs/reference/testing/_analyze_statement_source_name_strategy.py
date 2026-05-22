# src/migration/decomposer.py
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, List, Set
import sqlglot
import sqlglot.expressions as exp



def _find_literal_assignment(stmt: exp.Expression, target_column: Optional[str] = None) -> Optional[str]:
    for alias in stmt.find_all(exp.Alias):
        if alias.alias.upper() == target_column:
            if isinstance(alias.this, exp.Literal):
                return alias.this.name.upper()
    return None


def _analyze_statement_source_key_strategy(stmt: exp.Expression, target_table: str) -> tuple[str, bool, Optional[str]]:
    """
    Trả về: (Source ID, Có phải là DDL bảng tạm không, Tên bảng tạm nếu có)
    """

    # 2. Các luật bóc tách Source ID (như cũ)
    partition = stmt.find(exp.Partition)
    if partition:
        for eq in partition.find_all(exp.EQ):
            if isinstance(eq.left, exp.Column) and eq.left.name.upper() == 'SOURCE_KEY':
                if isinstance(eq.right, exp.Literal):
                    return eq.right.name.upper(), False, None

    if isinstance(stmt, exp.Insert):
        select = stmt.expression

        if isinstance(select, exp.Select):
            for alias in select.find_all(exp.Alias):
                if alias.alias.upper() == 'SOURCE_KEY':
                    if isinstance(alias.this, exp.Literal):
                        return alias.this.name.upper(), False, None

    if stmt.find(exp.With) is not None:
        print("YES")
        ctes = stmt.find(exp.With).expressions
        for cte in ctes:
            result =  _analyze_statement_source_key_strategy(cte, target_table)
            if result != "UNKNOWN_SOURCE":
                return result

    return "UNKNOWN_SOURCE", False, None

def _analyze_statement_source_name_strategy(stmt: exp.Expression, target_table: str, is_cte: bool = False) -> tuple[str, bool, Optional[str]]:
    """
    Trả về: (Source ID, Có phải là DDL bảng tạm không, Tên bảng tạm nếu có)
    """

    # 2. Các luật bóc tách Source ID (như cũ)
    partition = stmt.find(exp.Partition)
    if partition:
        for eq in partition.find_all(exp.EQ):
            if isinstance(eq.left, exp.Column) and eq.left.name.upper() == 'SOURCE_NAME':
                if isinstance(eq.right, exp.Literal):
                    return eq.right.name.upper(), False, None

    if isinstance(stmt, exp.Insert):
        select = stmt.expression

        if isinstance(select, exp.Select):
            result = _find_literal_assignment(select, "SOURCE_NAME")
            if result is not None:
                return result, False, None

    if isinstance(stmt, exp.Union):
        print("Union YES")
        select = stmt.expression

        if isinstance(select, exp.Select):
            result = _find_literal_assignment(stmt, "SOURCE_NAME")
            if result is not None:
                return result, False, None
            else:
                result = _analyze_statement_source_name_strategy(select, target_table, is_cte=True)
                if result != ("UNKNOWN_SOURCE", False, None):
                    return result

    if is_cte and isinstance(stmt, exp.Select):
        result = _find_literal_assignment(stmt, "SOURCE_NAME")
        if result is not None:
            return result, False, None

    if stmt.find(exp.With) is not None:
        print("YES")
        ctes = stmt.find(exp.With).expressions
        for cte in ctes:
            result =  _analyze_statement_source_name_strategy(cte.this, target_table, is_cte=True)
            if result != ("UNKNOWN_SOURCE", False, None):
                return result

    return "UNKNOWN_SOURCE", False, None



def _analyze_statement_select_table_strategy(stmt: exp.Expression, target_table: str) -> tuple[str, bool, Optional[str]]:
    """
    Trả về: (Source ID, Có phải là DDL bảng tạm không, Tên bảng tạm nếu có)
    """
    tables = list(stmt.find_all(exp.Table))

    if stmt.find(exp.With) is not None:
        print("YES")
        ctes = stmt.find(exp.With).expressions
        for cte in ctes:
            result =  _analyze_statement_select_table_strategy(cte.this, target_table)
            if result != "UNKNOWN_SOURCE":
                return result

    source_candidates = set()
    for t in tables:

        name = t.name.upper()
        parts = name.split('_')
        if len(parts) >= 2 and parts[0] in ('T', 'M', 'R'):
            source_candidates.add(parts[1])

    if len(source_candidates) == 1:
        return list(source_candidates)[0], False, None
    elif len(source_candidates) > 1:
        return "FINAL_CONSOLIDATION", False, None

    for t in tables:
        if t.name.upper() == target_table:
            return "FINAL_CONSOLIDATION", False, None

    return "UNKNOWN_SOURCE", False, None


def _determine_source_recursive(stmt: exp.Expression, target_table: str, registry: Dict[str, str]) -> str:
    """
    Dùng logic truy vết: Nếu lệnh này dùng bảng tạm X,
    mà X thuộc sở hữu của Source Y -> Lệnh này thuộc Source Y.
    """
    # Luật trực tiếp (Hardcoded SOURCE_NAME)
    direct_source, _, _ = _analyze_statement(stmt, target_table)
    if direct_source in ["FINAL_CONSOLIDATION", "UNKNOWN_SOURCE"] or direct_source in registry:
        return direct_source

    # Luật truy vết (Lineage Tracking)
    table_name = direct_source.upper()
    for source in registry:
        if table_name in registry[source]:
            return source  # Trả về Source của bảng tạm đó

    return "UNKNOWN_SOURCE"