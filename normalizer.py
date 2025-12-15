# normalizer.py
from sqlglot import parse_one, expressions
from typing import Optional

def normalize_select_order(ast):
    """
    Deterministically reorder SELECT expressions by their SQL string.
    Returns modified AST.
    """
    select = ast.find(expressions.Select)
    if select and getattr(select, "expressions", None):
        # sort by sql representation
        select.expressions = sorted(select.expressions, key=lambda e: e.sql().lower())
    return ast

def normalize_joins(ast):
    """
    Reorder join list deterministically by their sql string.
    """
    joins = list(ast.find_all(expressions.Join))
    if joins:
        # naive: convert to set of strings and reapply order
        # More advanced rewriting requires graph isomorphism (not implemented)
        return sorted(joins, key=lambda j: j.sql().lower())
    return joins

def full_normalize(sql: str, dialect: str = "ansi") -> Optional[str]:
    """
    Full normalization pipeline: parse → normalize select order → to_sql
    """
    try:
        ast = parse_one(sql, read=dialect, error_level="raise")
        ast = normalize_select_order(ast)
        # apply more transformations here if needed
        return ast.to_sql(dialect="ansi", pretty=False)
    except Exception:
        return None
