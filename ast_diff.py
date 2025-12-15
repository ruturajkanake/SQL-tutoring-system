# ast_diff.py
from typing import Dict, List, Tuple
import sqlglot
from sqlglot import parse_one
from sqlglot.expressions import Column, Table, Subquery, Select
from canonicalizer import canonicalize

class ASTDiffResult:
    def __init__(self):
        self.parse_error = None
        self.structural_diffs: List[str] = []
        self.normalized_student: str = ""
        self.normalized_reference: str = ""

def _collect_columns(ast) -> List[str]:
    return sorted({c.sql().lower() for c in ast.find_all(Column)})

def _collect_tables(ast) -> List[str]:
    return sorted({t.sql().lower() for t in ast.find_all(Table)})

def _count_subqueries(ast) -> int:
    return len(list(ast.find_all(Subquery)))

def ast_diff(student_sql: str, reference_sql: str, dialect: str = "ansi") -> ASTDiffResult:
    res = ASTDiffResult()
    # Canonicalize first (best-effort)
    can_student = canonicalize(student_sql, dialect=dialect)
    can_ref = canonicalize(reference_sql, dialect=dialect)

    res.normalized_student = can_student or student_sql
    res.normalized_reference = can_ref or reference_sql

    try:
        ast_s = parse_one(res.normalized_student, read="ansi", error_level="raise")
        ast_r = parse_one(res.normalized_reference, read="ansi", error_level="raise")
    except Exception as e:
        res.parse_error = str(e)
        return res

    # Compare projected columns
    s_cols, r_cols = _collect_columns(ast_s), _collect_columns(ast_r)
    if s_cols != r_cols:
        missing = [c for c in r_cols if c not in s_cols]
        extra = [c for c in s_cols if c not in r_cols]
        if missing:
            res.structural_diffs.append(f"Missing columns in SELECT: {missing}")
        if extra:
            res.structural_diffs.append(f"Extra columns in SELECT: {extra}")

    # Compare tables
    s_tables, r_tables = _collect_tables(ast_s), _collect_tables(ast_r)
    if s_tables != r_tables:
        missing_t = [t for t in r_tables if t not in s_tables]
        extra_t = [t for t in s_tables if t not in r_tables]
        if missing_t:
            res.structural_diffs.append(f"Missing tables in FROM/JOIN: {missing_t}")
        if extra_t:
            res.structural_diffs.append(f"Extra tables in FROM/JOIN: {extra_t}")

    # Compare number of subqueries (quick structural check)
    s_sub = _count_subqueries(ast_s)
    r_sub = _count_subqueries(ast_r)
    if s_sub != r_sub:
        res.structural_diffs.append(f"Different nested-subquery count (student={s_sub}, reference={r_sub})")

    # Check GROUP BY columns
    s_group = {g.sql().lower() for g in (ast_s.args.get("group").expressions if ast_s.args.get("group") else [])}
    r_group = {g.sql().lower() for g in (ast_r.args.get("group").expressions if ast_r.args.get("group") else [])}
    if s_group != r_group:
        res.structural_diffs.append(f"GROUP BY mismatch: student={sorted(s_group)}, ref={sorted(r_group)}")

    # Join condition structural check: compare join expressions strings
    s_joins = sorted({str(j).lower() for j in ast_s.find_all(sqlglot.expressions.Join)})
    r_joins = sorted({str(j).lower() for j in ast_r.find_all(sqlglot.expressions.Join)})
    if s_joins != r_joins:
        res.structural_diffs.append("JOIN structure differs (check join keys and types)")

    return res
