#!/usr/bin/env python3

from typing import Optional, List, Dict, Any, Tuple, Callable
from sqlglot import parse_one
from sqlglot.expressions import Column, Table, Join, Subquery, Select, Group, Window, Func, CTE
from sqlglot.errors import ParseError
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
from semantic_diff import semantic_diff
import duckdb
import re
import json
import traceback
import requests
import os
import html

# -----------------------
# LLM integration: Fireworks Serverless API (Level-4 hints)
# -----------------------

FIREWORKS_API_KEY = os.getenv("FIREWORKS_API_KEY")


FIREWORKS_MODEL = "accounts/fireworks/models/gpt-oss-20b"


def call_fireworks_api(prompt: str, max_tokens: int = 500) -> str:
    """
    Calls Fireworks serverless chat completion endpoint.
    Returns model text or "" on error.
    """
    if not FIREWORKS_API_KEY:
        return ""

    url = "https://api.fireworks.ai/inference/v1/chat/completions"

    try:
        response = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {FIREWORKS_API_KEY}"
            },
            json={
                "model": FIREWORKS_MODEL,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.0,        
                "top_p": 1.0,
                "n": 1,
                "stream": False,
                "max_tokens": max_tokens,
            },
            timeout=20
        )

        data = response.json()

        if "choices" in data and len(data["choices"]) > 0:
            return data["choices"][0]["message"]["content"].strip()

        return ""
    except Exception as e:
        print(f"Fireworks API error: {e}")
        return ""
    
# -----------------------
# LLM integration: safe Level-4 conceptual hint generator (Qwen / HF transformers)
# -----------------------

def summarize_context_for_llm(ast_diffs, metadata, exec_student, exec_ref, constraint_name, evidence):
    # Extract safe, abstract structural details
    student_ops = metadata.get("student_ops", [])
    ref_ops = metadata.get("ref_ops", [])
    exec_rows_student = exec_student.get("rows")
    exec_rows_ref = exec_ref.get("rows")

    return {
        "constraint": constraint_name,
        "ast_diffs": ast_diffs,
        "student_ops": student_ops,
        "ref_ops": ref_ops,
        "row_diff": (
            (len(exec_rows_student) if exec_rows_student else 0)
            - (len(exec_rows_ref) if exec_rows_ref else 0)
        ),
        "evidence": list(evidence.keys()) if evidence else []
    }

def summarize_sql_for_llm(ast):
    if ast is None:
        return "Could not parse query."

    tables = sorted({t.name for t in ast.find_all("Table")})
    columns = sorted({c.alias_or_name for c in ast.find_all("Column")})
    ops = sorted({type(n).__name__ for n in ast.find_all()})

    return f"tables: {tables}; columns: {columns}; operations: {ops}"


# -------------------------------------------
# Build Safe Prompt
# -------------------------------------------

def build_llm_prompt(summary, student_ast, ref_ast, level: int, context) -> str:
    """
    Produces a safe, SQL-free prompt for conceptual hints.
    """
    student_summary = summarize_sql_for_llm(student_ast)
    reference_summary = summarize_sql_for_llm(ref_ast)

    if level == 4:
        prompt = f"""
Provide one short conceptual hint about a logic difference in a query.

Student summary: {student_summary}
Reference summary: {reference_summary}

Information:
- category: {summary['constraint']}
- structural differences: {summary['ast_diffs']}
- student operators: {summary['student_ops']}
- reference operators: {summary['ref_ops']}
- row count diff: {summary['row_diff']}

Rules:
- MAX 25 words
- No code
- High-level concept only
- Output only the hint text, no prefix.
"""
    else:
        prompt = f"""
Provide one short conceptual hint about for a student to correct their SQL query. Do not provide any SQL code or mention about the reference.

Student SQL: {context.get('student_sql','')}
Reference SQL: {context.get('reference_sql','')}

Information: 
- row count diff: {summary['row_diff']}

Rules:
- MAX 25 words
- No code
- High-level concept only
- Output only the hint text, no prefix.
"""

    return prompt


# -------------------------------------------
# Output Validator (Strict but not overly restrictive)
# -------------------------------------------

BANNED_SQL_KEYWORDS = [
]

def validate_llm_output(text: str) -> bool:
    if not text:
        return False

    lower = text.lower().strip()

    # Block real SQL keywords
    for kw in BANNED_SQL_KEYWORDS:
        if kw in lower:
            return False

    # Must be short
    if len(text.split()) > 35:
        return False

    return True


# -------------------------------------------
# Safe LLM Hint Generator
# -------------------------------------------

def llm_generate_safe_hint(ast_diffs, metadata, exec_student, exec_ref,
                           matched_constraint_name, matched_evidence, level, context):

    summary = summarize_context_for_llm(
        ast_diffs, metadata, exec_student, exec_ref,
        matched_constraint_name, matched_evidence
    )

    prompt = build_llm_prompt(summary, metadata.get("student_ast"), metadata.get("ref_ast"), level, context)

    # Retry up to 3 times
    raw = call_fireworks_api(prompt)

    raw = html.unescape(raw).strip()

    # keep at most 2 short sentences
    parts = [p.strip() for p in raw.split(".") if p.strip()]
    if parts:
        candidate = parts[0]
        if len(parts) > 1:
            candidate = f"{parts[0]}. {parts[1]}."
        raw = candidate

    if validate_llm_output(raw):
        return raw

    # fallback
    return "Review the underlying logic structure; ensure conditions and relationships reflect the intended meaning."



# -----------------------------
# Utilities: canonicalize / normalize
# -----------------------------
def canonicalize(sql: str, dialect: str = "mysql") -> Tuple[Optional[str], Optional[str]]:
    """
    Parse and return a canonical/normalized SQL using sqlglot.
    Returns (canonical_sql, error_message)
    """
    if not sql or not sql.strip():
        return None, "Empty SQL"
    try:
        ast = parse_one(sql, read=dialect, error_level="raise")
        # Deterministic normalize: sort SELECT expressions, canonicalize names if possible
        try:
            sel = ast.find(Select)
            if sel and getattr(sel, "expressions", None):
                sel.expressions = sorted(sel.expressions, key=lambda e: e.sql().lower())
        except Exception:
            pass
        # Use sql to get normalized representation
        can = ast.sql(dialect="mysql", pretty=False)
        # reparse to get a stable representation
        ast2 = parse_one(can, read="mysql", error_level="raise")
        return ast2.sql(dialect="mysql", pretty=False), None
    except ParseError as pe:
        return None, str(pe)
    except Exception as e:
        return None, str(e)

# -----------------------------
# AST Diff / Structural comparison
# -----------------------------
class ASTDiffResult:
    def __init__(self):
        self.parse_error_student: Optional[str] = None
        self.parse_error_reference: Optional[str] = None
        self.normalized_student: Optional[str] = None
        self.normalized_reference: Optional[str] = None
        self.structural_diffs: List[str] = []
        self.metadata: Dict[str, Any] = {}

# def collect_columns(ast) -> List[str]:
#     if not ast:
#         return []
#     return sorted({c.sql().lower() for c in ast.find_all(Column)})

def collect_select_columns(ast) -> List[str]:
    if not ast:
        return []
    select_node = ast.find(Select)
    if not select_node:
        return []
    
    cols = []
    for expr in select_node.expressions:
        if expr.sql().strip() == '*':
            return ['*']
        for col in expr.find_all(Column):
            cols.append(col.sql().lower())
    return sorted(set(cols))

def collect_all_columns(ast) -> List[str]:
    if not ast:
        return []
    return sorted({c.sql().lower() for c in ast.find_all(Column)})

def collect_tables(ast) -> List[str]:
    if not ast:
        return []
    return sorted({t.sql().lower() for t in ast.find_all(Table)})

def count_subqueries(ast) -> int:
    if not ast:
        return 0
    return len(list(ast.find_all(Subquery)))

def has_window(ast) -> bool:
    if not ast:
        return False
    return bool(list(ast.find_all(Window)))

def has_cte(ast) -> bool:
    if not ast:
        return False
    return bool(list(ast.find_all(CTE)))

def ast_diff(student_sql: str, reference_sql: str, dialect: str = "mysql") -> ASTDiffResult:
    res = ASTDiffResult()
    # canonicalize both
    can_s, err_s = canonicalize(student_sql, dialect)
    can_r, err_r = canonicalize(reference_sql, dialect)
    res.normalized_student = can_s
    res.normalized_reference = can_r
    res.parse_error_student = err_s
    res.parse_error_reference = err_r

    # if parse errors exist for both, return
    try:
        ast_s = parse_one(can_s, read="mysql", error_level="raise") if can_s else None
    except Exception as e:
        ast_s = None
        if not res.parse_error_student:
            res.parse_error_student = str(e)
    try:
        ast_r = parse_one(can_r, read="mysql", error_level="raise") if can_r else None
    except Exception as e:
        ast_r = None
        if not res.parse_error_reference:
            res.parse_error_reference = str(e)

    # structural checks
    try:
        s_select_cols = collect_select_columns(ast_s)
        r_select_cols = collect_select_columns(ast_r)
        
        if set(s_select_cols) != set(r_select_cols):
            missing = [c for c in r_select_cols if c not in s_select_cols]
            extra = [c for c in s_select_cols if c not in r_select_cols]
            if missing:
                res.structural_diffs.append(f"Missing SELECT columns: {missing}")
            if extra:
                res.structural_diffs.append(f"Extra SELECT columns: {extra}")
        
        # Store in metadata
        res.metadata["student_columns"] = s_select_cols
        res.metadata["reference_columns"] = r_select_cols
        s_tables = collect_tables(ast_s)
        r_tables = collect_tables(ast_r)
        if set(s_tables) != set(r_tables):
            missing_t = [t for t in r_tables if t not in s_tables]
            extra_t = [t for t in s_tables if t not in r_tables]
            if missing_t:
                res.structural_diffs.append(f"Missing tables in FROM/JOIN: {missing_t}")
            if extra_t:
                res.structural_diffs.append(f"Extra tables in FROM/JOIN: {extra_t}")
        s_sub = count_subqueries(ast_s)
        r_sub = count_subqueries(ast_r)
        if s_sub != r_sub:
            res.structural_diffs.append(f"Different nested-subquery count (student={s_sub}, ref={r_sub})")
        if has_window(ast_r) != has_window(ast_s):
            res.structural_diffs.append("Window function usage differs between student and reference.")
        if has_cte(ast_r) != has_cte(ast_s):
            res.structural_diffs.append("CTE/ WITH usage differs between student and reference.")
        # group by differences
        try:
            s_group = {g.sql().lower() for g in (ast_s.args.get("group").expressions if ast_s and ast_s.args.get("group") else [])}
            r_group = {g.sql().lower() for g in (ast_r.args.get("group").expressions if ast_r and ast_r.args.get("group") else [])}
            if s_group != r_group:
                res.structural_diffs.append(f"GROUP BY mismatch: student={sorted(s_group)}, ref={sorted(r_group)}")
        except Exception:
            pass
        # join structure check (textual best-effort)
        try:
            s_joins = sorted({j.sql().lower() for j in ast_s.find_all(Join)}) if ast_s else []
            r_joins = sorted({j.sql().lower() for j in ast_r.find_all(Join)}) if ast_r else []
            if s_joins != r_joins:
                res.structural_diffs.append("Join structure differs (check join keys/types).")
        except Exception:
            pass
        # record metadata
        res.metadata["student_columns"] = s_select_cols
        res.metadata["reference_columns"] = r_select_cols
        res.metadata["student_tables"] = s_tables
        res.metadata["reference_tables"] = r_tables
        res.metadata["student_subqueries"] = s_sub
        res.metadata["reference_subqueries"] = r_sub
    except Exception as e:
        res.structural_diffs.append("AST diff internal error: " + str(e))
    return res

# -----------------------------
# Execution-based verifier (DuckDB)
# -----------------------------
def execute_in_memory(setup_sql: Optional[str], sql: str) -> Dict[str, Any]:
    """
    Execute given SQL against an in-memory DuckDB database after running setup_sql (if provided).
    Returns dict: { success: bool, error: str|None, rows: list, cols: list }
    """
    conn = duckdb.connect(database=":memory:")
    try:
        if setup_sql:
            conn.execute(setup_sql)
        res = conn.execute(sql)
        rows = res.fetchall()
        cols = [desc[0] for desc in res.description] if res.description else []
        return {"success": True, "error": None, "rows": rows, "cols": cols}
    except Exception as e:
        return {"success": False, "error": str(e), "rows": [], "cols": []}
    finally:
        conn.close()

def compare_results(res_s: Dict[str, Any], res_r: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    try:
        if not res_s["success"] or not res_r["success"]:
            return False, None
        
        # First check if column names match
        cols_s = [str(c).lower() for c in res_s.get("cols", [])]
        cols_r = [str(c).lower() for c in res_r.get("cols", [])]
        
        if cols_s != cols_r:
            return False, None
        
        # Then check if row data matches
        def normalize(rows):
            # convert to tuple of simple types
            normalized = [tuple(r) for r in rows]
            return sorted(normalized)
        
        eq = normalize(res_s["rows"]) == normalize(res_r["rows"])
        return eq, None
    except Exception as e:
        return False, str(e)

# -----------------------------
# Constraint framework
# -----------------------------
# Constraint: function(ctx) -> (bool, evidence_dict)
ConstraintFn = Callable[[Dict[str, Any]], Tuple[bool, Dict[str, Any]]]

class Constraint:
    def __init__(self, id_: int, name: str, priority: int, checker: ConstraintFn,
                 hint_l1: str, hint_l2: str):
        self.id = id_
        self.name = name
        self.priority = priority
        self.checker = checker
        self.hint_l1 = hint_l1
        self.hint_l2 = hint_l2

# We'll collect ~55 constraints covering many categories.
CONSTRAINTS: List[Constraint] = []

def register_constraint(c: Constraint):
    CONSTRAINTS.append(c)

# Helper context accessors
def ctx_student_ast(ctx): return ctx.get("student_ast")
def ctx_ref_ast(ctx): return ctx.get("ref_ast")
def ctx_student_sql(ctx): return ctx.get("student_sql","").lower()
def ctx_ref_sql(ctx): return ctx.get("reference_sql","").lower()
def ctx_exec_student(ctx): return ctx.get("exec_student", {})
def ctx_exec_ref(ctx): return ctx.get("exec_ref", {})

# --- Constraint implementations (grouped) ---
# We'll assign IDs in sequence; priority lower => earlier/higher priority
_next_id = 1
def next_id():
    global _next_id
    i = _next_id
    _next_id += 1
    return i

# 1. Parsing / Syntax constraints
def check_parse_error(ctx):
    if ctx.get("parse_error_student"):
        return True, {"error": ctx["parse_error_student"]}
    return False, {}
register_constraint(Constraint(next_id(), "parse_error", 1, check_parse_error,
                               "Your SQL has a syntax error.",
                               "Check for missing commas, unmatched parentheses, or incorrect keywords."))

# 2. FROM / Tables / Join constraints
def check_missing_table(ctx):
    s_tables = ctx.get("metadata",{}).get("student_tables",[])
    r_tables = ctx.get("metadata",{}).get("reference_tables",[])
    missing = [t for t in r_tables if t not in s_tables]
    if missing:
        return True, {"missing_tables": missing}
    return False, {}
register_constraint(Constraint(next_id(), "missing_table", 2, check_missing_table,
                               "A required table is missing from your FROM clause.",
                               "Include all tables needed to access the required columns or join conditions."))

def check_extra_table(ctx):
    s_tables = ctx.get("metadata",{}).get("student_tables",[])
    r_tables = ctx.get("metadata",{}).get("reference_tables",[])
    extra = [t for t in s_tables if t not in r_tables]
    if extra:
        return True, {"extra_tables": extra}
    return False, {}
register_constraint(Constraint(next_id(), "extra_table", 50, check_extra_table,
                               "Your query references unnecessary tables.",
                               "Remove extra tables that aren't needed, as they may cause duplicate rows."))

def check_missing_join_condition(ctx):
    # naive: multiple tables but no JOIN expressions and no WHERE clause joining them
    ast = ctx_student_ast(ctx)
    if ast:
        tables = collect_tables(ast)
        joins = list(ast.find_all(Join))
        if len(tables) > 1 and not joins and "where" not in ctx_student_sql(ctx):
            return True, {"tables": tables}
    return False, {}
register_constraint(Constraint(next_id(), "missing_join_condition", 3, check_missing_join_condition,
                               "Multiple tables detected but no join conditions found.",
                               "Add ON conditions or WHERE predicates to specify how tables relate."))

def check_join_type_mismatch(ctx):
    # crude textual check for left join vs join mismatches
    s = ctx_student_sql(ctx)
    r = ctx_ref_sql(ctx)
    if ("left join" in s and "left join" not in r) or ("left join" in r and "left join" not in s):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "join_type_mismatch", 40, check_join_type_mismatch,
                               "Your JOIN type differs from expected (INNER vs LEFT/RIGHT).",
                               "Use LEFT JOIN to keep unmatched rows or INNER JOIN to exclude them."))

def check_join_on_constant(ctx):
    s = ctx_student_sql(ctx)
    if re.search(r"on\s+\d+\s*=\s*\d+", s):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "join_on_constant", 30, check_join_on_constant,
                               "JOIN condition uses constants (e.g., ON 1=1).",
                               "Replace constant conditions with actual column comparisons to avoid cross joins."))

def check_self_join_aliasing(ctx):
    s = ctx_student_sql(ctx)
    # detect same table repeated without aliases (heuristic)
    ast = ctx_student_ast(ctx)
    if not ast:
        return False, {}
    tables = collect_tables(ast)
    for t in tables:
        if s.count(t) > 1 and (" as " not in s and t + " " in s):
            return True, {"table": t}
    return False, {}
register_constraint(Constraint(next_id(), "self_join_alias", 7, check_self_join_aliasing,
                               "Self-join detected without proper aliasing.",
                               "Use table aliases (e.g., employees e1, employees e2) to distinguish instances."))

# 3. SELECT / Projection constraints
def check_select_star(ctx):
    if "*" in ctx_student_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "select_star", 80, check_select_star,
                               "Avoid using SELECT * in your query.",
                               "List specific columns instead to match expected output and improve clarity."))

def check_missing_select_column(ctx):
    s_cols = ctx.get("metadata",{}).get("student_columns",[])
    r_cols = ctx.get("metadata",{}).get("reference_columns",[])
    if s_cols == ['*']:
        return False, {}
    
    missing = [c for c in r_cols if c not in s_cols]
    if missing:
        return True, {"missing_columns": missing}
    return False, {}
register_constraint(Constraint(next_id(), "missing_select_column", 60, check_missing_select_column,
                               "Required columns are missing from your SELECT clause.",
                               "Add the missing columns or compute them using appropriate expressions."))

def check_extra_select_column(ctx):
    s_cols = ctx.get("metadata",{}).get("student_columns",[])
    r_cols = ctx.get("metadata",{}).get("reference_columns",[])
    if s_cols == ['*'] and r_cols != ['*']:
        return True, {"extra_columns": ["*"]}
    
    extra = [c for c in s_cols if c not in r_cols]
    if extra:
        return True, {"extra_columns": extra}
    return False, {}
register_constraint(Constraint(next_id(), "extra_select_column", 4, check_extra_select_column,
                               "Your SELECT contains extra columns not required.",
                               "Remove unnecessary columns to match the expected output structure."))

def check_aggregate_without_group_by(ctx):
    ast = ctx_student_ast(ctx)
    if ast:
        agg_funcs = [f for f in ast.find_all(Func) if getattr(f, "name", "").upper() in {"SUM","COUNT","AVG","MIN","MAX"}]
        if agg_funcs:
            sel = ast.find(Select)
            nonagg = []
            if sel:
                for e in sel.expressions:
                    cols = list(e.find_all(Column))
                    funcs = list(e.find_all(Func))
                    if cols and not any(getattr(ff,"name","").upper() in {"SUM","COUNT","AVG","MIN","MAX"} for ff in funcs):
                        nonagg.extend([c.sql().lower() for c in cols])
            if nonagg and not ast.args.get("group"):
                return True, {"nonagg": nonagg}
    return False, {}
register_constraint(Constraint(next_id(), "aggregate_without_groupby", 5, check_aggregate_without_group_by,
                               "Aggregate functions used without GROUP BY clause.",
                               "Add GROUP BY with all non-aggregated columns from your SELECT clause."))

def check_group_by_missing_cols(ctx):
    ast_r = ctx_ref_ast(ctx)
    ast_s = ctx_student_ast(ctx)
    if ast_r and ast_r.args.get("group"):
        r_group = {g.sql().lower() for g in ast_r.args.get("group").expressions}
        s_group = {g.sql().lower() for g in (ast_s.args.get("group").expressions if ast_s and ast_s.args.get("group") else [])}
        missing = [c for c in r_group if c not in s_group]
        if missing:
            return True, {"missing_group_by": missing}
    return False, {}
register_constraint(Constraint(next_id(), "group_by_missing_columns", 6, check_group_by_missing_cols,
                               "GROUP BY is missing required columns.",
                               "Include all non-aggregated SELECT columns in GROUP BY to define correct grouping."))

def check_having_without_aggregate(ctx):
    ast = ctx_student_ast(ctx)
    if ast and ast.args.get("having") and not any(getattr(f,"name","").upper() in {"SUM","COUNT","AVG","MIN","MAX"} for f in ast.find_all(Func)):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "having_without_aggregate", 35, check_having_without_aggregate,
                               "HAVING clause used without aggregate functions.",
                               "Use HAVING to filter aggregated results, or move non-aggregate filters to WHERE."))

def check_missing_aggregation_alias(ctx):
    ast_r = ctx_ref_ast(ctx)
    if ast_r:
        sel_r = ast_r.find(Select)
        sel_s = ctx_student_ast(ctx).find(Select) if ctx_student_ast(ctx) else None
        if sel_r and sel_s:
            ref_aliases = [e.alias for e in sel_r.expressions if getattr(e,"alias",None)]
            if ref_aliases and not any(getattr(e,"alias",None) for e in sel_s.expressions):
                return True, {"expected_aliases": ref_aliases}
    return False, {}
register_constraint(Constraint(next_id(), "aggregation_alias_missing", 100, check_missing_aggregation_alias,
                               "Consider aliasing your aggregated expressions.",
                               "Use AS to name aggregate columns (e.g., COUNT(*) AS total_count)."))

# 4. WHERE / Predicates / Boolean logic
def check_missing_where(ctx):
    if "where" in ctx_ref_sql(ctx) and "where" not in ctx_student_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "missing_where", 8, check_missing_where,
                               "A WHERE clause is required but missing.",
                               "Add WHERE to filter rows before grouping or aggregation."))

def check_extra_where(ctx):
    if "where" in ctx_student_sql(ctx) and "where" not in ctx_ref_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "extra_where", 65, check_extra_where,
                               "Your query has an extra WHERE clause.",
                               "Remove unnecessary filtering that may exclude required rows."))

def check_tautological_predicate(ctx):
    s = ctx_student_sql(ctx)
    if re.search(r"\b1\s*=\s*1\b|\btrue\s*=\s*true\b", s):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "tautology_predicate", 90, check_tautological_predicate,
                               "Tautological predicate detected (always true).",
                               "Remove conditions like 1=1 that don't actually filter any rows."))

def check_contradictory_predicate(ctx):
    s = ctx_student_sql(ctx)
    if re.search(r"\b1\s*=\s*0\b|\btrue\s*=\s*false\b", s):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "contradictory_predicate", 20, check_contradictory_predicate,
                               "Contradictory predicate detected (always false).",
                               "Remove conditions that are always false and prevent any rows from being returned."))

def check_aggregate_in_where(ctx):
    s = ctx_student_sql(ctx)
    if re.search(r"where\b.*\b(sum|count|avg|min|max)\s*\(", s, re.IGNORECASE):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "aggregate_in_where", 9, check_aggregate_in_where,
                               "Aggregate functions found in WHERE clause.",
                               "Move aggregate conditions from WHERE to HAVING (used after GROUP BY)."))

def check_where_differs(ctx):
    try:
        ast_r = ctx_ref_ast(ctx); ast_s = ctx_student_ast(ctx)
        wr = str(ast_r.args.get("where")) if ast_r and ast_r.args.get("where") else ""
        ws = str(ast_s.args.get("where")) if ast_s and ast_s.args.get("where") else ""
        if wr and ws and wr.strip().lower() != ws.strip().lower():
            return True, {"ref_where": wr, "stu_where": ws}
    except Exception:
        pass
    return False, {}
register_constraint(Constraint(next_id(), "where_differs", 12, check_where_differs,
                               "Your WHERE clause logic differs from expected.",
                               "Review each condition, operator, and logical connector (AND/OR) carefully."))

# 5. Subqueries / CTEs / Nesting
def check_missing_subquery(ctx):
    if ctx.get("metadata",{}).get("reference_subqueries",0) > ctx.get("metadata",{}).get("student_subqueries",0):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "missing_subquery", 15, check_missing_subquery,
                               "This problem requires a subquery or nested query.",
                               "Use a subquery to compute intermediate results before the final aggregation."))

def check_cte_missing(ctx):
    if "with " in ctx_ref_sql(ctx) and "with " not in ctx_student_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "cte_expected", 25, check_cte_missing,
                               "Consider using a CTE (Common Table Expression).",
                               "Use WITH to define named subqueries that simplify complex multi-step logic."))

def check_window_expected_but_missing(ctx):
    if "over(" in ctx_ref_sql(ctx) and "over(" not in ctx_student_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "window_expected", 17, check_window_expected_but_missing,
                               "A window function may be required for this problem.",
                               "Use window functions with OVER() for rankings or running calculations across rows."))

# 6. Execution / semantic constraints (result-based)
def check_execution_error(ctx):
    es = ctx_exec_student(ctx)
    er = ctx_exec_ref(ctx)
    if es and not es.get("success"):
        return True, {"student_error": es.get("error")}
    if er and not er.get("success"):
        return True, {"reference_error": er.get("error")}
    return False, {}
register_constraint(Constraint(next_id(), "execution_error", 1, check_execution_error,
                               "Query execution failed with an error.",
                               "Fix syntax errors, check table/column names, and verify function usage."))

def check_student_returns_no_rows(ctx):
    es = ctx_exec_student(ctx); er = ctx_exec_ref(ctx)
    if er and er.get("success") and es and es.get("success"):
        if len(es.get("rows",[])) == 0 and len(er.get("rows",[])) > 0:
            return True, {"student_rows":0, "reference_rows": len(er.get("rows",[]))}
    return False, {}
register_constraint(Constraint(next_id(), "student_no_rows", 11, check_student_returns_no_rows,
                               "Your query returns zero rows but should return results.",
                               "Check WHERE conditions and join types—you may be over-filtering."))

def check_student_more_rows(ctx):
    es = ctx_exec_student(ctx); er = ctx_exec_ref(ctx)
    if er and er.get("success") and es and es.get("success"):
        if len(es.get("rows",[])) > len(er.get("rows",[])):
            return True, {"student_rows": len(es.get("rows",[])), "reference_rows": len(er.get("rows",[]))}
    return False, {}
register_constraint(Constraint(next_id(), "student_more_rows", 14, check_student_more_rows,
                               "Your query returns too many rows.",
                               "Add missing filters or fix join conditions to reduce duplicate rows."))

def check_aggregation_value_mismatch(ctx):
    es = ctx_exec_student(ctx); er = ctx_exec_ref(ctx)
    if er and er.get("success") and es and es.get("success"):
        try:
            if len(er.get("rows",[])) == 1 and len(es.get("rows",[])) == 1 and er["rows"][0] != es["rows"][0]:
                return True, {"ref": er["rows"][0], "stu": es["rows"][0]}
        except Exception:
            pass
    return False, {}
register_constraint(Constraint(next_id(), "aggregate_value_mismatch", 13, check_aggregation_value_mismatch,
                               "Aggregate calculation result is incorrect.",
                               "Verify which rows are included in your aggregate and check GROUP BY logic."))

def check_ordering_difference(ctx):
    es = ctx_exec_student(ctx); er = ctx_exec_ref(ctx)
    if er and er.get("success") and es and es.get("success"):
        if sorted(er.get("rows",[])) == sorted(es.get("rows",[])) and er.get("rows",[]) != es.get("rows",[]):
            return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "ordering_difference", 52, check_ordering_difference,
                               "Results are correct but ordering is wrong.",
                               "Add ORDER BY with the correct columns and sort direction (ASC/DESC)."))

# 7. Style / dialect / functions / misc constraints
def check_nonstandard_functions(ctx):
    s = ctx_student_sql(ctx)
    if re.search(r"regexp|str_to_date|to_char\(|date_part\(|date_trunc\(", s):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "nonstandard_function", 85, check_nonstandard_functions,
                               "Your query uses dialect-specific functions.",
                               "Verify these functions are supported by the target database system."))

def check_quoted_identifiers(ctx):
    s = ctx_student_sql(ctx)
    if '"' in s or '`' in s:
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "quoted_identifiers", 93, check_quoted_identifiers,
                               "Quoted identifiers detected in your query.",
                               "Avoid quotes around table/column names unless necessary for case-sensitivity."))

def check_distinct_mismatch(ctx):
    if ("distinct" in ctx_student_sql(ctx)) != ("distinct" in ctx_ref_sql(ctx)):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "distinct_mismatch", 55, check_distinct_mismatch,
                               "DISTINCT usage differs from expected solution.",
                               "Add or remove DISTINCT based on whether duplicate rows should be eliminated."))

def check_union_unexpected(ctx):
    if "union" in ctx_student_sql(ctx) and "union" not in ctx_ref_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "unexpected_union", 84, check_union_unexpected,
                               "UNION detected but may not be needed.",
                               "Consider if JOINs would be more appropriate for combining related data."))

def check_json_ops(ctx):
    s = ctx_student_sql(ctx)
    if "->" in s or "json" in s:
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "json_ops", 88, check_json_ops,
                               "JSON/array operators detected in query.",
                               "Verify that JSON operations are supported by your database system."))

def check_case_when_incomplete(ctx):
    s = ctx_student_sql(ctx)
    if "case when" in s and "end" not in s:
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "case_when_incomplete", 95, check_case_when_incomplete,
                               "CASE WHEN expression is incomplete.",
                               "Ensure each CASE statement has matching END and proper WHEN...THEN structure."))

# Additional constraints
def check_alias_conflict(ctx):
    s = ctx_student_sql(ctx)
    aliases = re.findall(r"\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)", s, flags=re.IGNORECASE)
    for a in set(aliases):
        if aliases.count(a) > 1:
            return True, {"alias": a}
    return False, {}
register_constraint(Constraint(next_id(), "alias_conflict", 22, check_alias_conflict,
                               "Duplicate alias name detected.",
                               "Use unique alias names to avoid ambiguous column references."))

def check_like_usage(ctx):
    if "like" in ctx_student_sql(ctx) and "like" not in ctx_ref_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "like_usage", 70, check_like_usage,
                               "LIKE pattern matching differs from expected.",
                               "Verify your pattern is correct and consider case sensitivity issues."))

def check_limit_missing_when_expected(ctx):
    if "limit" in ctx_ref_sql(ctx) and "limit" not in ctx_student_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "limit_missing", 57, check_limit_missing_when_expected,
                               "LIMIT clause is missing from your query.",
                               "Add LIMIT with ORDER BY to restrict results to the top N rows."))

def check_null_handling(ctx):
    s = ctx_student_sql(ctx)
    if "is null" in s or "is not null" in s:
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "null_handling", 21, check_null_handling,
                               "NULL comparison detected in your query.",
                               "Use IS NULL or IS NOT NULL—regular equality operators don't work with NULL."))

def check_literal_string_number_mismatch(ctx):
    s = ctx_student_sql(ctx)
    if re.search(r"=\s*'\d+'", s):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "literal_vs_number", 32, check_literal_string_number_mismatch,
                               "Comparing numeric values as strings detected.",
                               "Remove quotes around numbers or use explicit CAST for type conversion."))

def check_complex_where(ctx):
    s = ctx_student_sql(ctx)
    if len(s) > 300 and "where" in s:
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "complex_where", 92, check_complex_where,
                               "WHERE clause is very complex.",
                               "Break complex logic into CTEs or subqueries for easier debugging."))

def check_order_by_missing(ctx):
    if "order by" in ctx_ref_sql(ctx) and "order by" not in ctx_student_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "order_by_missing", 58, check_order_by_missing,
                               "ORDER BY clause is missing.",
                               "Add ORDER BY to sort results in the expected sequence."))

def check_window_usage_mismatch(ctx):
    if "over(" in ctx_ref_sql(ctx) and "over(" in ctx_student_sql(ctx):
        return False, {}
    if "over(" in ctx_ref_sql(ctx) and "over(" not in ctx_student_sql(ctx):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "window_mismatch", 17, check_window_usage_mismatch,
                               "Window function expected but not found.",
                               "Use OVER() clause for calculations like ROW_NUMBER, RANK, or running totals."))

def check_function_misuse(ctx):
    s = ctx_student_sql(ctx)
    if re.search(r"count\s*\(\s*\*\s*\)\s+over", s, flags=re.IGNORECASE):
        return True, {}
    return False, {}
register_constraint(Constraint(next_id(), "function_misuse", 76, check_function_misuse,
                               "Potential function misuse detected.",
                               "Verify aggregate and window functions are used in appropriate clauses."))

def check_cartesian_product(ctx):
    ast = ctx_student_ast(ctx)
    if ast:
        tables = collect_tables(ast)
        joins = list(ast.find_all(Join))
        if len(tables) > 1 and not joins and "where" not in ctx_student_sql(ctx):
            return True, {"tables": tables}
    return False, {}
register_constraint(Constraint(next_id(), "cartesian_product", 18, check_cartesian_product,
                               "Possible Cartesian product detected.",
                               "Add join conditions to match rows correctly and avoid unnecessary combinations."))

def check_unused_table(ctx):
    return False, {}
register_constraint(Constraint(next_id(), "unused_table", 90, check_unused_table,
                               "Check for unused tables in your query.",
                               "Remove tables that don't contribute columns or join conditions."))

# Reorder constraints by priority ascending
CONSTRAINTS = sorted(CONSTRAINTS, key=lambda c: c.priority)

# -----------------------------
# Hint generation
# -----------------------------
def format_hint(constraint: Constraint, level: int, evidence: Dict[str,Any]) -> str:
    if level == 1:
        return constraint.hint_l1
    elif level == 2:
        return constraint.hint_l2
    else:
        # safe LLM conceptual stub-level message
        return llm_conceptual_hint_stub(constraint.name)

# Level 4 LLM stub (safe). IMPORTANT: Replace with real LLM call but NEVER pass SQL or schema.
def llm_conceptual_hint_stub(category_label: str) -> str:
    # Short conceptual statements per category; fallback generic guidance
    mapping = {
        "missing_table": "Level 4 concept: Consider where required attributes come from; a missing relation will prevent access to its columns.",
        "missing_join_condition": "Level 4 concept: Join keys pair related rows; missing or wrong keys lead to Cartesian products or missing matches.",
        "aggregate_without_groupby": "Level 4 concept: When aggregating, non-aggregated select columns must appear in GROUP BY.",
        "group_by_missing_columns": "Level 4 concept: GROUP BY defines grouping keys; missing keys change aggregation buckets.",
        "execution_error": "Level 4 concept: Fix syntax/runtime errors first; they indicate structural issues or unsupported functions.",
        "student_no_rows": "Level 4 concept: Zero rows usually means over-filtering or incorrect join types; test parts of the query to isolate the predicate.",
        # default fallback
    }
    return mapping.get(category_label, f"Level 4 concept: The category '{category_label}' refers to a conceptual area. Review the relevant SQL concept (joins, aggregation, predicates) and avoid requesting direct SQL corrections.")

def build_semantic_explanation(signals):
    if "ordering_difference" in signals:
        return (
            "The values match the expected output, but their order differs. "
            "Check whether explicit ordering is required."
        )

    if "row_count_mismatch" in signals:
        return (
            "The number of returned results does not match the expected output. "
            "This often indicates missing filters, joins, or grouping logic."
        )

    if "aggregation_or_grouping_issue" in signals:
        return (
            "The output size suggests that rows may be grouped or aggregated incorrectly. "
            "Review how records are combined."
        )

    if "null_handling_difference" in signals:
        return (
            "The output differs in how missing values are handled. "
            "Check how NULL values are treated in conditions or expressions."
        )

    return (
        "The output differs from the expected result. "
        "Review the logic that determines which values are produced."
    )


# -----------------------------
# Public single-call function
# -----------------------------
def get_sql_hint(student_sql: str, reference_sql: str, hint_level: int = 1, setup_sql: Optional[str] = None, dialect: str = "mysql") -> Dict[str, Any]:
    """
    Main function: compares student_sql and reference_sql and returns a dict with diagnostics and a single hint.
    hint_level: 1 (high-level) | 2 (targeted) | 3 (conceptual) | 4 (LLM conceptual - safe stub included)
    """
    out: Dict[str, Any] = {
        "normalized_student": None,
        "normalized_reference": None,
        "parse_error_student": None,
        "parse_error_reference": None,
        "ast_diffs": [],
        "execution": {"student": None, "reference": None, "equal": None, "error": None},
        "hint": {"level": hint_level, "text": None, "constraint_id": None, "constraint_name": None, "evidence": None},
    }

    # 1) Canonicalize
    can_s, err_s = canonicalize(student_sql, dialect=dialect)
    can_r, err_r = canonicalize(reference_sql, dialect=dialect)
    out["normalized_student"] = can_s
    out["normalized_reference"] = can_r
    out["parse_error_student"] = err_s
    out["parse_error_reference"] = err_r

    # 2) AST diff (metadata)
    ad = ast_diff(student_sql, reference_sql, dialect=dialect)
    out["ast_diffs"] = ad.structural_diffs
    # copy parse errors/normalized inside ad (already present)
    if ad.normalized_student:
        out["normalized_student"] = ad.normalized_student
    if ad.normalized_reference:
        out["normalized_reference"] = ad.normalized_reference
    if ad.parse_error_student:
        out["parse_error_student"] = ad.parse_error_student
    if ad.parse_error_reference:
        out["parse_error_reference"] = ad.parse_error_reference

    # attach metadata for constraints
    metadata = ad.metadata
    context = {
        "student_sql": student_sql,
        "reference_sql": reference_sql,
        "student_ast": None,
        "ref_ast": None,
        "parse_error_student": out["parse_error_student"],
        "parse_error_reference": out["parse_error_reference"],
        "metadata": metadata
    }
    # parse ASTs safely
    try:
        context["student_ast"] = parse_one(out["normalized_student"], read="mysql", error_level="raise") if out["normalized_student"] else None
    except Exception:
        context["student_ast"] = None
    try:
        context["ref_ast"] = parse_one(out["normalized_reference"], read="mysql", error_level="raise") if out["normalized_reference"] else None
    except Exception:
        context["ref_ast"] = None

    # 3) Execution-based verification (if setup provided or attempt with no setup)
    exec_student = execute_in_memory(setup_sql, student_sql)
    exec_reference = execute_in_memory(setup_sql, reference_sql)
    out["execution"]["student"] = exec_student
    out["execution"]["reference"] = exec_reference
    equal, exec_err = compare_results(exec_student, exec_reference)
    out["execution"]["equal"] = equal
    out["execution"]["error"] = exec_err

    context["exec_student"] = exec_student
    context["exec_ref"] = exec_reference
    context["parse_error_student"] = out["parse_error_student"]
    context["parse_error_reference"] = out["parse_error_reference"]
    semantic_result = semantic_diff(context)

        # --- EARLY EXIT: If outputs match, the solution is correct ---
    if exec_student.get("success") and exec_reference.get("success"):
        # Exact match of rows & columns = semantically correct
        if equal:
            out["hint"]["text"] = "Your query is correct. It produces the expected output."
            out["hint"]["constraint_id"] = None
            out["hint"]["constraint_name"] = None
            out["hint"]["evidence"] = {}
            return out


    # 4) Run constraints in priority order and find first applicable one
    matched_constraint: Optional[Constraint] = None
    matched_evidence: Dict[str,Any] = {}
    for c in CONSTRAINTS:
        try:
            flag, evidence = c.checker(context)
        except Exception as e:
            flag, evidence = False, {"error": "Constraint checker exception: " + str(e) + "\n" + traceback.format_exc()}
        if flag:
            matched_constraint = c
            matched_evidence = evidence or {}
            break

    # 5) If none matched but execution shows inequality, add generic mismatch
    if not matched_constraint:
        if exec_student.get("success") and exec_reference.get("success") and not equal:
            # generic difference
            generic = Constraint(999, "semantic_mismatch", 500, lambda ctx: (True, {} if True else {}), 
                                 "Your query output differs from expected output.",
                                 "Row counts or values differ — consider joins/filters/grouping/aggregates.")
            matched_constraint = generic
            matched_evidence = {}

    if hint_level == 3 and not semantic_result["equal"]:
        # add semantic explanation for level 3
        out["hint"]["text"] = build_semantic_explanation(semantic_result["signals"])
        return out

    # 6) Build hint from matched constraint
    if matched_constraint:
        out["hint"]["constraint_id"] = matched_constraint.id
        out["hint"]["constraint_name"] = matched_constraint.name
        out["hint"]["evidence"] = matched_evidence
        # cap level to 4
        lvl = hint_level if hint_level in (1,2,3,4,5) else 1
        if lvl == 4 or lvl == 5:
            # safe LLM conceptual hint (stub)
            out["hint"]["text"] = llm_generate_safe_hint(
                ast_diffs=out.get("ast_diffs", []),
                metadata=metadata,
                exec_student=exec_student,
                exec_ref=exec_reference,
                matched_constraint_name=matched_constraint.name if matched_constraint else "none",
                matched_evidence=matched_evidence,
                level=lvl,
                context=context
            )
        else:
            out["hint"]["text"] = format_hint(matched_constraint, lvl, matched_evidence)
    else:
        out["hint"]["text"] = "No hints applicable; your query may already match or require deeper manual analysis."

    return out

app = FastAPI()

origins = [
    "http://localhost:5173",
    "*",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # allow methods
    allow_headers=["*"], # allow headers
)


QUESTIONS_FILE = Path("questions.json")
SETUP_FILE = Path("sample_setup.sql")

with open(QUESTIONS_FILE, "r") as f:
    QUESTION_BANK = json.load(f)

with open(SETUP_FILE, "r") as f:
    GLOBAL_SETUP_SQL = f.read()



class ValidateRequest(BaseModel):
    student_sql: str
    question_number: int

class HintRequest(BaseModel):
    student_sql: str
    question_number: int
    hint_level: int


# ----------------------------
# Helpers
# ----------------------------
def get_question_or_404(qid: int):
    for q in QUESTION_BANK:
        if q["id"] == qid:
            return q
    raise HTTPException(status_code=404, detail="Invalid question number")

@app.get("/")
def read_root():
    return {"message": "SQL Hinting Service is running."}


# ----------------------------
# API 1 — Validate SQL Output
# ----------------------------
@app.post("/validate")
def validate_sql(req: ValidateRequest):

    question = get_question_or_404(req.question_number)
    ref_sql = question["answer_ref"]

    # Isolated execution via DuckDB
    exec_student = execute_in_memory(GLOBAL_SETUP_SQL, req.student_sql)
    exec_ref = execute_in_memory(GLOBAL_SETUP_SQL, ref_sql)

    equal, err = compare_results(exec_student, exec_ref)

    return {
        "success": bool(equal),
        "error": err,
        "student_output": exec_student,
        "reference_output": exec_ref
    }


# ----------------------------
# API 2 — Validate + Hint
# ----------------------------
@app.post("/hint")
def get_hint_api(req: HintRequest):

    question = get_question_or_404(req.question_number)
    ref_sql = question["answer_ref"]

    exec_student = execute_in_memory(GLOBAL_SETUP_SQL, req.student_sql)
    exec_ref = execute_in_memory(GLOBAL_SETUP_SQL, ref_sql)
    equal, err = compare_results(exec_student, exec_ref)

    hint = get_sql_hint(
        student_sql=req.student_sql,
        reference_sql=ref_sql,
        hint_level=req.hint_level,
        setup_sql=GLOBAL_SETUP_SQL
    )

    return {
        "success": bool(equal),
        "error": err,
        "hint": hint["hint"]["text"],
        "constraint_id": hint["hint"].get("constraint_id"),
        "constraint_name": hint["hint"].get("constraint_name"),
        "execution": {
            "student": exec_student,
            "reference": exec_ref,
            "equal": equal,
            "error": err
        }
    }

