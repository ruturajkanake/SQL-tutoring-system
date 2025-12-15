# cbm_constraints.py

from dataclasses import dataclass
from typing import Callable, Dict, Any, Optional, List
from sqlglot import exp

# ============================
# Constraint Definition
# ============================

@dataclass
class Constraint:
    id: int
    name: str
    priority: int
    checker: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]
    short_hint: str
    long_hint: str


# ============================
# AST Helpers (Deep)
# ============================

def find_all(ast, node_type):
    return list(ast.find_all(node_type)) if ast else []

def has(ast, node_type):
    return any(True for _ in ast.find_all(node_type)) if ast else False

def select_exprs(ast):
    sel = ast.find(exp.Select)
    return sel.expressions if sel else []

def where_preds(ast):
    w = ast.find(exp.Where)
    return list(w.find_all(exp.Expression)) if w else []

def joins(ast):
    return list(ast.find_all(exp.Join))

def group_cols(ast):
    g = ast.find(exp.Group)
    return [c.sql() for c in g.expressions] if g else []

def aggs(ast):
    return list(ast.find_all(exp.AggFunc))


# ============================
# Constraint Detectors (25)
# ============================

def missing_where(ctx):
    return {} if ctx["ref_ast"].find(exp.Where) and not ctx["student_ast"].find(exp.Where) else None

def extra_where(ctx):
    return {} if ctx["student_ast"].find(exp.Where) and not ctx["ref_ast"].find(exp.Where) else None

def between_mismatch(ctx):
    return {} if has(ctx["student_ast"], exp.Between) != has(ctx["ref_ast"], exp.Between) else None

def and_or_mix(ctx):
    w = ctx["student_ast"].find(exp.Where)
    return {} if w and has(w, exp.And) and has(w, exp.Or) else None

def contradictory_filters(ctx):
    seen = {}
    for p in where_preds(ctx["student_ast"]):
        if isinstance(p, exp.EQ) and isinstance(p.left, exp.Column):
            col = p.left.sql()
            val = p.right.sql()
            if col in seen and seen[col] != val:
                return {"column": col}
            seen[col] = val
    return None

def missing_join(ctx):
    return {} if joins(ctx["ref_ast"]) and not joins(ctx["student_ast"]) else None

def join_type_mismatch(ctx):
    for s, r in zip(joins(ctx["student_ast"]), joins(ctx["ref_ast"])):
        if s.kind != r.kind:
            return {}
    return None

def join_without_on(ctx):
    return {} if any(j.args.get("on") is None for j in joins(ctx["student_ast"])) else None

def cartesian_join(ctx):
    f = ctx["student_ast"].find(exp.From)
    return {} if f and len(f.expressions) > 1 and not joins(ctx["student_ast"]) else None

def self_join_no_alias(ctx):
    tables = [t.sql() for t in ctx["student_ast"].find_all(exp.Table)]
    return {} if len(tables) != len(set(tables)) else None

def missing_group(ctx):
    return {} if ctx["ref_ast"].find(exp.Group) and not ctx["student_ast"].find(exp.Group) else None

def extra_group(ctx):
    return {} if ctx["student_ast"].find(exp.Group) and not ctx["ref_ast"].find(exp.Group) else None

def non_grouped_column(ctx):
    gcols = set(group_cols(ctx["student_ast"]))
    for e in select_exprs(ctx["student_ast"]):
        if isinstance(e, exp.Column) and e.sql() not in gcols:
            return {"column": e.sql()}
    return None

def agg_function_mismatch(ctx):
    return {} if {type(a) for a in aggs(ctx["student_ast"])} != {type(a) for a in aggs(ctx["ref_ast"])} else None

def count_star_mismatch(ctx):
    for a in aggs(ctx["student_ast"]):
        if isinstance(a, exp.Count) and not isinstance(a.this, exp.Star):
            return {}
    return None

def having_without_group(ctx):
    return {} if ctx["student_ast"].find(exp.Having) and not ctx["student_ast"].find(exp.Group) else None

def distinct_mismatch(ctx):
    return {} if ctx["student_ast"].find(exp.Distinct) != ctx["ref_ast"].find(exp.Distinct) else None

def projection_count(ctx):
    return {} if len(select_exprs(ctx["student_ast"])) != len(select_exprs(ctx["ref_ast"])) else None

def expression_type_mismatch(ctx):
    for s, r in zip(select_exprs(ctx["student_ast"]), select_exprs(ctx["ref_ast"])):
        if type(s) != type(r):
            return {}
    return None

def alias_mismatch(ctx):
    for s, r in zip(select_exprs(ctx["student_ast"]), select_exprs(ctx["ref_ast"])):
        if s.alias != r.alias:
            return {}
    return None

def star_vs_explicit(ctx):
    return {} if has(ctx["student_ast"], exp.Star) != has(ctx["ref_ast"], exp.Star) else None

def null_comparison(ctx):
    for p in where_preds(ctx["student_ast"]):
        if isinstance(p, exp.EQ) and isinstance(p.right, exp.Null):
            return {}
    return None

def operator_mismatch(ctx):
    return {} if {type(p) for p in where_preds(ctx["student_ast"])} != {type(p) for p in where_preds(ctx["ref_ast"])} else None

def limit_mismatch(ctx):
    return {} if ctx["student_ast"].find(exp.Limit) != ctx["ref_ast"].find(exp.Limit) else None


# ============================
# Constraint Registry
# ============================

CONSTRAINTS: List[Constraint] = [
    Constraint(1,"missing_where",10,missing_where,
        "Your query is missing a filter.",
        "The expected solution restricts rows using a WHERE clause. Review which records should be included."),
    Constraint(2,"extra_where",10,extra_where,
        "Your query applies an unnecessary filter.",
        "The reference solution does not apply filtering here. Check whether the WHERE condition is required."),
    Constraint(3,"between_bounds",15,between_mismatch,
        "Range logic differs.",
        "The reference solution handles boundary conditions differently. Ensure your comparisons match."),
    Constraint(4,"and_or",18,and_or_mix,
        "Logical condition grouping may be incorrect.",
        "Mixing AND and OR without parentheses can change evaluation order."),
    Constraint(5,"contradiction",20,contradictory_filters,
        "Your filters contradict each other.",
        "Multiple conditions on the same column conflict and eliminate all rows."),
    Constraint(6,"missing_join",25,missing_join,
        "A required join is missing.",
        "The expected solution combines multiple tables using JOIN."),
    Constraint(7,"join_type",28,join_type_mismatch,
        "The join type differs.",
        "Different join types change which unmatched rows are included."),
    Constraint(8,"join_on",30,join_without_on,
        "A join condition is missing.",
        "Each JOIN must specify how rows are matched using an ON clause."),
    Constraint(9,"cartesian",32,cartesian_join,
        "Your query produces a Cartesian product.",
        "Tables are combined without a join condition, resulting in excessive rows."),
    Constraint(10,"self_join_alias",34,self_join_no_alias,
        "Self-join lacks proper aliasing.",
        "When joining a table to itself, distinct aliases are required."),
    Constraint(11,"missing_group",36,missing_group,
        "Grouping is missing.",
        "Aggregations with non-aggregated columns require GROUP BY."),
    Constraint(12,"extra_group",36,extra_group,
        "Unnecessary grouping detected.",
        "The reference solution does not group results."),
    Constraint(13,"non_grouped",38,non_grouped_column,
        "A selected column is not grouped.",
        "All non-aggregated columns must appear in GROUP BY."),
    Constraint(14,"agg_func",40,agg_function_mismatch,
        "Aggregation function differs.",
        "Ensure the correct aggregation (COUNT, SUM, etc.) is used."),
    Constraint(15,"count_star",42,count_star_mismatch,
        "COUNT usage differs.",
        "COUNT(*) and COUNT(column) behave differently with NULLs."),
    Constraint(16,"having",44,having_without_group,
        "HAVING is used incorrectly.",
        "HAVING filters groups and requires GROUP BY."),
    Constraint(17,"distinct",46,distinct_mismatch,
        "Duplicate handling differs.",
        "DISTINCT changes whether duplicate rows are removed."),
    Constraint(18,"projection",48,projection_count,
        "The number of selected expressions differs.",
        "The expected output contains a different number of columns."),
    Constraint(19,"expr_type",50,expression_type_mismatch,
        "Selected expressions differ in form.",
        "The reference solution uses a different expression structure."),
    Constraint(20,"alias",52,alias_mismatch,
        "Column aliases differ.",
        "Aliases affect column names in the output."),
    Constraint(21,"star",54,star_vs_explicit,
        "Column selection differs.",
        "Selecting all columns (*) differs from selecting specific columns."),
    Constraint(22,"null",56,null_comparison,
        "NULL is compared incorrectly.",
        "NULL must be tested using IS NULL or IS NOT NULL."),
    Constraint(23,"operator",58,operator_mismatch,
        "Comparison operators differ.",
        "Different operators can change which rows match."),
    Constraint(24,"limit",60,limit_mismatch,
        "Row limiting differs.",
        "LIMIT affects how many rows are returned."),
]
