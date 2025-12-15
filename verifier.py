# verifier.py
import duckdb
import json
from typing import Dict, Any, List, Optional
from canonicalizer import canonicalize

class ExecutionResult:
    def __init__(self):
        self.success = False
        self.error = None
        self.rows = []
        self.columns = []

def _execute_query_in_memory(con: duckdb.DuckDBPyConnection, sql: str) -> ExecutionResult:
    r = ExecutionResult()
    try:
        # duckdb executes and returns result as list of tuples + column names
        res = con.execute(sql).fetchall()
        cols = [c[0] for c in con.description] if con.description else []
        r.success = True
        r.rows = res
        r.columns = cols
    except Exception as e:
        r.error = str(e)
    return r

def compare_query_results(student_sql: str, reference_sql: str, setup_sql: str = "") -> Dict[str, Any]:
    """
    Run both queries against an in-memory DuckDB instance.
    - setup_sql: DDL + INSERTs to create sample schema and data.
    Returns dict with execution info and semantic equality result.
    """
    con = duckdb.connect(database=':memory:')
    out = {"student": None, "reference": None, "equal": False, "error": None}
    try:
        if setup_sql:
            con.execute(setup_sql)

        # canonicalize queries for stable execution; duckdb supports standard SQL
        s_can = canonicalize(student_sql) or student_sql
        r_can = canonicalize(reference_sql) or reference_sql

        s_res = _execute_query_in_memory(con, s_can)
        r_res = _execute_query_in_memory(con, r_can)

        out["student"] = {"success": s_res.success, "error": s_res.error, "rows": s_res.rows, "cols": s_res.columns}
        out["reference"] = {"success": r_res.success, "error": r_res.error, "rows": r_res.rows, "cols": r_res.columns}

        if not s_res.success or not r_res.success:
            out["error"] = "Execution failed for one or both queries."
            return out

        # Compare result sets semantically: treat them as multisets of rows
        # Normalize rows: convert to JSON-serializable and sort
        def normalize_rows(rows):
            # convert types to str for stable comparison (or use tuple of values)
            return sorted([tuple(row) for row in rows])

        out["equal"] = normalize_rows(s_res.rows) == normalize_rows(r_res.rows)
        return out
    finally:
        con.close()
