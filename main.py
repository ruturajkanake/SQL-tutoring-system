#!/usr/bin/env python3
"""
sql_hint_tool.py

Single-file tool that:
- canonicalizes & normalizes two SQL queries (sqlglot)
- computes AST structural diffs (columns, tables, joins, groupby, subqueries)
- executes both queries on an in-memory DuckDB instance against a small setup dataset
- compares results semantically (multiset equality)
- produces tiered hints (Level 1 general -> Level 3 conceptual) suitable for a tutoring system

Usage:
    python sql_hint_tool.py --student "SELECT ..." --reference "SELECT ..."
    python sql_hint_tool.py --student-file student.sql --reference-file ref.sql --setup sample_setup.sql
"""

import argparse
import json
import sys
from typing import List, Optional, Tuple, Dict, Any

try:
    import sqlglot
    from sqlglot import parse_one
    from sqlglot.expressions import Column, Table, Join, Subquery, Select
    from sqlglot.errors import ParseError
except Exception as e:
    print("Please install sqlglot: pip install sqlglot", file=sys.stderr)
    raise

try:
    import duckdb
except Exception as e:
    print("Please install duckdb: pip install duckdb", file=sys.stderr)
    raise


DEFAULT_SETUP_SQL = """
CREATE TABLE province_names
(
    province_id   char(2) PRIMARY KEY,
    province_name text
);

INSERT INTO province_names (province_id, province_name)
VALUES ('SC', 'Spartanburg');
INSERT INTO province_names (province_id, province_name)
VALUES ('FL', 'Fort Lauderdale');
INSERT INTO province_names (province_id, province_name)
VALUES ('CO', 'Fort Collins');
INSERT INTO province_names (province_id, province_name)
VALUES ('OH', 'Cleveland');
INSERT INTO province_names (province_id, province_name)
VALUES ('DC', 'Washington');
INSERT INTO province_names (province_id, province_name)
VALUES ('IA', 'Des Moines');
INSERT INTO province_names (province_id, province_name)
VALUES ('AK', 'Juneau');

INSERT INTO province_names (province_id, province_name)
VALUES ('WI', 'Madison');
INSERT INTO province_names (province_id, province_name)
VALUES ('LA', 'Baton Rouge');
INSERT INTO province_names (province_id, province_name)
VALUES ('DE', 'Wilmington');


INSERT INTO province_names (province_id, province_name)
VALUES ('IL', 'Chicago');
INSERT INTO province_names (province_id, province_name)
VALUES ('CA', 'Whittier');
INSERT INTO province_names (province_id, province_name)
VALUES ('MO', 'Columbia');

INSERT INTO province_names (province_id, province_name)
VALUES ('MD', 'Laurel');
INSERT INTO province_names (province_id, province_name)
VALUES ('MI', 'Kalamazoo');
INSERT INTO province_names (province_id, province_name)
VALUES ('TN', 'Nashville');
INSERT INTO province_names (province_id, province_name)
VALUES ('TX', 'Fort Worth');

INSERT INTO province_names (province_id, province_name)
VALUES ('NY', 'Albany');
INSERT INTO province_names (province_id, province_name)
VALUES ('VA', 'Virginia Beach');
INSERT INTO province_names (province_id, province_name)
VALUES ('PA', 'Reading');
INSERT INTO province_names (province_id, province_name)
VALUES ('IN', 'Evansville');
INSERT INTO province_names (province_id, province_name)
VALUES ('MN', 'Minneapolis');



CREATE TABLE patients
(
    patient_id  integer PRIMARY KEY,
    first_name  text,
    last_name   text,
    gender      varchar(1),
    birth_date  DATE,
    city        text,
    allergies   text,
    height      integer,
    weight      integer,
    province_id char(2) REFERENCES province_names (province_id)
);
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (1, 'Thomasina', 'Galiero', 'F', '1987-10-23', 'Taihe Chengguanzhen', 'Rabbit Hair', 42, 23, 'TX');
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (2, 'Misha', 'Learmonth', 'F', '1993-11-30', 'Bagay', 'Treatment Set TS128811', 11, 164, 'TX');
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (3, 'Hasheem', 'Karpenya', 'M', '1979-06-14', 'Sijiqing', 'PREDNISONE', 94, 250, 'TX');
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (4, 'Sibby', 'Burril', 'F', '1993-08-13', 'Bograd', 'potassium chloride', 26, 175, 'CA');
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (5, 'Deidre', 'Messier', 'F', '2010-07-18', 'Tây Hồ', 'bupropion hydrochloride', 159, 243, 'TX');
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (6, 'Seth', 'Bachura', 'M', '1989-02-07', 'Baiyun', 'Aluminum Zirconium Tetrachlorohydrex GLY', 232, 61, 'CA');
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (7, 'Nonna', 'Breston', 'F', '1979-05-08', 'Pakuranga', 'Dextromethorphan HBr, Phenylephrine HCl', 214, 83,
        'MI');
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (8, 'Thibaut', 'Mordy', 'M', '1998-04-02', 'Oji River', 'ENALAPRIL MALEATE', 107, 115, 'MD');
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (9, 'Nathanil', 'Berzin', 'M', '1956-07-22', 'Llauta', 'Sodium Fluoride', 192, 186, 'MO');
INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (10, 'Derk', 'Willetts', 'M', '2018-01-22', 'Murygino', 'Furosemide', 77, 230, 'TN');

INSERT INTO patients (patient_id, first_name, last_name, gender, birth_date, city, allergies, height, weight,
                      province_id)
VALUES (11, 'Seth', 'Tatule', 'M', '2014-01-22', 'Ankara', 'Furosemide', 17, 130, 'TN');



CREATE TABLE doctors
(
    doctor_id  integer PRIMARY KEY,
    first_name text,
    last_name  text,
    speciality text
);

INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (1, 'Averil', 'Tredget', 'Dandruff');
INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (2, 'Griff', 'Spradbrow', 'Nitrostat');
INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (3, 'Chas', 'Lavalde', 'Caffeic Acid');
INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (4, 'Cindee', 'Rosentholer', 'ATORVASTATIN CALCIUM');
INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (5, 'Tracy', 'Meeking', 'PREDNISOLONE');
INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (6, 'Alastair', 'Phythian', 'Lisinopril and Hydrochlorothiazide');
INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (7, 'Doralia', 'Trim', 'Ulta Vanilla Sugar Anti-Bacterial Deep Cleansing');
INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (8, 'Josie', 'Hurlestone', 'Vinorelbine');
INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (9, 'Dougy', 'Dury', 'NON-DROWSY DAYTIME SINUS RELIEF');
INSERT INTO doctors (doctor_id, first_name, last_name, speciality)
VALUES (10, 'Devin', 'Mensler', 'Oral Defense');


CREATE TABLE admissions
(
    patient_id          integer REFERENCES patients (patient_id),
    admission_date      date,
    discharge_date      date,
    diagnosis           text,
    attending_doctor_id integer REFERENCES doctors (doctor_id)
);

INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (1, '2021-03-26', '2022-12-17', 'Corrosion of third degree of unspecified palm, subs encntr', 9);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (2, '2021-02-18', '2020-01-19', 'Other disorders of patella, unspecified knee', 7);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (3, '2021-06-22', '2022-12-16', 'Poisoning by opth drugs and prep, accidental, sequela', 3);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (1, '2020-11-28', '2022-01-31', 'Nondisp fx of medial epicondyle of r humerus, sequela', 1);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (5, '2022-01-27', '2022-08-20', 'Leakage of biological heart valve graft, subs encntr', 5);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (6, '2022-07-21', '2020-10-28', 'Disp fx of trapezoid, left wrist, subs for fx w nonunion', 4);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (1, '2022-06-02', '2022-11-17', 'Oth viral infections with skin and mucous membrane lesions', 4);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (8, '2020-10-28', '2021-10-20', 'Burn of first degree of right shoulder, sequela', 7);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (9, '2022-04-24', '2020-09-28', 'Poisn by anticoag antag, vitamin K and oth coag, undet, init', 9);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (10, '2020-06-23', '2022-01-08', 'Presence of right artificial elbow joint', 10);
INSERT INTO admissions (patient_id, admission_date, discharge_date, diagnosis, attending_doctor_id)
VALUES (11, '2020-06-23', '2022-01-09', 'Presence of right artificial elbow joint', 10);
"""

# -------------------------
# Canonicalization & Normalization
# -------------------------
def canonicalize(sql: str, dialect: str = "ansi") -> Optional[str]:
    """
    Parse & produce a canonical SQL string using sqlglot.
    Returns canonical SQL or None on parse error.
    """
    try:
        ast = parse_one(sql, read=dialect, error_level="raise")
        # Deterministic ordering: sort select expressions by their SQL repr
        try:
            sel = ast.find(Select)
            if sel and getattr(sel, "expressions", None):
                sel.expressions = sorted(sel.expressions, key=lambda e: e.sql().lower())
        except Exception:
            pass

        # Convert to stable SQL representation
        canonical = ast.to_sql( pretty=False)
        # Re-parse to ensure consistent formatting
        ast2 = parse_one(canonical, error_level="raise")
        return ast2.to_sql( pretty=False)
    except ParseError as e:
        return None
    except Exception:
        return None

# -------------------------
# AST structural diff
# -------------------------
class ASTDiff:
    def __init__(self):
        self.parse_error: Optional[str] = None
        self.structural_diffs: List[str] = []
        self.normalized_student: str = ""
        self.normalized_reference: str = ""

def _collect_columns(ast) -> List[str]:
    return sorted({c.sql().lower() for c in ast.find_all(Column)})

def _collect_tables(ast) -> List[str]:
    return sorted({t.sql().lower() for t in ast.find_all(Table)})

def _count_subqueries(ast) -> int:
    return len(list(ast.find_all(Subquery)))

def ast_diff(student_sql: str, reference_sql: str, dialect: str = "ansi") -> ASTDiff:
    res = ASTDiff()
    can_student = canonicalize(student_sql, dialect=dialect)
    can_ref = canonicalize(reference_sql, dialect=dialect)

    res.normalized_student = can_student or student_sql
    res.normalized_reference = can_ref or reference_sql

    try:
        ast_s = parse_one(res.normalized_student, error_level="raise")
        ast_r = parse_one(res.normalized_reference, error_level="raise")
    except Exception as e:
        res.parse_error = str(e)
        return res

    # Columns
    s_cols, r_cols = _collect_columns(ast_s), _collect_columns(ast_r)
    if s_cols != r_cols:
        missing = [c for c in r_cols if c not in s_cols]
        extra = [c for c in s_cols if c not in r_cols]
        if missing:
            res.structural_diffs.append(f"Missing columns in SELECT: {missing}")
        if extra:
            res.structural_diffs.append(f"Extra columns in SELECT: {extra}")

    # Tables
    s_tables, r_tables = _collect_tables(ast_s), _collect_tables(ast_r)
    if s_tables != r_tables:
        missing_t = [t for t in r_tables if t not in s_tables]
        extra_t = [t for t in s_tables if t not in r_tables]
        if missing_t:
            res.structural_diffs.append(f"Missing tables in FROM/JOIN: {missing_t}")
        if extra_t:
            res.structural_diffs.append(f"Extra tables in FROM/JOIN: {extra_t}")

    # Subqueries
    s_sub = _count_subqueries(ast_s)
    r_sub = _count_subqueries(ast_r)
    if s_sub != r_sub:
        res.structural_diffs.append(f"Different nested-subquery count (student={s_sub}, reference={r_sub})")

    # GROUP BY
    try:
        s_group = {g.sql().lower() for g in (ast_s.args.get("group").expressions if ast_s.args.get("group") else [])}
        r_group = {g.sql().lower() for g in (ast_r.args.get("group").expressions if ast_r.args.get("group") else [])}
        if s_group != r_group:
            res.structural_diffs.append(f"GROUP BY mismatch: student={sorted(s_group)}, ref={sorted(r_group)}")
    except Exception:
        pass

    # JOINs: compare textual join expressions (best-effort)
    try:
        s_joins = sorted({j.sql().lower() for j in ast_s.find_all(Join)})
        r_joins = sorted({j.sql().lower() for j in ast_r.find_all(Join)})
        if s_joins != r_joins:
            res.structural_diffs.append("JOIN structure differs (check join keys and types)")
    except Exception:
        pass

    return res

# -------------------------
# Output verifier (DuckDB)
# -------------------------
class ExecutionResult:
    def __init__(self):
        self.success: bool = False
        self.error: Optional[str] = None
        self.rows: List[Tuple] = []
        self.columns: List[str] = []

def _execute_query_in_memory(con: duckdb.DuckDBPyConnection, sql: str) -> ExecutionResult:
    r = ExecutionResult()
    try:
        # execute and fetch
        result = con.execute(sql)
        # fetchall may return list of tuples
        rows = result.fetchall()
        # columns attribute is available on the result object
        cols = list(result.columns) if hasattr(result, "columns") else []
        r.success = True
        r.rows = rows
        r.columns = cols
    except Exception as e:
        r.error = str(e)
    return r

def compare_query_results(student_sql: str, reference_sql: str, setup_sql: str = "") -> Dict[str, Any]:
    con = duckdb.connect(database=":memory:")
    out: Dict[str, Any] = {"student": None, "reference": None, "equal": False, "error": None}
    try:
        if setup_sql:
            try:
                con.execute(setup_sql)
            except Exception as e:
                out["error"] = f"Failed to run setup SQL: {e}"
                return out

        s_can = canonicalize(student_sql) or student_sql
        r_can = canonicalize(reference_sql) or reference_sql

        s_res = _execute_query_in_memory(con, s_can)
        r_res = _execute_query_in_memory(con, r_can)

        out["student"] = {"success": s_res.success, "error": s_res.error, "rows": s_res.rows, "cols": s_res.columns}
        out["reference"] = {"success": r_res.success, "error": r_res.error, "rows": r_res.rows, "cols": r_res.columns}

        if not s_res.success or not r_res.success:
            out["error"] = "Execution failed for one or both queries."
            return out

        # Semantic comparison: treat as multisets of rows
        def normalize_rows(rows):
            # convert to list of tuples for comparison (DuckDB often returns tuples)
            return sorted([tuple(r) for r in rows])

        out["equal"] = normalize_rows(s_res.rows) == normalize_rows(r_res.rows)
        # also provide counts for hinting
        out["student_count"] = len(s_res.rows)
        out["reference_count"] = len(r_res.rows)
        return out
    finally:
        con.close()

# -------------------------
# Hint generation
# -------------------------
def generate_tiered_hints(ast_diff_res: ASTDiff, exec_res: Optional[Dict[str, Any]]) -> List[str]:
    hints: List[str] = []

    # Parsing error first
    if ast_diff_res.parse_error:
        hints.append("Level 1: Your SQL could not be parsed.")
        hints.append(f"Level 2: Parse error: {ast_diff_res.parse_error}")
        hints.append("Level 3: Check clause ordering and punctuation (SELECT → FROM → WHERE → GROUP BY → HAVING → ORDER BY).")
        return hints

    # Structural diffs -> Level 1/2 hints
    if ast_diff_res.structural_diffs:
        hints.append("Level 1: Review the general area(s) indicated below.")
        for d in ast_diff_res.structural_diffs:
            # map to more friendly messages
            if "Missing columns" in d:
                hints.append("Level 2: It looks like some required output columns are missing from your SELECT.")
                hints.append("Level 3: Ensure every attribute required by the task appears in SELECT (or is produced by an aggregate).")
            elif "Extra columns" in d:
                hints.append("Level 2: You have included extra columns in SELECT that the task doesn't require.")
                hints.append("Level 3: Remove unnecessary columns to match the expected projection.")
            elif "Missing tables" in d:
                hints.append("Level 2: One or more tables needed for the solution are not in your FROM/JOIN.")
                hints.append("Level 3: Check the FROM and JOIN clauses to ensure all referenced relations are present.")
            elif "Extra tables" in d:
                hints.append("Level 2: You are using additional tables not needed for the task.")
                hints.append("Level 3: Remove irrelevant tables or verify join keys.")
            elif "GROUP BY" in d:
                hints.append("Level 1: Check your GROUP BY clause.")
                hints.append("Level 2: Your grouping columns differ from expected.")
                hints.append("Level 3: Remember: non-aggregated SELECT columns must be in GROUP BY.")
            elif "JOIN structure" in d:
                hints.append("Level 1: Check your JOIN conditions.")
                hints.append("Level 2: The joins (keys or types) seem different; verify join columns and inner/outer type.")
                hints.append("Level 3: Ensure you're joining on the correct foreign-key relationships.")
            elif "subquery" in d.lower():
                hints.append("Level 1: Check your nested/subquery structure.")
                hints.append("Level 2: A subquery may be missing or placed incorrectly.")
                hints.append("Level 3: Verify subquery aliases and where they are used in the outer query.")
            else:
                # generic structural hint
                hints.append("Level 2: " + d)

        # if structural diffs exist, return them first (they're usually most actionable)
        return hints

    # If structure looks similar, use execution-level hints (if available)
    if exec_res:
        if exec_res.get("error"):
            hints.append("Level 1: Execution failed for one or both queries.")
            if exec_res.get("student") and exec_res["student"].get("error"):
                hints.append(f"Level 2: Student query error: {exec_res['student']['error']}")
            if exec_res.get("reference") and exec_res["reference"].get("error"):
                hints.append(f"Level 2: Reference query error: {exec_res['reference']['error']}")
            hints.append("Level 3: Fix syntax/runtime errors (e.g., unknown relation, bad column).")
            return hints

        # both executed successfully
        if exec_res.get("equal"):
            hints.append("Level 1: The queries produce the same results on the provided dataset.")
            hints.append("Level 2: Structurally they may differ but semantically match for this dataset.")
            hints.append("Level 3: Consider efficiency or style improvements (indexes, join order) if required.")
            return hints
        else:
            scount = exec_res.get("student_count", None)
            rcount = exec_res.get("reference_count", None)
            hints.append("Level 1: Your query returns different results than expected.")
            if scount is not None and rcount is not None:
                hints.append(f"Level 2: Your query returned {scount} row(s); expected {rcount} row(s).")
                # give direction based on row count differences
                if scount == 0 and rcount > 0:
                    hints.append("Level 3: You may be over-filtering (missing rows). Check your WHERE/HAVING predicates or join types (INNER vs OUTER).")
                elif scount > rcount:
                    hints.append("Level 3: You may be under-filtering (extra rows). Check join cardinality and WHERE conditions.")
                else:
                    hints.append("Level 3: Check aggregation/having logic and join relationships.")
            else:
                hints.append("Level 3: Inspect predicate logic, join keys, and aggregation.")
            return hints

    # Final fallback
    hints.append("Level 1: No obvious structural differences detected.")
    hints.append("Level 2: Try running both queries on richer test data or inspect intermediate results (e.g., partial aggregates).")
    hints.append("Level 3: If queries still differ semantically, consider using an LLM to produce conceptual hints (do not request the full solution).")
    return hints

# -------------------------
# CLI & main
# -------------------------
def read_file_or_string(path_or_sql: Optional[str]) -> Optional[str]:
    if not path_or_sql:
        return None
    # if path exists, try read file
    try:
        with open(path_or_sql, "r", encoding="utf-8") as f:
            txt = f.read()
            if txt.strip():
                return txt
    except Exception:
        # not a file; treat as inline SQL
        return path_or_sql
    return None

def main():
    parser = argparse.ArgumentParser(description="Compare two SQL queries and generate tiered hints.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--student", help="Student SQL (as string)")
    group.add_argument("--student-file", help="Path to file containing student SQL")
    group2 = parser.add_mutually_exclusive_group(required=True)
    group2.add_argument("--reference", help="Reference SQL (as string)")
    group2.add_argument("--reference-file", help="Path to file containing reference SQL")
    parser.add_argument("--setup", help="Optional setup SQL (DDL + INSERTs). If omitted, a small default dataset is used.")
    parser.add_argument("--dialect", help="SQL dialect for parsing (sqlglot)")
    parser.add_argument("--json", action="store_true", help="Output JSON (machine readable)")
    args = parser.parse_args()

    student_input = read_file_or_string(args.student_file or args.student)
    reference_input = read_file_or_string(args.reference_file or args.reference)
    if student_input is None or reference_input is None:
        print("Could not read SQL input(s). Provide valid SQL or file paths.", file=sys.stderr)
        sys.exit(2)

    setup_sql = None
    if args.setup:
        try:
            with open(args.setup, "r", encoding="utf-8") as f:
                setup_sql = f.read()
        except Exception as e:
            print(f"Failed to read setup file {args.setup}: {e}", file=sys.stderr)
            sys.exit(2)
    else:
        print("HI")
        # setup_sql = DEFAULT_SETUP_SQL

    # 1) AST diff (canonicalization + structural compare)
    ast_res = ast_diff(student_input, reference_input, dialect=args.dialect)

    # 2) Execution-based verification
    exec_res = compare_query_results(student_input, reference_input, setup_sql=setup_sql)

    # 3) Generate tiered hints
    hints = generate_tiered_hints(ast_res, exec_res)

    # Output
    if args.json:
        out = {
            "normalized_student": ast_res.normalized_student,
            "normalized_reference": ast_res.normalized_reference,
            "parse_error": ast_res.parse_error,
            "structural_diffs": ast_res.structural_diffs,
            "execution": exec_res,
            "hints": hints,
        }
        print(json.dumps(out, indent=2, default=str))
    else:
        print("\n--- Normalized (student) ---")
        print(ast_res.normalized_student[:1000] if ast_res.normalized_student else "<parse failed>")
        print("\n--- Normalized (reference) ---")
        print(ast_res.normalized_reference[:1000] if ast_res.normalized_reference else "<parse failed>")
        if ast_res.parse_error:
            print("\nParse error:", ast_res.parse_error)
        if ast_res.structural_diffs:
            print("\nStructural differences detected:")
            for d in ast_res.structural_diffs:
                print(" -", d)
        print("\nExecution results:")
        if exec_res.get("error"):
            print("Execution error:", exec_res["error"])
        else:
            print(" Student success:", exec_res["student"]["success"], "rows:", len(exec_res["student"]["rows"]))
            print(" Reference success:", exec_res["reference"]["success"], "rows:", len(exec_res["reference"]["rows"]))
            print(" Semantic equality on dataset:", exec_res.get("equal"))
        print("\nHints (tiered):")
        for h in hints:
            print(" -", h)

if __name__ == "__main__":
    main()
