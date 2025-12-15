# semantic_diff.py
from typing import Dict, Any, List


def semantic_diff(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Language-agnostic semantic differencing.
    Uses execution results + lightweight structural signals.
    """

    student_exec = context.get("exec_student", {})
    ref_exec = context.get("exec_ref", {})

    result = {
        "equal": False,
        "signals": [],
        "summary": None
    }

    # -------------------------
    # 1. Execution failure
    # -------------------------
    if not student_exec.get("success"):
        result["summary"] = "The program fails to execute."
        result["signals"].append("runtime_error")
        return result

    if not ref_exec.get("success"):
        result["summary"] = "The reference solution failed unexpectedly."
        result["signals"].append("reference_error")
        return result

    student_rows = student_exec.get("rows", [])
    ref_rows = ref_exec.get("rows", [])

    # -------------------------
    # 2. Output equivalence
    # -------------------------
    if student_rows == ref_rows:
        result["equal"] = True
        result["summary"] = "Outputs match exactly."
        return result

    # -------------------------
    # 3. Cardinality difference
    # -------------------------
    if len(student_rows) != len(ref_rows):
        result["signals"].append("row_count_mismatch")

    # -------------------------
    # 4. Value difference
    # -------------------------
    if len(student_rows) == len(ref_rows):
        for s, r in zip(student_rows, ref_rows):
            if s != r:
                result["signals"].append("value_mismatch")
                break

    # -------------------------
    # 5. Ordering difference
    # -------------------------
    if sorted(student_rows) == sorted(ref_rows):
        result["signals"].append("ordering_difference")

    # -------------------------
    # 6. NULL sensitivity
    # -------------------------
    def has_null(rows: List[Any]):
        return any(any(v is None for v in row) for row in rows)

    if has_null(student_rows) != has_null(ref_rows):
        result["signals"].append("null_handling_difference")

    # -------------------------
    # 7. Aggregation suspicion
    # -------------------------
    if (
        len(student_rows) < len(ref_rows)
        and len(student_rows) > 0
    ):
        result["signals"].append("aggregation_or_grouping_issue")

    # -------------------------
    # 8. Final summary
    # -------------------------
    result["summary"] = summarize_signals(result["signals"])
    return result


def summarize_signals(signals: List[str]) -> str:
    if not signals:
        return "Outputs differ in a non-obvious way."

    if "row_count_mismatch" in signals:
        return "The number of results differs from the expected output."

    if "ordering_difference" in signals:
        return "The results contain the same values but appear in a different order."

    if "aggregation_or_grouping_issue" in signals:
        return "The output size suggests a grouping or aggregation difference."

    if "null_handling_difference" in signals:
        return "The handling of missing or NULL values differs."

    return "Some result values differ from the expected output."
