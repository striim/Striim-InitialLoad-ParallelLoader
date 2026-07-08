"""Pure run-end verdict + SCN-anchored reconcile helpers.

Its own module so final_verdict is importable by BOTH main.py and manage.py
without an import cycle. No DB / oracledb imports here — every function is pure.
manage.classify_run stays for the board/status display; final_verdict is the
stricter run-end judgment.
"""

_TERMINAL = frozenset({"COMPLETED", "FAILED", "COMPLETED-FAILEDDROP"})


def final_verdict(rows):
    """Strict run-end judgment over QueryResult rows (.status)."""
    if not rows:
        return "NOT_STARTED"
    if any(r.status not in _TERMINAL for r in rows):
        return "IN_PROGRESS"
    if all(r.status == "COMPLETED" for r in rows):
        return "ALL_COMPLETE"
    return "INCOMPLETE"


def offending_counts(rows):
    """Counts of the statuses blocking ALL_COMPLETE (everything != COMPLETED)."""
    counts = {}
    for r in rows:
        if r.status != "COMPLETED":
            counts[r.status] = counts.get(r.status, 0) + 1
    return counts


def reconcile_count_sql(
    slice_query, scn
):  # scn kept for backward-compat; SCN applied via session-level DBMS_FLASHBACK
    """Source COUNT for one stored slice query.

    AS OF SCN is NOT embedded here — the caller is responsible for setting the
    session-level snapshot via the dialect's snapshot_enable_sql /
    snapshot_disable_sql before/after executing this SQL.  Inline
    ``AS OF SCN`` on an inline view is invalid Oracle syntax (ORA-00933).
    """
    inner = slice_query.rstrip().rstrip(";").rstrip()
    return f"SELECT COUNT(*) FROM ({inner})"


def summarize(
    per_slice_counts,
    rows,
    *,
    flashback_lost=False,
    flashback_lost_slice=None,
):
    """Aggregate a reconcile pass into a printable dict.

    flashback_lost: True when an ORA-01555 caused the tail of the loop to fall
        back to live counts rather than SCN-anchored flashback counts.
    flashback_lost_slice: roworder of the first slice that triggered ORA-01555.
    """
    return {
        "verdict": final_verdict(rows),
        "slice_count": len(per_slice_counts),
        "expected_source_rows": sum(per_slice_counts),
        "offending": offending_counts(rows),
        "flashback_lost": flashback_lost,
        "flashback_lost_slice": flashback_lost_slice,
    }
