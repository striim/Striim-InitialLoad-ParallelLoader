from models import QueryResult
import reconcile
from oracle_dialect import OracleDialect


def _qr(status):
    return QueryResult(roworder=1, query="q", targettbl="t", status=status)


def test_final_verdict_empty_not_started():
    assert reconcile.final_verdict([]) == "NOT_STARTED"


def test_final_verdict_in_progress_when_non_terminal():
    assert reconcile.final_verdict([_qr("COMPLETED"), _qr("RUNNING")]) == "IN_PROGRESS"


def test_final_verdict_new_is_in_progress():
    assert reconcile.final_verdict([_qr("NEW")]) == "IN_PROGRESS"


def test_final_verdict_all_complete():
    assert (
        reconcile.final_verdict([_qr("COMPLETED"), _qr("COMPLETED")]) == "ALL_COMPLETE"
    )


def test_final_verdict_incomplete_when_failed():
    assert reconcile.final_verdict([_qr("COMPLETED"), _qr("FAILED")]) == "INCOMPLETE"


def test_final_verdict_faileddrop_not_complete():
    assert (
        reconcile.final_verdict([_qr("COMPLETED"), _qr("COMPLETED-FAILEDDROP")])
        == "INCOMPLETE"
    )


def test_offending_counts_excludes_completed():
    rows = [_qr("COMPLETED"), _qr("FAILED"), _qr("FAILED"), _qr("COMPLETED-FAILEDDROP")]
    assert reconcile.offending_counts(rows) == {"FAILED": 2, "COMPLETED-FAILEDDROP": 1}


def test_reconcile_count_sql_strips_semicolon():
    # AS OF SCN is now applied via session-level DBMS_FLASHBACK, not inline
    sql = reconcile.reconcile_count_sql("SELECT * FROM SALES.T WHERE X = 1 ;", 998877)
    assert sql == "SELECT COUNT(*) FROM (SELECT * FROM SALES.T WHERE X = 1)"


def test_flashback_enable_plsql_embeds_scn():
    # Oracle flashback PL/SQL now lives on OracleDialect.snapshot_enable_sql.
    sql = OracleDialect().snapshot_enable_sql(998877)
    assert "DBMS_FLASHBACK.ENABLE_AT_SYSTEM_CHANGE_NUMBER" in sql
    assert "998877" in sql
    assert sql.startswith("BEGIN") and sql.endswith("END;")


def test_flashback_disable_plsql():
    sql = OracleDialect().snapshot_disable_sql()
    assert "DBMS_FLASHBACK.DISABLE" in sql
    assert sql.startswith("BEGIN") and sql.endswith("END;")


def test_summarize_all_complete():
    rows = [_qr("COMPLETED"), _qr("COMPLETED"), _qr("COMPLETED")]
    out = reconcile.summarize([10, 20, 30], rows)
    assert out == {
        "verdict": "ALL_COMPLETE",
        "slice_count": 3,
        "expected_source_rows": 60,
        "offending": {},
        "flashback_lost": False,
        "flashback_lost_slice": None,
    }


def test_summarize_flags_failed():
    rows = [_qr("COMPLETED"), _qr("FAILED")]
    out = reconcile.summarize([5, 0], rows)
    assert out["verdict"] == "INCOMPLETE"
    assert out["expected_source_rows"] == 5
    assert out["offending"] == {"FAILED": 1}


# ---------------------------------------------------------------------------
# NEW-3: summarize flashback_lost labeling (pure, no Oracle)
# ---------------------------------------------------------------------------


def test_summarize_flashback_lost_false_by_default():
    """summarize without kwargs: flashback_lost=False, flashback_lost_slice=None."""
    rows = [_qr("COMPLETED")]
    out = reconcile.summarize([10], rows)
    assert out["flashback_lost"] is False
    assert out["flashback_lost_slice"] is None


def test_summarize_flashback_lost_true_carries_fields():
    """flashback_lost=True: flag and slice index present; totals unaffected."""
    rows = [_qr("COMPLETED"), _qr("COMPLETED"), _qr("COMPLETED")]
    out = reconcile.summarize(
        [10, 20, 30], rows, flashback_lost=True, flashback_lost_slice=2
    )
    assert out["flashback_lost"] is True
    assert out["flashback_lost_slice"] == 2
    assert out["expected_source_rows"] == 60
    assert out["slice_count"] == 3
    assert out["verdict"] == "ALL_COMPLETE"


class _Code:
    def __init__(self, code):
        self.code = code


class _FakeOraErr(Exception):
    def __init__(self, code, text):
        super().__init__(text)
        self.args = (_Code(code),)


def test_is_snapshot_too_old_by_string():
    # ORA-01555 detection now lives on OracleDialect.is_snapshot_lost.
    assert (
        OracleDialect().is_snapshot_lost(Exception("ORA-01555: snapshot too old"))
        is True
    )


def test_is_snapshot_too_old_by_code():
    assert (
        OracleDialect().is_snapshot_lost(_FakeOraErr(1555, "snapshot too old")) is True
    )


def test_is_snapshot_too_old_false_for_other():
    assert OracleDialect().is_snapshot_lost(_FakeOraErr(942, "ORA-00942")) is False
