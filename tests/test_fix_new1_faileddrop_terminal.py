"""Tests for NEW-1 fix: COMPLETED-FAILEDDROP treated as terminal in all constant sets.

Regression guard ensuring the loop-hang + re-dispatch-dup bug cannot silently
reappear. Two reviewers (panel_exactlyonce + panel_ops) confirmed the fix.
"""

from models import QueryResult
import config
import manage
import reconcile


def _qr(status):
    return QueryResult(roworder=1, query="q", targettbl="t", status=status)


# --- config constants ---------------------------------------------------------


def test_faileddrop_in_done_statuses():
    """Loop terminator (main.py:901) must exit when all slices are COMPLETED-FAILEDDROP."""
    assert "COMPLETED-FAILEDDROP" in config.DONE_STATUSES


def test_faileddrop_in_new_excludes_statuses():
    """Dispatch gate (main.py:521) must not re-deploy a COMPLETED-FAILEDDROP slice."""
    assert "COMPLETED-FAILEDDROP" in config.NEW_EXCLUDES_STATUSES


def test_done_statuses_still_contains_completed_and_failed():
    """Existing members must not have been accidentally dropped."""
    assert "COMPLETED" in config.DONE_STATUSES
    assert "FAILED" in config.DONE_STATUSES


def test_new_excludes_statuses_still_contains_running_completed_failed():
    assert "RUNNING" in config.NEW_EXCLUDES_STATUSES
    assert "COMPLETED" in config.NEW_EXCLUDES_STATUSES
    assert "FAILED" in config.NEW_EXCLUDES_STATUSES


# --- manage.DONE + classify_run -----------------------------------------------


def test_manage_done_contains_faileddrop():
    """manage.DONE must include COMPLETED-FAILEDDROP so classify_run exits IN PROGRESS."""
    assert "COMPLETED-FAILEDDROP" in manage.DONE


def test_classify_run_faileddrop_only_is_finished():
    """All slices COMPLETED-FAILEDDROP → run is FINISHED (not IN PROGRESS)."""
    rows = [_qr("COMPLETED-FAILEDDROP"), _qr("COMPLETED-FAILEDDROP")]
    assert manage.classify_run(rows) == "FINISHED"


def test_classify_run_mixed_faileddrop_and_completed_is_finished():
    rows = [_qr("COMPLETED"), _qr("COMPLETED-FAILEDDROP")]
    assert manage.classify_run(rows) == "FINISHED"


def test_classify_run_running_with_faileddrop_is_in_progress():
    """A RUNNING slice alongside COMPLETED-FAILEDDROP must stay IN PROGRESS."""
    rows = [_qr("RUNNING"), _qr("COMPLETED-FAILEDDROP")]
    assert manage.classify_run(rows) == "IN PROGRESS"


# --- status_headline + reconcile.final_verdict --------------------------------


def test_status_headline_faileddrop_run_shows_with_failures():
    """status_headline must NOT report bare FINISHED for a FAILEDDROP run."""
    rows = [_qr("COMPLETED"), _qr("COMPLETED-FAILEDDROP")]
    headline = manage.status_headline(rows)
    assert "FAILURES" in headline or "FAILURE" in headline


def test_final_verdict_faileddrop_is_incomplete():
    """reconcile.final_verdict: COMPLETED-FAILEDDROP is terminal but not ALL_COMPLETE."""
    rows = [_qr("COMPLETED"), _qr("COMPLETED-FAILEDDROP")]
    assert reconcile.final_verdict(rows) == "INCOMPLETE"


def test_final_verdict_all_faileddrop_is_incomplete():
    rows = [_qr("COMPLETED-FAILEDDROP")]
    assert reconcile.final_verdict(rows) == "INCOMPLETE"


def test_final_verdict_faileddrop_not_in_progress():
    """After loop exits, verdict must NOT be IN_PROGRESS (that would stall indefinitely)."""
    rows = [_qr("COMPLETED-FAILEDDROP")]
    assert reconcile.final_verdict(rows) != "IN_PROGRESS"
