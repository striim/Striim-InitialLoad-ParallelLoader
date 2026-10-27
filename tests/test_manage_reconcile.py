"""Pure wiring tests for Task 14: manage.py reconcile command + status headline surfacing.

Live source-Oracle execution is operator-verified (marked below). Tests cover:
  - reconcile parser wiring (arg parsing)
  - cmd_status headline surfacing (human print path)
  - cmd_reconcile: no-rows early exit
  - cmd_reconcile: no-SCN sidecar → state-only gate (OSError + ValueError paths)
  - cmd_reconcile: ORA-01555 fallback wiring
  - reconcile_count_sql assembly (pure, no Oracle)
"""

import json
import sys
import types
from unittest.mock import MagicMock, mock_open, patch

import manage
import reconcile
from models import QueryResult
from oracle_dialect import OracleDialect

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _qr(
    roworder=1,
    status="COMPLETED",
    query="SELECT * FROM T WHERE rownum <= 100",
    targettbl="OWNER.T",
):
    return QueryResult(
        roworder=roworder,
        query=query,
        targettbl=targettbl,
        _id=roworder,
        uniquerunid=100,
        status=status,
        iscurrentrow=True,
        appname=f"ILA_100_{roworder}.App",
        namespace=f"ILA_100_{roworder}",
        notes="",
    )


def _args(run_id=100):
    return types.SimpleNamespace(run_id=run_id)


# ---------------------------------------------------------------------------
# Step 4: Parser wiring
# ---------------------------------------------------------------------------


def test_reconcile_parser_defaults():
    args = manage.build_parser().parse_args(["reconcile"])
    assert args.func is manage.cmd_reconcile


def test_reconcile_parser_run_id():
    args = manage.build_parser().parse_args(["reconcile", "--run-id", "42"])
    assert args.run_id == 42
    assert args.func is manage.cmd_reconcile


# ---------------------------------------------------------------------------
# Step 2: cmd_status headline surfacing
# ---------------------------------------------------------------------------


def test_cmd_status_headline_shows_failures(capsys):
    """Human path must print status_headline, not bare classify_run state."""
    rows = [_qr(1, "COMPLETED"), _qr(2, "FAILED")]
    args = types.SimpleNamespace(
        run_id=100, all_runs=False, json=False, failed=False, rows=False
    )
    with patch("manage._read_current", return_value=rows):
        manage.cmd_status(args)
    out = capsys.readouterr().out
    assert "FINISHED (with FAILURES" in out


def test_cmd_status_json_preserves_classify_run_state(capsys):
    """--json path keeps classify_run state for board back-compat, not status_headline."""
    rows = [_qr(1, "COMPLETED"), _qr(2, "FAILED")]
    args = types.SimpleNamespace(
        run_id=100, all_runs=False, json=True, failed=False, rows=False
    )
    with patch("manage._read_current", return_value=rows):
        manage.cmd_status(args)
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "FINISHED"  # classify_run, not the enhanced headline


# ---------------------------------------------------------------------------
# Step 3: cmd_reconcile — no rows early exit
# ---------------------------------------------------------------------------


def test_cmd_reconcile_no_rows_returns_1(capsys):
    with patch("manage._read_current", return_value=[]):
        rc = manage.cmd_reconcile(_args())
    assert rc == 1
    assert "nothing to reconcile" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Step 3: cmd_reconcile — no SCN sidecar (state-only gate)
# ---------------------------------------------------------------------------


def test_cmd_reconcile_no_sidecar_all_complete_returns_0(capsys):
    """OSError on sidecar open + ALL_COMPLETE verdict → state-only gate, return 0."""
    rows = [_qr(1, "COMPLETED")]
    with (
        patch("manage._read_current", return_value=rows),
        patch("watermark.sidecar_path", return_value="/tmp/no_such_file.json"),
        patch("builtins.open", side_effect=OSError("not found")),
    ):
        rc = manage.cmd_reconcile(_args())
    out = capsys.readouterr().out
    assert rc == 0
    assert "state-only reconcile" in out


def test_cmd_reconcile_no_sidecar_incomplete_returns_1(capsys):
    """OSError on sidecar open + INCOMPLETE verdict → state-only gate, return 1."""
    rows = [_qr(1, "COMPLETED"), _qr(2, "FAILED")]
    with (
        patch("manage._read_current", return_value=rows),
        patch("watermark.sidecar_path", return_value="/tmp/no_such_file.json"),
        patch("builtins.open", side_effect=OSError("not found")),
    ):
        rc = manage.cmd_reconcile(_args())
    assert rc == 1


def test_cmd_reconcile_bad_json_sidecar_degrades_to_state_only(capsys):
    """ValueError (bad JSON) treated as no-SCN → state-only gate."""
    rows = [_qr(1, "COMPLETED")]
    with (
        patch("manage._read_current", return_value=rows),
        patch("watermark.sidecar_path", return_value="/tmp/bad.json"),
        patch("builtins.open", mock_open(read_data="not-json")),
    ):
        rc = manage.cmd_reconcile(_args())
    assert rc == 0
    assert "state-only reconcile" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Step 3: cmd_reconcile — ORA-01555 fallback wiring
# OPERATOR-VERIFIED: live Oracle execution is not tested here.
# ---------------------------------------------------------------------------


def test_cmd_reconcile_ora01555_fallback_wiring(capsys):
    """ORA-01555 on COUNT falls back to live (non-flashback) COUNT(*).

    OPERATOR-VERIFIED: The live Oracle path (real SCN + real source DB) is not
    exercised here. This test validates the wiring — exception detection,
    flashback disable before live fallback, and per_slice accumulation — using
    a mock cursor.

    Call sequence with DBMS_FLASHBACK:
      [0] dialect.snapshot_enable_sql  → succeeds
      [1] reconcile_count_sql          → raises ORA-01555
      [2] dialect.snapshot_disable_sql → succeeds (ORA-01555 handler)
      [3] live COUNT(*)                → succeeds, returns 42
      [4] dialect.snapshot_disable_sql → best-effort finally (succeeds)
    """
    rows = [_qr(1, "COMPLETED", query="SELECT * FROM T WHERE rownum <= 100")]
    scn = 99999
    sidecar_data = json.dumps({"value": scn})

    execute_calls = []

    class _MockCursor:
        """Second execute (the COUNT query) raises ORA-01555; all others succeed."""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            execute_calls.append(sql)
            if len(execute_calls) == 2:  # COUNT query after flashback enable
                raise RuntimeError("ORA-01555: snapshot too old")

        def fetchone(self):
            return [42]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = _MockCursor()

    mock_dialect_obj = OracleDialect()
    mock_dialect_obj.get_connection = MagicMock(return_value=mock_conn)

    mock_source_dialect = MagicMock()
    mock_source_dialect.get_dialect.return_value = mock_dialect_obj

    with (
        patch("manage._read_current", return_value=rows),
        patch("watermark.sidecar_path", return_value="/tmp/wm.json"),
        patch("builtins.open", mock_open(read_data=sidecar_data)),
        patch.dict(sys.modules, {"source_dialect": mock_source_dialect}),
    ):
        rc = manage.cmd_reconcile(_args())

    out = capsys.readouterr().out
    assert "WARNING: ORA-01555" in out
    assert "falling back to a LIVE" in out
    # Flashback was enabled first
    assert any("DBMS_FLASHBACK.ENABLE" in s for s in execute_calls)
    # Flashback was disabled (at least once — ORA-01555 handler + finally)
    assert any("DBMS_FLASHBACK.DISABLE" in s for s in execute_calls)
    # No inline AS OF SCN in any COUNT query
    count_calls = [s for s in execute_calls if "SELECT COUNT(*)" in s]
    assert count_calls, "expected at least one COUNT(*) call"
    assert all("AS OF SCN" not in s for s in count_calls)
    # ALL_COMPLETE verdict → return 0
    assert rc == 0


# ---------------------------------------------------------------------------
# SQL assembly — reconcile_count_sql + flashback helpers (pure, no Oracle)
# ---------------------------------------------------------------------------


def test_reconcile_count_sql_no_inline_as_of_scn():
    """reconcile_count_sql wraps in COUNT(*) without inline AS OF SCN.

    AS OF SCN on an inline view is invalid Oracle syntax (ORA-00933).
    The SCN is now applied at the session level via DBMS_FLASHBACK.
    """
    sql = reconcile.reconcile_count_sql("SELECT * FROM T WHERE x > 1", 54321)
    assert sql.startswith("SELECT COUNT(*)")
    assert "AS OF SCN" not in sql
    assert "SELECT * FROM T WHERE x > 1" in sql


def test_reconcile_count_sql_strips_trailing_semicolon():
    """Trailing semicolons in the slice query are stripped before wrapping."""
    sql = reconcile.reconcile_count_sql("SELECT * FROM T;", 100)
    assert ";;" not in sql
    assert "AS OF SCN" not in sql
    assert sql == "SELECT COUNT(*) FROM (SELECT * FROM T)"


def test_flashback_enable_plsql_embeds_scn():
    """OracleDialect.snapshot_enable_sql returns PL/SQL with the SCN literal."""
    sql = OracleDialect().snapshot_enable_sql(54321)
    assert "DBMS_FLASHBACK.ENABLE_AT_SYSTEM_CHANGE_NUMBER" in sql
    assert "54321" in sql


def test_flashback_disable_plsql_contains_disable():
    """OracleDialect.snapshot_disable_sql returns the DBMS_FLASHBACK.DISABLE PL/SQL."""
    sql = OracleDialect().snapshot_disable_sql()
    assert "DBMS_FLASHBACK.DISABLE" in sql


# ---------------------------------------------------------------------------
# Flashback enable/disable wiring — happy path
# OPERATOR-VERIFIED: live Oracle execution is not tested here.
# ---------------------------------------------------------------------------


def test_cmd_reconcile_flashback_enable_disable_wiring(capsys):
    """Happy path: flashback is enabled before COUNTs and disabled in finally.

    OPERATOR-VERIFIED: live Oracle execution not exercised here.
    Call sequence:
      [0] dialect.snapshot_enable_sql (SCN in body)
      [1] reconcile_count_sql         (no AS OF SCN)
      [2] dialect.snapshot_disable_sql (finally)
    """
    rows = [_qr(1, "COMPLETED", query="SELECT * FROM T WHERE rownum <= 100")]
    scn = 55555
    sidecar_data = json.dumps({"value": scn})

    execute_calls = []

    class _MockCursor:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            execute_calls.append(sql)

        def fetchone(self):
            return [10]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = _MockCursor()

    mock_dialect_obj = OracleDialect()
    mock_dialect_obj.get_connection = MagicMock(return_value=mock_conn)

    mock_source_dialect = MagicMock()
    mock_source_dialect.get_dialect.return_value = mock_dialect_obj

    with (
        patch("manage._read_current", return_value=rows),
        patch("watermark.sidecar_path", return_value="/tmp/wm.json"),
        patch("builtins.open", mock_open(read_data=sidecar_data)),
        patch.dict(sys.modules, {"source_dialect": mock_source_dialect}),
    ):
        rc = manage.cmd_reconcile(_args())

    assert rc == 0
    # Enable fires before COUNT; disable fires after
    enable_idx = next(
        (i for i, s in enumerate(execute_calls) if "DBMS_FLASHBACK.ENABLE" in s), None
    )
    count_idx = next(
        (i for i, s in enumerate(execute_calls) if s.startswith("SELECT COUNT(*)")),
        None,
    )
    disable_idx = next(
        (i for i, s in enumerate(execute_calls) if "DBMS_FLASHBACK.DISABLE" in s), None
    )
    assert enable_idx is not None, "flashback_enable_plsql was never called"
    assert count_idx is not None, "COUNT(*) was never called"
    assert disable_idx is not None, "flashback_disable_plsql was never called"
    assert enable_idx < count_idx < disable_idx
    # SCN is embedded in the enable call
    assert str(scn) in execute_calls[enable_idx]
    # No inline AS OF SCN in the COUNT
    assert "AS OF SCN" not in execute_calls[count_idx]


# ---------------------------------------------------------------------------
# NEW-3: mid-loop ORA-01555 — remaining slices live-counted, summary re-labeled
# OPERATOR-VERIFIED: live Oracle execution is not tested here.
# ---------------------------------------------------------------------------


def test_cmd_reconcile_ora01555_mid_loop_remaining_slices_live(capsys):
    """ORA-01555 on slice 2/3 → slices 2+3 are live-counted without re-enabling
    flashback; ONE global NOT-SCN-anchored warning; summary says 'live counts'
    and does NOT claim 'AS OF SCN {scn}'.

    OPERATOR-VERIFIED: live Oracle path not exercised here.

    Sequence:
      Slice 1  — flashback COUNT(*) → 50  (happy path)
      Slice 2  — flashback COUNT(*) → ORA-01555
                  handler: disable flashback, live COUNT(*) → 50, flashback_lost=True
      Slice 3  — flashback_lost branch: live COUNT(*) directly → 50  (no second DISABLE churn)
      Finally  — best-effort DISABLE (already disabled; mock swallows it)
    """
    rows = [
        _qr(1, "COMPLETED", query="SELECT * FROM T1 WHERE rownum <= 100"),
        _qr(2, "COMPLETED", query="SELECT * FROM T2 WHERE rownum <= 100"),
        _qr(3, "COMPLETED", query="SELECT * FROM T3 WHERE rownum <= 100"),
    ]
    scn = 77777
    sidecar_data = json.dumps({"value": scn})

    execute_calls = []
    count_call_n = [0]

    class _MockCursor:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            execute_calls.append(sql)
            if "SELECT COUNT(*)" in sql:
                count_call_n[0] += 1
                if count_call_n[0] == 1:
                    pass  # slice 1 flashback COUNT: succeeds
                elif count_call_n[0] == 2:
                    raise RuntimeError("ORA-01555: snapshot too old")
                # count_call_n 3 (slice 2 live) and 4 (slice 3 live): succeed

        def fetchone(self):
            return [50]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = _MockCursor()

    mock_dialect_obj = OracleDialect()
    mock_dialect_obj.get_connection = MagicMock(return_value=mock_conn)

    mock_source_dialect = MagicMock()
    mock_source_dialect.get_dialect.return_value = mock_dialect_obj

    with (
        patch("manage._read_current", return_value=rows),
        patch("watermark.sidecar_path", return_value="/tmp/wm.json"),
        patch("builtins.open", mock_open(read_data=sidecar_data)),
        patch.dict(sys.modules, {"source_dialect": mock_source_dialect}),
    ):
        rc = manage.cmd_reconcile(_args())

    out = capsys.readouterr().out

    # ONE per-slice ORA-01555 warning (slice 2 only; slice 3 skips flashback entirely)
    ora01555_lines = [ln for ln in out.splitlines() if "ORA-01555" in ln]
    assert (
        len(ora01555_lines) == 1
    ), f"Expected exactly 1 ORA-01555 warning line, got {len(ora01555_lines)}: {ora01555_lines}"

    # Global warning that the result is not SCN-anchored
    assert "NOT SCN-anchored" in out

    # Summary count line must say 'live counts' and must NOT claim 'AS OF SCN {scn}'
    assert "live counts" in out
    count_summary_lines = [
        ln
        for ln in out.splitlines()
        if "live counts" in ln or "expected source rows" in ln
    ]
    assert count_summary_lines, "no count summary line found"
    for ln in count_summary_lines:
        assert (
            f"AS OF SCN {scn}" not in ln
        ), f"SCN-anchored label must not appear in count summary: {ln!r}"

    # Flashback was enabled at start and disabled at least once
    assert any("DBMS_FLASHBACK.ENABLE" in s for s in execute_calls)
    disable_calls = [s for s in execute_calls if "DBMS_FLASHBACK.DISABLE" in s]
    assert disable_calls

    # Exactly 4 COUNT(*) attempts: slice1-flashback, slice2-raises, slice2-live, slice3-live
    assert (
        count_call_n[0] == 4
    ), f"Expected 4 COUNT(*) execute calls, got {count_call_n[0]}"

    # ALL_COMPLETE verdict → return 0
    assert rc == 0
