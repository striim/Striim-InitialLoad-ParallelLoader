"""Pure-unit tests for the Task-2 setup helpers in manage.py.

No DB connections are made — all tested helpers are import-safe without heavy deps.
"""

import manage


def test_normalize_backend():
    assert manage.normalize_backend("ORACLE") == "ORACLE"
    assert manage.normalize_backend("ORA") == "ORACLE"
    assert manage.normalize_backend("PG") == "PG"
    assert manage.normalize_backend("POSTGRES") == "PG"
    assert manage.normalize_backend("POSTGRESQL") == "PG"
    assert manage.normalize_backend("BQ") == "BQ"
    assert manage.normalize_backend("BIGQUERY") == "BQ"
    assert manage.normalize_backend("TinyDB") == "TinyDB"
    assert manage.normalize_backend("") == "TinyDB"
    assert manage.normalize_backend("unknown") == "TinyDB"
    assert manage.normalize_backend(None) == "TinyDB"


def test_required_env_vars_oracle():
    vars_ = manage.required_env_vars("ORACLE")
    assert "ORCH_ORACLE_USER" in vars_
    assert "ORCH_ORACLE_DSN" in vars_
    assert "ORCH_ORACLE_PASSWORD" in vars_
    assert "ORACLE_USER" not in vars_


def test_required_env_vars_pg():
    assert "PG_HOST" in manage.required_env_vars("PG")


def test_required_env_vars_bq():
    assert "BQ_PROJECT_ID" in manage.required_env_vars("BQ")


def test_required_env_vars_tinydb_empty():
    assert manage.required_env_vars("TinyDB") == []


def test_required_env_vars_accepts_aliases():
    assert manage.required_env_vars("ORA") == manage.required_env_vars("ORACLE")


def test_env_block_oracle():
    block = manage.env_block("ORACLE")
    assert "export ORCH_ORACLE_USER=" in block


def test_env_block_tinydb_empty():
    assert manage.env_block("TinyDB") == ""


def test_bq_ddl_from_template():
    template = (
        "CREATE TABLE `YOUR_PROJECT_ID.YOUR_DATASET_ID.striim_orchestration`"
        " (\n    id INTEGER NOT NULL\n);"
    )
    result = manage._bq_ddl_from_template(
        template, "proj", "ds", "striim_orchestration"
    )
    assert "`proj.ds.striim_orchestration`" in result
    assert "CREATE TABLE IF NOT EXISTS" in result
    assert "YOUR_PROJECT_ID" not in result


def test_setup_parser_wiring():
    p = manage.build_parser()
    args = p.parse_args(["setup"])
    assert args.func is manage.cmd_setup
    assert args.backend is None
    assert args.yes is False

    args2 = p.parse_args(["setup", "--backend", "ORACLE", "--yes"])
    assert args2.backend == "ORACLE"
    assert args2.yes is True


# --- Task 8: status_headline + schema_diff -----------------------------------
from models import QueryResult


def _qr(status):
    return QueryResult(roworder=1, query="q", targettbl="t", status=status)


def test_status_headline_finished_with_failures():
    rows = [_qr("COMPLETED"), _qr("FAILED")]
    assert manage.status_headline(rows) == "FINISHED (with FAILURES — run not cleared)"


def test_status_headline_all_complete():
    rows = [_qr("COMPLETED"), _qr("COMPLETED")]
    assert manage.status_headline(rows) == "FINISHED"


def test_status_headline_in_progress():
    rows = [_qr("RUNNING"), _qr("COMPLETED")]
    assert manage.status_headline(rows) == "IN PROGRESS"


def test_schema_diff_match_case_insensitive():
    ok, missing, extra = manage.schema_diff(["id", "Status"], ["ID", "status"])
    assert ok is True and missing == [] and extra == []


def test_schema_diff_missing_and_extra():
    ok, missing, extra = manage.schema_diff(["id", "status", "notes"], ["id", "extra1"])
    assert ok is False
    assert missing == ["notes", "status"]
    assert extra == ["extra1"]


# --- Task 16: _verify_schema + cmd_setup schema/PK/session-only messaging ---
import argparse
import sys
import types


def test_verify_schema_non_oracle_returns_ok():
    """Non-Oracle backends skip verification and return (True, [], [])."""
    for backend in ("PG", "BQ", "TinyDB"):
        ok, missing, extra = manage._verify_schema(backend)
        assert ok is True and missing == [] and extra == [], f"failed for {backend}"


def test_verify_schema_oracle_calls_schema_diff(monkeypatch):
    """Oracle path queries live columns and runs schema_diff; match -> (True, [], [])."""

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def execute(self, sql, **kw):
            pass

        def fetchall(self):
            return [("ID",), ("STATUS",), ("NOTES",)]

    class FakeConn:
        def cursor(self):
            return FakeCursor()

    fake_oracle = types.SimpleNamespace(
        get_oracle_connection=lambda: FakeConn(),
        _table_name=lambda: "MY_TABLE",
        _COLUMNS=["id", "status", "notes"],
    )
    monkeypatch.setitem(sys.modules, "data_oracle", fake_oracle)

    ok, missing, extra = manage._verify_schema("ORACLE")
    assert ok is True
    assert missing == []
    assert extra == []


def test_verify_schema_oracle_reports_mismatch(monkeypatch):
    """schema_diff mismatch propagates correctly from _verify_schema."""

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def execute(self, sql, **kw):
            pass

        def fetchall(self):
            # missing 'notes', has 'extra_col' instead
            return [("ID",), ("STATUS",), ("EXTRA_COL",)]

    class FakeConn:
        def cursor(self):
            return FakeCursor()

    fake_oracle = types.SimpleNamespace(
        get_oracle_connection=lambda: FakeConn(),
        _table_name=lambda: "MY_TABLE",
        _COLUMNS=["id", "status", "notes"],
    )
    monkeypatch.setitem(sys.modules, "data_oracle", fake_oracle)

    ok, missing, extra = manage._verify_schema("ORACLE")
    assert ok is False
    assert "notes" in missing
    assert "extra_col" in [e.lower() for e in extra]


def test_verify_schema_exception_returns_ok(monkeypatch, capsys):
    """Any exception inside _verify_schema is swallowed; returns (True, [], [])."""

    def bad_connect():
        raise RuntimeError("connect failed")

    fake_oracle = types.SimpleNamespace(
        get_oracle_connection=bad_connect,
        _table_name=lambda: "T",
        _COLUMNS=[],
    )
    monkeypatch.setitem(sys.modules, "data_oracle", fake_oracle)

    ok, missing, extra = manage._verify_schema("ORACLE")
    assert ok is True and missing == [] and extra == []
    assert "schema verify skipped" in capsys.readouterr().out


def _setup_args(backend, yes=True):
    return argparse.Namespace(backend=backend, yes=yes)


def test_cmd_setup_session_only_in_env_template(monkeypatch, capsys):
    """env template print includes SESSION-ONLY warning for backends that need env vars."""
    monkeypatch.setattr(manage, "_check_connectivity", lambda b: (True, "OK"))
    monkeypatch.setattr(manage, "_confirm", lambda msg, yes: False)
    monkeypatch.setattr(manage, "_verify_schema", lambda b: (True, [], []))

    manage.cmd_setup(_setup_args("PG"))
    out = capsys.readouterr().out
    assert "SESSION-ONLY" in out


def test_cmd_setup_target_pk_warning_for_pg(monkeypatch, capsys):
    """PG backend prints the target-PK idempotency reminder."""
    monkeypatch.setattr(manage, "_check_connectivity", lambda b: (True, "OK"))
    monkeypatch.setattr(manage, "_confirm", lambda msg, yes: False)
    monkeypatch.setattr(manage, "_verify_schema", lambda b: (True, [], []))

    manage.cmd_setup(_setup_args("PG"))
    out = capsys.readouterr().out
    assert "HEAP target" in out
    assert "DUPLICATE_ROW_EXISTS" in out


def test_cmd_setup_target_pk_warning_for_bq(monkeypatch, capsys):
    """BQ backend also prints the target-PK reminder."""
    monkeypatch.setattr(manage, "_check_connectivity", lambda b: (True, "OK"))
    monkeypatch.setattr(manage, "_confirm", lambda msg, yes: False)
    monkeypatch.setattr(manage, "_verify_schema", lambda b: (True, [], []))

    manage.cmd_setup(_setup_args("BQ"))
    out = capsys.readouterr().out
    assert "HEAP target" in out


def test_cmd_setup_no_pk_warning_for_tinydb(monkeypatch, capsys):
    """TinyDB backend does NOT print the target-PK reminder."""
    monkeypatch.setattr(manage, "_check_connectivity", lambda b: (True, "OK"))
    monkeypatch.setattr(manage, "_verify_schema", lambda b: (True, [], []))

    manage.cmd_setup(_setup_args("TinyDB"))
    out = capsys.readouterr().out
    assert "HEAP target" not in out


def test_cmd_setup_schema_mismatch_printed(monkeypatch, capsys):
    """When _verify_schema returns mismatch, the ✗ schema mismatch line is printed."""
    monkeypatch.setattr(manage, "_check_connectivity", lambda b: (True, "OK"))
    monkeypatch.setattr(manage, "_confirm", lambda msg, yes: True)
    monkeypatch.setattr(manage, "_run_ddl", lambda b: (True, "table ok"))
    monkeypatch.setattr(
        manage, "_verify_schema", lambda b: (False, ["notes"], ["extra_col"])
    )

    manage.cmd_setup(_setup_args("PG"))
    out = capsys.readouterr().out
    assert "schema mismatch" in out
    assert "notes" in out
    assert "extra_col" in out


def test_cmd_setup_oracle_schema_ok_prints_verified(monkeypatch, capsys):
    """ORACLE backend with a clean schema diff prints the ✓ verified message."""
    monkeypatch.setattr(manage, "_check_connectivity", lambda b: (True, "Oracle OK"))
    monkeypatch.setattr(manage, "_confirm", lambda msg, yes: True)
    monkeypatch.setattr(manage, "_run_ddl", lambda b: (True, "table ok"))
    monkeypatch.setattr(manage, "_verify_schema", lambda b: (True, [], []))

    manage.cmd_setup(_setup_args("ORACLE"))
    out = capsys.readouterr().out
    assert "schema columns verified" in out


def test_cmd_setup_pg_no_schema_verified_message(monkeypatch, capsys):
    """PG backend with clean diff does NOT print the Oracle-specific verified message."""
    monkeypatch.setattr(manage, "_check_connectivity", lambda b: (True, "OK"))
    monkeypatch.setattr(manage, "_confirm", lambda msg, yes: True)
    monkeypatch.setattr(manage, "_run_ddl", lambda b: (True, "table ok"))
    monkeypatch.setattr(manage, "_verify_schema", lambda b: (True, [], []))

    manage.cmd_setup(_setup_args("PG"))
    out = capsys.readouterr().out
    assert "schema columns verified" not in out


import pytest


@pytest.mark.parametrize("good", ["", "1", "5432", "1521", "  1433  "])
def test_valid_port_input_accepts_blank_and_positive(good):
    assert manage._valid_port_input(good)


@pytest.mark.parametrize("bad", ["0", "-1", "abc", "54.32", "12a", "-"])
def test_valid_port_input_rejects_zero_negative_and_nonnumeric(bad):
    assert not manage._valid_port_input(bad)
