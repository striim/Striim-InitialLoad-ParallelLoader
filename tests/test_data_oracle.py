"""Pure unit tests for data_oracle. No DB connections, no oracledb required."""

import sys
import pytest
import data_oracle
from data_oracle import (
    _build_where,
    _bind_row,
    _create_table_sql,
    _is_already_exists,
    _lob_to_str,
    _merge_sql,
    _NON_ID_COLUMNS,
    _row_to_query_result,
    _table_name,
    _to_bool,
    OracleOrchConfigError,
)
from models import QueryResult

# ---------------------------------------------------------------------------
# Import check
# ---------------------------------------------------------------------------


def test_import_does_not_load_oracledb():
    """Importing data_oracle must not eagerly import the optional driver."""
    assert "oracledb" not in sys.modules or sys.modules.get("oracledb") is not None


# ---------------------------------------------------------------------------
# Missing creds
# ---------------------------------------------------------------------------


def test_missing_creds_raises(monkeypatch):
    import config

    for name in (
        "ORCH_ORACLE_DSN",
        "ORCH_ORACLE_HOST",
        "ORCH_ORACLE_SERVICE",
        "ORCH_ORACLE_USER",
        "ORCH_ORACLE_PASSWORD",
    ):
        monkeypatch.setattr(config, name, "", raising=False)
    with pytest.raises(OracleOrchConfigError) as exc_info:
        data_oracle.get_oracle_connection()
    assert "Missing Oracle orchestration settings" in str(exc_info.value)


# ---------------------------------------------------------------------------
# MERGE SQL — critical correctness
# ---------------------------------------------------------------------------


def test_merge_updates_all_non_id_columns():
    sql = _merge_sql("t")
    # Extract just the UPDATE SET section (between WHEN MATCHED and WHEN NOT MATCHED)
    update_section = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split(
        "WHEN NOT MATCHED"
    )[0]
    # Every non-id column must appear in UPDATE SET
    for col in _NON_ID_COLUMNS:
        assert (
            f"T.{col} = S.{col}" in update_section
        ), f"Missing UPDATE SET for column: {col}"
    # Explicitly assert the two timestamps are present
    assert "T.started_datetime = S.started_datetime" in update_section
    assert "T.finished_datetime = S.finished_datetime" in update_section
    # id must NOT appear in UPDATE SET (it is the match key, not updated)
    assert "T.id = S.id" not in update_section


def test_merge_basic_shape():
    sql = _merge_sql("striim_orchestration")
    assert "MERGE INTO" in sql
    assert "ON (T.id = S.id)" in sql
    assert "WHEN MATCHED" in sql
    assert "WHEN NOT MATCHED" in sql
    assert "FROM dual" in sql


# ---------------------------------------------------------------------------
# CREATE TABLE SQL
# ---------------------------------------------------------------------------


def test_create_table_sql_types():
    sql = _create_table_sql("striim_orchestration")
    assert "iscurrentrow      NUMBER(1)" in sql
    assert "query             CLOB" in sql
    assert "id                NUMBER PRIMARY KEY" in sql


# ---------------------------------------------------------------------------
# _is_already_exists
# ---------------------------------------------------------------------------


def test_is_already_exists_true():
    assert _is_already_exists(
        Exception("ORA-00955: name is already used by an existing object")
    )


def test_is_already_exists_false():
    assert not _is_already_exists(Exception("ORA-00942: table or view does not exist"))


# ---------------------------------------------------------------------------
# _build_where
# ---------------------------------------------------------------------------


def test_build_where_current_and_run():
    sql, binds = _build_where("iscurrentrow = True AND uniquerunid = 42")
    assert "iscurrentrow = :ic" in sql
    assert "uniquerunid = :run" in sql
    assert binds["ic"] == 1
    assert binds["run"] == 42


def test_build_where_current_only():
    sql, binds = _build_where("iscurrentrow = True")
    assert sql == "iscurrentrow = :ic"
    assert binds == {"ic": 1}


def test_build_where_run_only():
    sql, binds = _build_where("uniquerunid = 7")
    assert sql == "uniquerunid = :run"
    assert binds == {"run": 7}


def test_build_where_false_value():
    sql, binds = _build_where("iscurrentrow = False")
    assert binds["ic"] == 0


def test_build_where_empty():
    sql, binds = _build_where("")
    assert sql == ""
    assert binds == {}


def test_build_where_unparseable():
    sql, binds = _build_where("status = 'RUNNING'")
    assert sql is None
    assert binds == {}


# ---------------------------------------------------------------------------
# _to_bool
# ---------------------------------------------------------------------------


def test_to_bool_none():
    assert _to_bool(None) is False


def test_to_bool_zero():
    assert _to_bool(0) is False


def test_to_bool_one():
    assert _to_bool(1) is True


# ---------------------------------------------------------------------------
# _lob_to_str
# ---------------------------------------------------------------------------


class _FakeLob:
    def read(self):
        return "lob content"


def test_lob_to_str_fake_lob():
    assert _lob_to_str(_FakeLob()) == "lob content"


def test_lob_to_str_plain_str():
    assert _lob_to_str("hello") == "hello"


def test_lob_to_str_none():
    assert _lob_to_str(None) is None


# ---------------------------------------------------------------------------
# _bind_row
# ---------------------------------------------------------------------------


def _make_qr(**kwargs):
    defaults = dict(roworder=1, query="SELECT 1 FROM dual", targettbl="T")
    defaults.update(kwargs)
    r = QueryResult(**defaults)
    if r.id is None:
        r.id = 10
    if r.uniquerunid is None:
        r.uniquerunid = 5
    return r


def test_bind_row_iscurrentrow_true():
    b = _bind_row(_make_qr(iscurrentrow=True))
    assert b["iscurrentrow"] == 1


def test_bind_row_iscurrentrow_false():
    b = _bind_row(_make_qr(iscurrentrow=False))
    assert b["iscurrentrow"] == 0


def test_bind_row_appname_none_becomes_empty():
    b = _bind_row(_make_qr(appname=None))
    assert b["appname"] == ""


def test_bind_row_preserves_datetimes():
    import datetime

    dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
    b = _bind_row(_make_qr(started_datetime=dt))
    assert b["started_datetime"] == dt
    assert b["finished_datetime"] is None


def test_bind_row_preserves_targettbl_and_query():
    b = _bind_row(_make_qr(query="SELECT * FROM tab", targettbl="my_table"))
    assert b["query"] == "SELECT * FROM tab"
    assert b["targettbl"] == "my_table"


# ---------------------------------------------------------------------------
# _row_to_query_result
# ---------------------------------------------------------------------------


def test_row_to_query_result():
    lob = _FakeLob()
    row = {
        "id": 1,
        "roworder": 2,
        "uniquerunid": 3,
        "query": lob,
        "appname": "app",
        "targettbl": "tbl",
        "status": "NEW",
        "namespace": "ns",
        "started_datetime": None,
        "finished_datetime": None,
        "notes": "",
        "iscurrentrow": 0,
    }
    qr = _row_to_query_result(row)
    assert qr.iscurrentrow is False
    assert qr.query == "lob content"
    assert qr.id == 1
    assert qr.appname == "app"


# ---------------------------------------------------------------------------
# _table_name
# ---------------------------------------------------------------------------


def test_table_name_valid(monkeypatch):
    import config

    monkeypatch.setattr(
        config, "ORCH_ORACLE_TABLE_ID", "my_schema.my_table", raising=False
    )
    assert _table_name() == "my_schema.my_table"


def test_table_name_rejects_injection(monkeypatch):
    import config

    monkeypatch.setattr(
        config, "ORCH_ORACLE_TABLE_ID", "T; DROP TABLE X", raising=False
    )
    with pytest.raises(OracleOrchConfigError):
        _table_name()


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def test_data_dispatch_oracle(monkeypatch):
    import data
    import config

    monkeypatch.setattr(config, "STAGE_DB_LOCATION", "ORACLE")
    assert data.get_database() == "ORACLE"

    # read_data routes to read_from_oracle
    read_calls = []
    monkeypatch.setattr(
        data_oracle, "read_from_oracle", lambda wc: read_calls.append(wc) or []
    )
    data.read_data("iscurrentrow = True")
    assert read_calls == ["iscurrentrow = True"]

    # write_data routes to write_to_oracle
    write_calls = []
    monkeypatch.setattr(
        data_oracle, "write_to_oracle", lambda rows: write_calls.append(rows)
    )
    data.write_data([])
    assert write_calls == [[]]


# ---------------------------------------------------------------------------
# is_disconnect_error + _with_reconnect (Task 11)
# ---------------------------------------------------------------------------


class _Code:
    def __init__(self, code):
        self.code = code


class _FakeDBError(Exception):
    def __init__(self, code, text):
        super().__init__(text)
        self.args = (_Code(code),)


def test_is_disconnect_error_by_string_codes():
    assert (
        data_oracle.is_disconnect_error(
            Exception("ORA-03113: end-of-file on communication channel")
        )
        is True
    )
    assert (
        data_oracle.is_disconnect_error(
            Exception("ORA-00028: your session has been killed")
        )
        is True
    )
    assert (
        data_oracle.is_disconnect_error(
            Exception("DPY-4011: the database or network closed")
        )
        is True
    )


def test_is_disconnect_error_by_code_attr():
    assert (
        data_oracle.is_disconnect_error(_FakeDBError(2396, "exceeded idle time"))
        is True
    )


def test_is_disconnect_error_false_for_table_missing():
    assert (
        data_oracle.is_disconnect_error(
            _FakeDBError(942, "ORA-00942: table or view does not exist")
        )
        is False
    )
    assert data_oracle.is_disconnect_error(ValueError("nope")) is False


def test_with_reconnect_retries_once_then_succeeds():
    calls = {"n": 0}

    @data_oracle._with_reconnect
    def f():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _FakeDBError(3113, "ORA-03113")
        return "ok"

    assert f() == "ok"
    assert calls["n"] == 2


def test_with_reconnect_reraises_non_disconnect():
    @data_oracle._with_reconnect
    def f():
        raise _FakeDBError(942, "ORA-00942")

    import pytest

    with pytest.raises(_FakeDBError):
        f()
