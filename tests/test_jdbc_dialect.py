"""Pure (no-DB, no-jaydebeapi) tests for jdbc_dialect.JdbcDialect.

The JDBC dialect is import-safe without JayDeBeApi/JPype/Java (the driver is
imported lazily inside get_connection only), so these tests never touch it. The
pure surface — render_predicate, _format_literal, limited, explain,
is_partitioned/list_partitions, segment_size_bytes, snapshot_* , _cut_points,
_is_range_type — is covered directly. Cursor-needing paths (validate_table,
discover_candidates, boundaries, capture_watermark) are exercised with light
fakes, never a real connection.
"""

import datetime
from decimal import Decimal

import pytest

import config
from jdbc_dialect import JdbcDialect, JdbcDialectError
from source_dialect import Boundary, Candidate, PlanSummary


@pytest.fixture
def d():
    return JdbcDialect()


# ---------------------------------------------------------------------------
# minimal fakes for cursor-needing paths
# ---------------------------------------------------------------------------


class FakeCursor:
    def __init__(
        self,
        *,
        description=None,
        fetchone_result=None,
        rows=None,
        raise_on_execute=None,
    ):
        self.description = description
        self._fetchone_result = fetchone_result
        self._rows = list(rows) if rows is not None else None
        self.raise_on_execute = raise_on_execute
        self.executed = []
        self.closed = False

    def execute(self, sql, *args, **kwargs):
        self.executed.append(sql)
        if self.raise_on_execute is not None:
            raise self.raise_on_execute

    def fetchone(self):
        if self._rows is not None:
            return self._rows.pop(0) if self._rows else None
        return self._fetchone_result

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


# ---------------------------------------------------------------------------
# instantiation + class attrs (import-safe, no jaydebeapi)
# ---------------------------------------------------------------------------


def test_instantiates_and_name():
    d = JdbcDialect()
    assert d.name == "jdbc"
    assert d.watermark_label == "JDBC watermark"


# ---------------------------------------------------------------------------
# render_predicate — column (half-open) + whole-table + IS NULL
# ---------------------------------------------------------------------------


def test_render_column_int_no_alias(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=1000, hi=2000, label="ID")
    assert d.render_predicate(cand, b) == "ID >= 1000 AND ID < 2000"


def test_render_column_alias_qualifies(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=1000, hi=2000, label="ID")
    assert d.render_predicate(cand, b, alias="s") == "s.ID >= 1000 AND s.ID < 2000"


def test_render_column_alias_skipped_when_key_already_dotted(d):
    cand = Candidate(kind="column", key="s.CREATED_DT", label="s.CREATED_DT")
    b = Boundary(lo=1, hi=2)
    assert (
        d.render_predicate(cand, b, alias="x")
        == "s.CREATED_DT >= 1 AND s.CREATED_DT < 2"
    )


def test_render_column_datetime_boundary(d):
    cand = Candidate(kind="column", key="CREATED_DT", label="CREATED_DT")
    b = Boundary(
        lo=datetime.datetime(2024, 1, 1, 0, 0, 0),
        hi=datetime.datetime(2024, 2, 1, 12, 30, 45),
        label="CREATED_DT",
    )
    assert d.render_predicate(cand, b, alias="s") == (
        "s.CREATED_DT >= '2024-01-01 00:00:00' "
        "AND s.CREATED_DT < '2024-02-01 12:30:45'"
    )


def test_render_column_date_boundary(d):
    cand = Candidate(kind="column", key="D", label="D")
    b = Boundary(lo=datetime.date(2024, 1, 1), hi=datetime.date(2024, 6, 1), label="D")
    assert d.render_predicate(cand, b) == "D >= '2024-01-01' AND D < '2024-06-01'"


def test_render_column_str_literal_escapes_quote(d):
    cand = Candidate(kind="column", key="NAME", label="NAME")
    b = Boundary(lo="A", hi="O'Brien", label="NAME")
    assert d.render_predicate(cand, b) == "NAME >= 'A' AND NAME < 'O''Brien'"


def test_render_column_open_below(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=None, hi=1000, label="ID")
    assert d.render_predicate(cand, b) == "ID < 1000"


def test_render_column_open_above(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=2000, hi=None, label="ID")
    assert d.render_predicate(cand, b) == "ID >= 2000"


def test_render_column_is_null(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(is_null=True, label="ID")
    assert d.render_predicate(cand, b) == "ID IS NULL"


def test_render_column_is_null_alias_qualifies(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(is_null=True, label="ID")
    assert d.render_predicate(cand, b, alias="s") == "s.ID IS NULL"


def test_render_column_whole_table_both_none(d):
    # boundaries() whole-table fallback: lo=None, hi=None, not is_null -> 1=1
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=None, hi=None, label="ID")
    assert d.render_predicate(cand, b) == "1=1"


def test_render_non_column_kind_raises(d):
    cand = Candidate(kind="physical", key=None, label="ROWID")
    with pytest.raises(ValueError):
        d.render_predicate(cand, Boundary(lo=1, hi=2))


# ---------------------------------------------------------------------------
# _format_literal (pure, generic)
# ---------------------------------------------------------------------------


def test_format_literal_int(d):
    assert d._format_literal(42) == "42"


def test_format_literal_float(d):
    assert d._format_literal(1.5) == "1.5"


def test_format_literal_decimal(d):
    assert d._format_literal(Decimal("3.14")) == "3.14"


def test_format_literal_bool_true(d):
    assert d._format_literal(True) == "1"


def test_format_literal_bool_false(d):
    assert d._format_literal(False) == "0"


def test_format_literal_str_doubles_quotes(d):
    assert d._format_literal("O'Brien") == "'O''Brien'"


def test_format_literal_date(d):
    assert d._format_literal(datetime.date(2024, 3, 4)) == "'2024-03-04'"


def test_format_literal_datetime(d):
    v = datetime.datetime(2024, 3, 4, 5, 6, 7)
    assert d._format_literal(v) == "'2024-03-04 05:06:07'"


def test_format_literal_datetime_drops_microseconds(d):
    # generic ISO 'YYYY-MM-DD HH:MM:SS' — no fractional seconds
    v = datetime.datetime(2024, 3, 4, 5, 6, 7, 123456)
    assert d._format_literal(v) == "'2024-03-04 05:06:07'"


# ---------------------------------------------------------------------------
# limited (pure) — switch on config.JDBC_ROW_LIMIT_SYNTAX (read at call time)
# ---------------------------------------------------------------------------


def test_limited_rownum_default(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "rownum")
    assert (
        d.limited("SELECT * FROM t", 100)
        == "SELECT * FROM (SELECT * FROM t) WHERE ROWNUM <= 100"
    )


def test_limited_limit(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "limit")
    assert (
        d.limited("SELECT * FROM t", 100)
        == "SELECT * FROM (SELECT * FROM t) _p LIMIT 100"
    )


def test_limited_top(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "top")
    assert (
        d.limited("SELECT * FROM t", 100)
        == "SELECT TOP (100) * FROM (SELECT * FROM t) _p"
    )


def test_limited_fetch(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "fetch")
    assert (
        d.limited("SELECT * FROM t", 100)
        == "SELECT * FROM (SELECT * FROM t) _p FETCH FIRST 100 ROWS ONLY"
    )


def test_limited_unknown_falls_back_to_rownum(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "wat")
    assert (
        d.limited("SELECT * FROM t", 5)
        == "SELECT * FROM (SELECT * FROM t) WHERE ROWNUM <= 5"
    )


def test_limited_coerces_int(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "rownum")
    assert (
        d.limited("SELECT a FROM t", "50")
        == "SELECT * FROM (SELECT a FROM t) WHERE ROWNUM <= 50"
    )


def test_limited_case_insensitive(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "LIMIT")
    assert (
        d.limited("SELECT * FROM t", 1) == "SELECT * FROM (SELECT * FROM t) _p LIMIT 1"
    )


# ---------------------------------------------------------------------------
# explain (fixed stub)
# ---------------------------------------------------------------------------


def test_explain_is_fixed_stub(d):
    ps = d.explain(None, "SELECT * FROM t WHERE x >= 1 AND x < 2")
    assert isinstance(ps, PlanSummary)
    assert ps.access_path == "(JDBC — no EXPLAIN)"
    assert ps.is_disjoint is True
    assert ps.full_scans == []
    assert ps.raw == ""


# ---------------------------------------------------------------------------
# partitions / sizing / snapshot — fixed best-effort behavior
# ---------------------------------------------------------------------------


def test_is_partitioned_always_false(d):
    assert d.is_partitioned(None, "O", "T") is False


def test_list_partitions_always_empty(d):
    assert d.list_partitions(None, "O", "T") == []
    assert d.list_partitions(None, "O", "T", sub=True) == []


def test_segment_size_bytes_zero(d):
    assert d.segment_size_bytes(None, "O", "T") == 0


def test_snapshot_enable_sql_none(d):
    assert d.snapshot_enable_sql({"label": "x", "value": 1}) is None
    assert d.snapshot_enable_sql(123) is None


def test_snapshot_disable_sql_none(d):
    assert d.snapshot_disable_sql() is None


def test_is_snapshot_lost_false(d):
    assert d.is_snapshot_lost(Exception("anything")) is False


# ---------------------------------------------------------------------------
# _cut_points (pure) — linear division across supported types
# ---------------------------------------------------------------------------


def test_cut_points_int(d):
    assert d._cut_points(0, 100, 4) == [25, 50, 75]


def test_cut_points_n_one_is_empty(d):
    assert d._cut_points(0, 100, 1) == []


def test_cut_points_decimal(d):
    assert d._cut_points(Decimal("0"), Decimal("10"), 2) == [Decimal("5")]


def test_cut_points_date(d):
    cuts = d._cut_points(datetime.date(2024, 1, 1), datetime.date(2024, 1, 11), 2)
    assert cuts == [datetime.date(2024, 1, 6)]


def test_cut_points_datetime(d):
    lo = datetime.datetime(2024, 1, 1, 0, 0, 0)
    hi = datetime.datetime(2024, 1, 1, 0, 0, 10)
    assert d._cut_points(lo, hi, 2) == [datetime.datetime(2024, 1, 1, 0, 0, 5)]


def test_cut_points_str_not_divisible(d):
    assert d._cut_points("a", "z", 4) is None


def test_cut_points_bool_not_divisible(d):
    assert d._cut_points(True, False, 4) is None


def test_cut_points_dedups_tiny_range(d):
    # range of 2 split 4 ways collapses duplicate cut points
    cuts = d._cut_points(0, 2, 4)
    assert cuts == sorted(set(cuts))
    assert all(isinstance(c, int) for c in cuts)


# ---------------------------------------------------------------------------
# _is_range_type / _range_columns (pure) — java.sql.Types introspection
# ---------------------------------------------------------------------------


def test_is_range_type_numeric_int_codes(d):
    assert d._is_range_type(4) is True  # INTEGER
    assert d._is_range_type(3) is True  # DECIMAL
    assert d._is_range_type(93) is True  # TIMESTAMP


def test_is_range_type_string_code_false(d):
    assert d._is_range_type(12) is False  # VARCHAR
    assert d._is_range_type(None) is False
    assert d._is_range_type(True) is False  # bool excluded


def test_is_range_type_name_fallback(d):
    assert d._is_range_type("BIGINT") is True
    assert d._is_range_type("TIMESTAMP") is True
    assert d._is_range_type("VARCHAR") is False


def test_range_columns_picks_numeric_and_date(d):
    # DBAPI description: (name, type_code, ...)
    desc = [
        ("ID", 4, None, None, None, None, None),  # INTEGER -> yes
        ("NAME", 12, None, None, None, None, None),  # VARCHAR -> no
        ("CREATED_DT", 93, None, None, None, None, None),  # TIMESTAMP -> yes
    ]
    assert d._range_columns(desc) == ["ID", "CREATED_DT"]


# ---------------------------------------------------------------------------
# validate_table (fake cursor) — best-effort 1-row probe
# ---------------------------------------------------------------------------


def test_validate_table_success(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "rownum")
    cur = FakeCursor(fetchone_result=(1,))
    conn = FakeConn(cur)
    d.validate_table(conn, "OWNER", "T")  # must not raise
    assert cur.closed is True
    assert "SELECT * FROM OWNER.T" in cur.executed[0]


def test_validate_table_no_owner(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "rownum")
    cur = FakeCursor(fetchone_result=None)
    d.validate_table(FakeConn(cur), None, "T")
    assert "FROM T)" in cur.executed[0] or "FROM T " in cur.executed[0]


def test_validate_table_failure_raises_clear_error(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_ROW_LIMIT_SYNTAX", "rownum")
    cur = FakeCursor(raise_on_execute=RuntimeError("ORA-00942 table does not exist"))
    with pytest.raises(JdbcDialectError) as ei:
        d.validate_table(FakeConn(cur), "O", "MISSING")
    assert "O.MISSING" in str(ei.value)


# ---------------------------------------------------------------------------
# discover_candidates (fake cursor) — column-range only
# ---------------------------------------------------------------------------


def test_discover_candidates_from_metadata(d):
    desc = [
        ("ID", 4, None, None, None, None, None),
        ("NAME", 12, None, None, None, None, None),
        ("CREATED_DT", 93, None, None, None, None, None),
    ]
    cur = FakeCursor(description=desc)
    cands = d.discover_candidates(FakeConn(cur), "O", "T")
    assert [c.kind for c in cands] == ["column", "column"]
    assert [c.key for c in cands] == ["ID", "CREATED_DT"]
    assert cands[0].meta == {"owner": "O", "table": "T"}


def test_discover_candidates_caps_at_two(d):
    desc = [(f"N{i}", 4, None, None, None, None, None) for i in range(5)]
    cands = d.discover_candidates(FakeConn(FakeCursor(description=desc)), "O", "T")
    assert len(cands) == 2


def test_discover_candidates_none_found_returns_empty(d):
    desc = [("NAME", 12, None, None, None, None, None)]
    assert d.discover_candidates(FakeConn(FakeCursor(description=desc)), "O", "T") == []


def test_discover_candidates_introspection_failure_returns_empty(d):
    cur = FakeCursor(raise_on_execute=RuntimeError("no such table"))
    assert d.discover_candidates(FakeConn(cur), "O", "T") == []


# ---------------------------------------------------------------------------
# boundaries (fake cursor) — linear split + IS NULL cover, whole-table fallback
# ---------------------------------------------------------------------------


def test_boundaries_column_linear_split(d):
    cur = FakeCursor(fetchone_result=(0, 100))
    cand = Candidate(kind="column", key="ID", meta={"owner": "O", "table": "T"})
    bnds = d.boundaries(FakeConn(cur), cand, 4)
    # 4 range slices + 1 IS NULL cover
    assert len(bnds) == 5
    assert (bnds[0].lo, bnds[0].hi) == (None, 25)
    assert (bnds[1].lo, bnds[1].hi) == (25, 50)
    assert (bnds[2].lo, bnds[2].hi) == (50, 75)
    assert (bnds[3].lo, bnds[3].hi) == (75, None)
    assert bnds[-1].is_null is True
    assert sum(1 for b in bnds if b.is_null) == 1


def test_boundaries_whole_table_when_min_equals_max(d):
    cur = FakeCursor(fetchone_result=(5, 5))
    cand = Candidate(kind="column", key="ID", meta={"owner": "O", "table": "T"})
    bnds = d.boundaries(FakeConn(cur), cand, 4)
    assert len(bnds) == 1
    assert (bnds[0].lo, bnds[0].hi, bnds[0].is_null) == (None, None, False)
    # renders to the whole-table cover
    assert d.render_predicate(cand, bnds[0]) == "1=1"


def test_boundaries_whole_table_when_empty(d):
    cur = FakeCursor(fetchone_result=(None, None))
    cand = Candidate(kind="column", key="ID", meta={"owner": "O", "table": "T"})
    bnds = d.boundaries(FakeConn(cur), cand, 4)
    assert len(bnds) == 1
    assert (bnds[0].lo, bnds[0].hi) == (None, None)


def test_boundaries_non_divisible_column_falls_back_whole_table(d):
    cur = FakeCursor(fetchone_result=("aaa", "zzz"))
    cand = Candidate(kind="column", key="NAME", meta={"owner": "O", "table": "T"})
    bnds = d.boundaries(FakeConn(cur), cand, 4)
    assert len(bnds) == 1
    assert (bnds[0].lo, bnds[0].hi) == (None, None)


def test_boundaries_rejects_non_column_candidate(d):
    cand = Candidate(kind="physical", key=None, meta={"owner": "O", "table": "T"})
    with pytest.raises(ValueError):
        d.boundaries(FakeConn(FakeCursor()), cand, 4)


# ---------------------------------------------------------------------------
# capture_watermark (fake cursor) — best-effort, never raises
# ---------------------------------------------------------------------------


def test_capture_watermark_no_sql_configured(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_WATERMARK_SQL", "")
    assert d.capture_watermark(FakeConn(FakeCursor())) == {
        "label": "JDBC watermark",
        "value": None,
    }


def test_capture_watermark_runs_configured_sql(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_WATERMARK_SQL", "SELECT current_lsn()")
    cur = FakeCursor(fetchone_result=("LSN-123",))
    assert d.capture_watermark(FakeConn(cur)) == {
        "label": "JDBC watermark",
        "value": "LSN-123",
    }
    assert cur.executed == ["SELECT current_lsn()"]


def test_capture_watermark_query_failure_degrades(d, monkeypatch):
    monkeypatch.setattr(config, "JDBC_WATERMARK_SQL", "SELECT boom")
    cur = FakeCursor(raise_on_execute=RuntimeError("syntax error"))
    assert d.capture_watermark(FakeConn(cur)) == {
        "label": "JDBC watermark",
        "value": None,
    }
