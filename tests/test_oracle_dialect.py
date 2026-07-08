"""Pure (no-DB, no-oracledb) tests for oracle_dialect.OracleDialect.

Every DB-calling method (get_connection, validate_table, boundaries,
discover_candidates, capture_watermark, explain, segment_size_bytes,
is_partitioned, list_partitions) is operator / in-env integration only and is
NOT exercised here — these tests never open a connection. Only the pure surface
(render_predicate, _format_literal, _summarize_plan, limited) is covered.
"""

import datetime
from decimal import Decimal

import pytest
import oracle_boundaries
from oracle_dialect import OracleDialect
from source_dialect import Boundary, Candidate, PlanSummary


@pytest.fixture
def d():
    return OracleDialect()


# ---------------------------------------------------------------------------
# instantiation + class attrs (import-safe, no oracledb)
# ---------------------------------------------------------------------------


def test_instantiates_and_name():
    d = OracleDialect()
    assert d.name == "oracle"
    assert d.watermark_label == "Oracle SCN"


# ---------------------------------------------------------------------------
# render_predicate — physical (ROWID)
# ---------------------------------------------------------------------------


def test_render_physical_no_alias(d):
    cand = Candidate(kind="physical", key=None, label="ROWID")
    b = Boundary(lo="AAA", hi="ZZZ", label="ROWID")
    assert d.render_predicate(cand, b) == "ROWID BETWEEN 'AAA' AND 'ZZZ'"


def test_render_physical_with_alias(d):
    cand = Candidate(kind="physical", key=None, label="ROWID")
    b = Boundary(lo="AAA", hi="ZZZ", label="ROWID")
    assert d.render_predicate(cand, b, alias="t") == "t.ROWID BETWEEN 'AAA' AND 'ZZZ'"


def test_render_physical_open_lower_tail(d):
    """First slice: no lower bound so rows below the min ROWID are still captured."""
    cand = Candidate(kind="physical", key=None, label="ROWID")
    b = Boundary(lo=None, hi="ZZZ", label="ROWID")
    assert d.render_predicate(cand, b, alias="t") == "t.ROWID <= 'ZZZ'"


def test_render_physical_open_upper_tail(d):
    """Last slice: no upper bound so rows above the max ROWID are still captured."""
    cand = Candidate(kind="physical", key=None, label="ROWID")
    b = Boundary(lo="AAA", hi=None, label="ROWID")
    assert d.render_predicate(cand, b) == "ROWID >= 'AAA'"


def test_render_physical_both_tails_open(d):
    """Single slice: both tails open => whole-table predicate."""
    cand = Candidate(kind="physical", key=None, label="ROWID")
    b = Boundary(lo=None, hi=None, label="ROWID")
    assert d.render_predicate(cand, b) == "1=1"


def test_oracle_opts_into_open_physical_tails(d):
    assert d.open_physical_tails is True


# ---------------------------------------------------------------------------
# render_predicate — column (half-open)
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
    # key already contains "." -> alias must NOT be re-applied
    assert (
        d.render_predicate(cand, b, alias="x")
        == "s.CREATED_DT >= 1 AND s.CREATED_DT < 2"
    )


def test_render_column_datetime_boundary_to_date(d):
    cand = Candidate(kind="column", key="CREATED_DT", label="CREATED_DT")
    b = Boundary(
        lo=datetime.datetime(2024, 1, 1, 0, 0, 0),
        hi=datetime.datetime(2024, 2, 1, 12, 30, 45),
        label="CREATED_DT",
    )
    out = d.render_predicate(cand, b, alias="s")
    assert out == (
        "s.CREATED_DT >= TO_DATE('2024-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') "
        "AND s.CREATED_DT < TO_DATE('2024-02-01 12:30:45','YYYY-MM-DD HH24:MI:SS')"
    )


def test_render_column_open_below(d):
    # first slice: lo=None -> only an upper bound
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=None, hi=1000, label="ID")
    assert d.render_predicate(cand, b) == "ID < 1000"


def test_render_column_open_above(d):
    # last slice: hi=None -> only a lower bound
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=2000, hi=None, label="ID")
    assert d.render_predicate(cand, b) == "ID >= 2000"


def test_render_column_is_null(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(is_null=True, label="ID")
    assert d.render_predicate(cand, b) == "ID IS NULL"


def test_render_column_all_non_null(d):
    # single slice (no cuts): lo=None and hi=None -> IS NOT NULL
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(lo=None, hi=None, label="ID")
    assert d.render_predicate(cand, b) == "ID IS NOT NULL"


def test_render_column_is_null_alias_qualifies(d):
    cand = Candidate(kind="column", key="ID", label="ID")
    b = Boundary(is_null=True, label="ID")
    assert d.render_predicate(cand, b, alias="s") == "s.ID IS NULL"


def test_render_column_datetime_microseconds_to_timestamp(d):
    cand = Candidate(kind="column", key="CREATED_DT", label="CREATED_DT")
    b = Boundary(
        lo=datetime.datetime(2024, 1, 1, 0, 0, 0, 123456),
        hi=None,
        label="CREATED_DT",
    )
    out = d.render_predicate(cand, b, alias="s")
    assert out == (
        "s.CREATED_DT >= "
        "TO_TIMESTAMP('2024-01-01 00:00:00.123456','YYYY-MM-DD HH24:MI:SS.FF6')"
    )


def test_render_unknown_kind_raises(d):
    cand = Candidate(kind="hash", key="X", label="X")
    with pytest.raises(ValueError):
        d.render_predicate(cand, Boundary(lo=1, hi=2))


# ---------------------------------------------------------------------------
# _format_literal (pure)
# ---------------------------------------------------------------------------


def test_format_literal_int(d):
    assert d._format_literal(42) == "42"


def test_format_literal_float(d):
    assert d._format_literal(1.5) == "1.5"


def test_format_literal_decimal(d):
    assert d._format_literal(Decimal("3.14")) == "3.14"


def test_format_literal_datetime(d):
    v = datetime.datetime(2024, 3, 4, 5, 6, 7)
    assert (
        d._format_literal(v) == "TO_DATE('2024-03-04 05:06:07','YYYY-MM-DD HH24:MI:SS')"
    )


def test_format_literal_datetime_microseconds(d):
    v = datetime.datetime(2024, 3, 4, 5, 6, 7, 89)
    assert d._format_literal(v) == (
        "TO_TIMESTAMP('2024-03-04 05:06:07.000089','YYYY-MM-DD HH24:MI:SS.FF6')"
    )


def test_format_literal_date(d):
    v = datetime.date(2024, 3, 4)
    assert d._format_literal(v) == "TO_DATE('2024-03-04','YYYY-MM-DD')"


def test_format_literal_str_doubles_quotes(d):
    assert d._format_literal("O'Brien") == "'O''Brien'"


# ---------------------------------------------------------------------------
# _summarize_plan (pure) — rows are (id, operation, options, object_name, cardinality)
# ---------------------------------------------------------------------------


def test_summarize_plan_rowid_range_disjoint(d):
    rows = [
        (0, "SELECT STATEMENT", None, None, None),
        (1, "TABLE ACCESS", "BY ROWID RANGE", "BIG_TABLE", 5000),
    ]
    ps = d._summarize_plan(rows)
    assert isinstance(ps, PlanSummary)
    assert ps.access_path == "TABLE ACCESS BY ROWID RANGE"
    assert ps.is_disjoint is True
    assert ps.full_scans == []
    assert "TABLE ACCESS BY ROWID RANGE BIG_TABLE" in ps.raw


def test_summarize_plan_full_scan_inner(d):
    rows = [
        (0, "SELECT STATEMENT", None, None, None),
        (1, "NESTED LOOPS", None, None, None),
        (2, "TABLE ACCESS", "BY ROWID RANGE", "DRIVING", 100_000),
        (3, "TABLE ACCESS", "FULL", "LOOKUP", 5_000_000),
    ]
    ps = d._summarize_plan(rows)
    # access_path is the FIRST table access (the disjoint driving one)
    assert ps.access_path == "TABLE ACCESS BY ROWID RANGE"
    assert ps.full_scans == ["LOOKUP"]
    assert ps.is_disjoint is True


def test_summarize_plan_fallback_first_operation(d):
    rows = [(0, "SELECT STATEMENT", None, None, None)]
    ps = d._summarize_plan(rows)
    assert ps.access_path == "SELECT STATEMENT"
    assert ps.is_disjoint is False
    assert ps.full_scans == []


def test_summarize_plan_small_full_scan_below_threshold_ignored(d):
    # A FULL scan of a tiny lookup table (card below 1e6) is NOT amplification.
    rows = [
        (0, "SELECT STATEMENT", None, None, None),
        (1, "NESTED LOOPS", None, None, None),
        (2, "TABLE ACCESS", "BY ROWID RANGE", "DRIVING", 100_000),
        (3, "TABLE ACCESS", "FULL", "SMALL_LOOKUP", 500_000),
    ]
    ps = d._summarize_plan(rows)
    assert "SMALL_LOOKUP" not in ps.full_scans
    assert ps.full_scans == []


def test_summarize_plan_large_full_scan_above_threshold_flagged(d):
    # A FULL scan above 1e6 rows IS flagged as amplification.
    rows = [
        (0, "SELECT STATEMENT", None, None, None),
        (1, "TABLE ACCESS", "FULL", "HUGE_LOOKUP", 2_000_000),
    ]
    ps = d._summarize_plan(rows)
    assert ps.full_scans == ["HUGE_LOOKUP"]


# ---------------------------------------------------------------------------
# limited (pure)
# ---------------------------------------------------------------------------


def test_limited_wraps_subquery(d):
    assert (
        d.limited("SELECT * FROM t", 100)
        == "SELECT * FROM (SELECT * FROM t) WHERE ROWNUM <= 100"
    )


def test_limited_coerces_int(d):
    assert (
        d.limited("SELECT a FROM t", "50")
        == "SELECT * FROM (SELECT a FROM t) WHERE ROWNUM <= 50"
    )


# ---------------------------------------------------------------------------
# _format_literal — tz-aware datetime → TO_TIMESTAMP_TZ
# ---------------------------------------------------------------------------


def test_format_literal_tz_aware_datetime(d):
    v = datetime.datetime(
        2024,
        3,
        4,
        5,
        6,
        7,
        123456,
        tzinfo=datetime.timezone(datetime.timedelta(hours=-8)),
    )
    assert d._format_literal(v) == (
        "TO_TIMESTAMP_TZ('2024-03-04 05:06:07.123456 -08:00',"
        "'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')"
    )


def test_format_literal_tz_aware_positive_offset(d):
    v = datetime.datetime(
        2024,
        3,
        4,
        5,
        6,
        7,
        0,
        tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
    )
    assert d._format_literal(v) == (
        "TO_TIMESTAMP_TZ('2024-03-04 05:06:07.000000 +05:30',"
        "'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')"
    )


def test_format_literal_naive_datetime_still_to_date(d):
    v = datetime.datetime(2024, 3, 4, 5, 6, 7)
    assert (
        d._format_literal(v) == "TO_DATE('2024-03-04 05:06:07','YYYY-MM-DD HH24:MI:SS')"
    )


# ---------------------------------------------------------------------------
# regression pins — boundaries() IS-NULL cover + ROWID label invariants
# ---------------------------------------------------------------------------


def test_boundaries_column_appends_single_is_null_slice(monkeypatch, d):
    monkeypatch.setattr(
        oracle_boundaries,
        "column_range_boundaries",
        lambda conn, o, t, col, n: [(None, 10), (10, 20), (20, None)],
    )
    cand = Candidate(kind="column", key="ID", meta={"owner": "O", "table": "T"})
    bnds = d.boundaries(None, cand, 3)
    assert len(bnds) == 4
    assert bnds[-1].is_null is True
    assert sum(1 for b in bnds if b.is_null) == 1


def test_boundaries_physical_rowid_labels(monkeypatch, d):
    monkeypatch.setattr(
        oracle_boundaries,
        "rowid_ranges",
        lambda conn, o, t, n: [("AAAlo", "AAAhi"), ("BBBlo", "BBBhi")],
    )
    cand = Candidate(kind="physical", key=None, meta={"owner": "O", "table": "T"})
    bnds = d.boundaries(None, cand, 2)
    assert [b.label for b in bnds] == ["ROWID", "ROWID"]
    assert (bnds[0].lo, bnds[0].hi) == ("AAAlo", "AAAhi")
    assert all(b.is_null is False for b in bnds)


# ---------------------------------------------------------------------------
# engine-aware reconcile — Oracle snapshot/identifier overrides (pure)
# ---------------------------------------------------------------------------


def test_snapshot_enable_sql_from_scn_value(d):
    assert (
        d.snapshot_enable_sql(123)
        == "BEGIN DBMS_FLASHBACK.ENABLE_AT_SYSTEM_CHANGE_NUMBER(123); END;"
    )


def test_snapshot_enable_sql_from_watermark_dict(d):
    assert (
        d.snapshot_enable_sql({"label": "Oracle SCN", "value": 998877})
        == "BEGIN DBMS_FLASHBACK.ENABLE_AT_SYSTEM_CHANGE_NUMBER(998877); END;"
    )


def test_snapshot_enable_sql_falsy_scn_returns_none(d):
    assert d.snapshot_enable_sql({"value": None}) is None
    assert d.snapshot_enable_sql(0) is None


def test_snapshot_disable_sql(d):
    assert d.snapshot_disable_sql() == "BEGIN DBMS_FLASHBACK.DISABLE; END;"


def test_is_snapshot_lost_by_string(d):
    assert d.is_snapshot_lost(Exception("ORA-01555: snapshot too old")) is True


def test_is_snapshot_lost_false_for_other(d):
    assert d.is_snapshot_lost(Exception("ORA-00942: table not found")) is False


def test_parse_owner_table_uppercases(d):
    # Oracle delegates to oracle_boundaries: upper-cases and allows $ / #.
    assert d.parse_owner_table("pay.cm_fb") == ("PAY", "CM_FB")


def test_parallel_hint_emits_oracle_hint(d):
    assert d.parallel_hint(4) == "PARALLEL(4)"
    assert d.parallel_hint(8) == "PARALLEL(8)"


def test_parallel_hint_none_for_degree_one_or_less(d):
    # Degree 1 (or 0/None) means "no parallelism" — no hint to inject.
    assert d.parallel_hint(1) is None
    assert d.parallel_hint(0) is None
    assert d.parallel_hint(None) is None
