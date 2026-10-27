"""Pure (no-DB, no-psycopg2) tests for postgres_dialect.PostgresDialect.

PostgresDialect is instantiated DIRECTLY (it is not yet registered in
get_dialect). Every DB-calling method is exercised only through fake
connection/cursor objects — no live PostgreSQL and no psycopg2 import are
needed. Only the pure / deterministic surface is asserted: render_predicate,
_format_literal, limited, explain (canned EXPLAIN JSON), boundaries (canned
cursors), and the snapshot overrides.
"""

import datetime
import json
from decimal import Decimal

import pytest

import postgres_boundaries
from postgres_dialect import PostgresDialect
from source_dialect import Boundary, Candidate, PlanSummary


@pytest.fixture
def d():
    return PostgresDialect()


# ---------------------------------------------------------------------------
# fakes — context-manager cursor, no psycopg2 needed
# ---------------------------------------------------------------------------
class FakeCursor:
    def __init__(self, fetchone=None, fetchall=None):
        self._one = fetchone
        self._all = fetchall or []
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._one

    def fetchall(self):
        return list(self._all)

    def close(self):
        pass


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


# ---------------------------------------------------------------------------
# instantiation + class attrs (import-safe, no psycopg2)
# ---------------------------------------------------------------------------
def test_instantiates_and_name():
    d = PostgresDialect()
    assert d.name == "postgres"
    assert d.watermark_label == "PostgreSQL WAL LSN"


# ---------------------------------------------------------------------------
# render_predicate — physical (ctid)
# ---------------------------------------------------------------------------
def test_render_physical_no_alias(d):
    cand = Candidate(kind="physical", key=None, label="ctid")
    b = Boundary(lo=0, hi=25, label="ctid")
    assert (
        d.render_predicate(cand, b) == "ctid >= '(0,0)'::tid AND ctid < '(25,0)'::tid"
    )


def test_render_physical_with_alias(d):
    cand = Candidate(kind="physical", key=None, label="ctid")
    b = Boundary(lo=50, hi=75, label="ctid")
    assert (
        d.render_predicate(cand, b, alias="t")
        == "t.ctid >= '(50,0)'::tid AND t.ctid < '(75,0)'::tid"
    )


def test_render_physical_open_above(d):
    cand = Candidate(kind="physical", key=None, label="ctid")
    b = Boundary(lo=75, hi=None, label="ctid")
    assert d.render_predicate(cand, b) == "ctid >= '(75,0)'::tid"


def test_render_physical_open_below(d):
    cand = Candidate(kind="physical", key=None, label="ctid")
    b = Boundary(lo=None, hi=25, label="ctid")
    assert d.render_predicate(cand, b) == "ctid < '(25,0)'::tid"


# ---------------------------------------------------------------------------
# render_partition_line — declarative child-relation swap
# ---------------------------------------------------------------------------
def test_render_partition_line_swaps_child_relation(d):
    q = "SELECT * FROM public.events e WHERE ~SPLIT~"
    line = d.render_partition_line(q, "public", "events", "events_2024", "public.tgt")
    assert line == "SELECT * FROM public.events_2024 e WHERE 1=1|public.tgt"


def test_render_partition_line_defaults_owner_to_public(d):
    q = "SELECT * FROM public.events WHERE ~SPLIT~"
    line = d.render_partition_line(q, None, "events", "events_p1", "public.tgt")
    assert "public.events_p1" in line
    assert "~SPLIT~" not in line


def test_partition_specs_defaults_to_list_partitions(d, monkeypatch):
    monkeypatch.setattr(
        postgres_boundaries,
        "list_partitions",
        lambda conn, owner, table, sub=False: ["events_2024", "events_2025"],
    )
    assert d.partition_specs(None, "public", "events") == ["events_2024", "events_2025"]


# ---------------------------------------------------------------------------
# render_predicate — column (half-open)
# ---------------------------------------------------------------------------
def test_render_column_int_no_alias(d):
    cand = Candidate(kind="column", key="id", label="id")
    b = Boundary(lo=1000, hi=2000, label="id")
    assert d.render_predicate(cand, b) == "id >= 1000 AND id < 2000"


def test_render_column_alias_qualifies(d):
    cand = Candidate(kind="column", key="id", label="id")
    b = Boundary(lo=1000, hi=2000, label="id")
    assert d.render_predicate(cand, b, alias="s") == "s.id >= 1000 AND s.id < 2000"


def test_render_column_alias_skipped_when_key_already_dotted(d):
    cand = Candidate(kind="column", key="s.created_dt", label="s.created_dt")
    b = Boundary(lo=1, hi=2)
    assert (
        d.render_predicate(cand, b, alias="x")
        == "s.created_dt >= 1 AND s.created_dt < 2"
    )


def test_render_column_date_boundary(d):
    cand = Candidate(kind="column", key="created_dt", label="created_dt")
    b = Boundary(
        lo=datetime.date(2024, 1, 1),
        hi=datetime.date(2024, 2, 1),
        label="created_dt",
    )
    assert (
        d.render_predicate(cand, b, alias="s")
        == "s.created_dt >= '2024-01-01' AND s.created_dt < '2024-02-01'"
    )


def test_render_column_datetime_boundary(d):
    cand = Candidate(kind="column", key="created_dt", label="created_dt")
    b = Boundary(
        lo=datetime.datetime(2024, 1, 1, 0, 0, 0),
        hi=datetime.datetime(2024, 2, 1, 12, 30, 45),
        label="created_dt",
    )
    assert d.render_predicate(cand, b) == (
        "created_dt >= '2024-01-01 00:00:00' " "AND created_dt < '2024-02-01 12:30:45'"
    )


def test_render_column_open_below(d):
    cand = Candidate(kind="column", key="id", label="id")
    b = Boundary(lo=None, hi=1000, label="id")
    assert d.render_predicate(cand, b) == "id < 1000"


def test_render_column_open_above(d):
    cand = Candidate(kind="column", key="id", label="id")
    b = Boundary(lo=2000, hi=None, label="id")
    assert d.render_predicate(cand, b) == "id >= 2000"


def test_render_column_is_null(d):
    cand = Candidate(kind="column", key="id", label="id")
    b = Boundary(is_null=True, label="id")
    assert d.render_predicate(cand, b) == "id IS NULL"


def test_render_column_is_null_alias_qualifies(d):
    cand = Candidate(kind="column", key="id", label="id")
    b = Boundary(is_null=True, label="id")
    assert d.render_predicate(cand, b, alias="s") == "s.id IS NULL"


def test_render_column_all_non_null(d):
    cand = Candidate(kind="column", key="id", label="id")
    b = Boundary(lo=None, hi=None, label="id")
    assert d.render_predicate(cand, b) == "id IS NOT NULL"


def test_render_unknown_kind_raises(d):
    cand = Candidate(kind="hash", key="x", label="x")
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


def test_format_literal_bool(d):
    assert d._format_literal(True) == "TRUE"
    assert d._format_literal(False) == "FALSE"


def test_format_literal_date(d):
    assert d._format_literal(datetime.date(2024, 3, 4)) == "'2024-03-04'"


def test_format_literal_datetime(d):
    v = datetime.datetime(2024, 3, 4, 5, 6, 7)
    assert d._format_literal(v) == "'2024-03-04 05:06:07'"


def test_format_literal_datetime_microseconds(d):
    v = datetime.datetime(2024, 3, 4, 5, 6, 7, 89)
    assert d._format_literal(v) == "'2024-03-04 05:06:07.000089'"


def test_format_literal_str_doubles_quotes(d):
    assert d._format_literal("O'Brien") == "'O''Brien'"


# ---------------------------------------------------------------------------
# limited (pure)
# ---------------------------------------------------------------------------
def test_limited_wraps_subquery(d):
    assert (
        d.limited("SELECT * FROM t", 100)
        == "SELECT * FROM (SELECT * FROM t) _probe LIMIT 100"
    )


def test_limited_coerces_int(d):
    assert (
        d.limited("SELECT a FROM t", "50")
        == "SELECT * FROM (SELECT a FROM t) _probe LIMIT 50"
    )


# ---------------------------------------------------------------------------
# explain — canned EXPLAIN (FORMAT JSON) via a fake cursor
# ---------------------------------------------------------------------------
def test_explain_seq_scan_not_disjoint_and_amplified(d):
    # psycopg2 hands back the json column already deserialized (a Python list).
    plan = [
        {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "big_table",
                "Plan Rows": 5_000_000,
            },
            "Planning Time": 0.1,
        }
    ]
    ps = d.explain(FakeConn(FakeCursor(fetchone=(plan,))), "SELECT * FROM big_table")
    assert isinstance(ps, PlanSummary)
    assert ps.access_path == "Seq Scan"
    assert ps.is_disjoint is False
    assert ps.full_scans == ["big_table"]


def test_explain_index_scan_disjoint_string_json(d):
    # Same payload but delivered as a JSON *string* (the other psycopg2 shape).
    plan = json.dumps(
        [
            {
                "Plan": {
                    "Node Type": "Index Scan",
                    "Relation Name": "t",
                    "Plan Rows": 10,
                    "Plans": [],
                }
            }
        ]
    )
    ps = d.explain(FakeConn(FakeCursor(fetchone=(plan,))), "SELECT ... WHERE id < 5")
    assert ps.access_path == "Index Scan"
    assert ps.is_disjoint is True
    assert ps.full_scans == []


def test_explain_tid_range_scan_disjoint(d):
    plan = [
        {"Plan": {"Node Type": "Tid Range Scan", "Relation Name": "t", "Plan Rows": 7}}
    ]
    ps = d.explain(FakeConn(FakeCursor(fetchone=(plan,))), "SELECT 1")
    assert ps.is_disjoint is True
    assert ps.access_path == "Tid Range Scan"


def test_explain_seq_scan_below_threshold_not_amplified(d):
    plan = [
        {"Plan": {"Node Type": "Seq Scan", "Relation Name": "small", "Plan Rows": 500}}
    ]
    ps = d.explain(FakeConn(FakeCursor(fetchone=(plan,))), "SELECT 1")
    assert ps.full_scans == []
    assert ps.is_disjoint is False


def test_explain_descends_past_limit_wrapper(d):
    # A Limit on top of the driving scan: access path is the scan, not "Limit".
    plan = [
        {
            "Plan": {
                "Node Type": "Limit",
                "Plan Rows": 1,
                "Plans": [
                    {
                        "Node Type": "Index Scan",
                        "Relation Name": "t",
                        "Plan Rows": 3,
                    }
                ],
            }
        }
    ]
    ps = d.explain(FakeConn(FakeCursor(fetchone=(plan,))), "SELECT 1")
    assert ps.access_path == "Index Scan"
    assert ps.is_disjoint is True


def test_explain_never_raises_on_cursor_error(d):
    class Boom(FakeCursor):
        def execute(self, *a, **k):
            raise RuntimeError("boom")

    ps = d.explain(FakeConn(Boom()), "SELECT 1")
    assert isinstance(ps, PlanSummary)
    assert ps.is_disjoint is False


def test_explain_never_raises_on_garbage_json(d):
    ps = d.explain(FakeConn(FakeCursor(fetchone=("not json at all",))), "SELECT 1")
    assert isinstance(ps, PlanSummary)
    assert ps.is_disjoint is False


# ---------------------------------------------------------------------------
# boundaries — physical (ctid) via fake relpages cursor
# ---------------------------------------------------------------------------
def test_boundaries_physical_from_relpages(d):
    # relpages=100, n=4 -> four equal half-open page bands, concrete int bounds.
    cur = FakeCursor(fetchone=(100,))
    cand = Candidate(kind="physical", key=None, meta={"owner": "public", "table": "t"})
    bnds = d.boundaries(FakeConn(cur), cand, 4)
    assert [b.label for b in bnds] == ["ctid"] * 4
    last = postgres_boundaries._UNKNOWN_PAGES  # final band is open above the estimate
    assert [(b.lo, b.hi) for b in bnds] == [(0, 25), (25, 50), (50, 75), (75, last)]
    assert all(b.hi is not None for b in bnds)  # never None (coalesce-safe)
    assert all(b.is_null is False for b in bnds)


def test_boundaries_physical_unknown_relpages_full_band(d):
    cur = FakeCursor(fetchone=(0,))
    cand = Candidate(kind="physical", key=None, meta={"owner": "public", "table": "t"})
    bnds = d.boundaries(FakeConn(cur), cand, 4)
    assert len(bnds) == 1
    assert bnds[0].lo == 0
    assert bnds[0].hi == postgres_boundaries._UNKNOWN_PAGES


def test_boundaries_physical_fewer_pages_than_n_drops_empties(d):
    cur = FakeCursor(fetchone=(2,))
    cand = Candidate(kind="physical", key=None, meta={"owner": "public", "table": "t"})
    bnds = d.boundaries(FakeConn(cur), cand, 4)
    assert [(b.lo, b.hi) for b in bnds] == [(0, 1), (1, postgres_boundaries._UNKNOWN_PAGES)]


def test_boundaries_physical_stale_estimate_still_covers_later_pages(d):
    # A partitioned parent's estimate comes from its largest ANALYZEd leaf (100 pages);
    # an unanalyzed leaf can already hold rows on page 150. Exactly one rendered band
    # must cover that page, so no existing row is left out of every slice.
    cur = FakeCursor(fetchone=(100,))
    cand = Candidate(kind="physical", key=None, meta={"owner": "public", "table": "events"})
    bnds = d.boundaries(FakeConn(cur), cand, 4)
    page = 150
    covering = [b for b in bnds if b.lo <= page < b.hi]
    assert len(covering) == 1
    pred = d.render_predicate(cand, covering[0], alias="e")
    assert pred == "e.ctid >= '(75,0)'::tid AND e.ctid < '(4294967295,0)'::tid"


# ---------------------------------------------------------------------------
# boundaries — column via fake percentile_cont cursor + IS NULL cover
# ---------------------------------------------------------------------------
def test_boundaries_column_from_percentiles(d):
    # percentile_disc(ARRAY[...]) returns one row whose only column is the array.
    cur = FakeCursor(fetchone=([10, 20, 30],))
    cand = Candidate(kind="column", key="id", meta={"owner": "public", "table": "t"})
    bnds = d.boundaries(FakeConn(cur), cand, 4)
    # 4 gap-free ranges + 1 explicit IS NULL cover.
    assert len(bnds) == 5
    assert [(b.lo, b.hi) for b in bnds[:4]] == [
        (None, 10),
        (10, 20),
        (20, 30),
        (30, None),
    ]
    assert bnds[-1].is_null is True
    assert sum(1 for b in bnds if b.is_null) == 1


def test_column_range_boundary_sql_uses_percentile_disc(d):
    """The column-range cut-point SQL must use percentile_disc, not percentile_cont:
    percentile_cont only accepts numeric/interval ORDER BY and errors on a
    date/timestamp split key, whereas percentile_disc works for any sortable type."""
    cur = FakeCursor(fetchone=([10, 20, 30],))
    cand = Candidate(
        kind="column", key="created_dt", meta={"owner": "public", "table": "t"}
    )
    d.boundaries(FakeConn(cur), cand, 4)
    sql_texts = [sql for sql, _ in cur.executed]
    assert any("percentile_disc(" in s for s in sql_texts)
    assert not any("percentile_cont(" in s for s in sql_texts)


def test_boundaries_column_appends_single_is_null_slice(monkeypatch, d):
    monkeypatch.setattr(
        postgres_boundaries,
        "column_range_boundaries",
        lambda conn, o, t, col, n: [(None, 10), (10, 20), (20, None)],
    )
    cand = Candidate(kind="column", key="id", meta={"owner": "public", "table": "t"})
    bnds = d.boundaries(None, cand, 3)
    assert len(bnds) == 4
    assert bnds[-1].is_null is True
    assert sum(1 for b in bnds if b.is_null) == 1


def test_boundaries_unknown_kind_raises(d):
    cand = Candidate(kind="hash", key="x", meta={"owner": "public", "table": "t"})
    with pytest.raises(ValueError):
        d.boundaries(None, cand, 4)


# ---------------------------------------------------------------------------
# segment_size_bytes + capture_watermark via fake cursors
# ---------------------------------------------------------------------------
def test_segment_size_bytes(d):
    cur = FakeCursor(fetchone=(8_192_000,))
    assert d.segment_size_bytes(FakeConn(cur), "public", "t") == 8_192_000


def test_capture_watermark_value_and_label(d):
    cur = FakeCursor(fetchone=("0/16B6360",))
    wm = d.capture_watermark(FakeConn(cur))
    assert wm["label"] == "PostgreSQL WAL LSN"
    assert wm["value"] == "0/16B6360"


def test_capture_watermark_never_raises(d):
    class Boom(FakeCursor):
        def execute(self, *a, **k):
            raise RuntimeError("no perms")

    wm = d.capture_watermark(FakeConn(Boom()))
    assert wm == {"label": "PostgreSQL WAL LSN", "value": None}


# ---------------------------------------------------------------------------
# snapshot overrides (pure)
# ---------------------------------------------------------------------------
def test_is_snapshot_lost_always_false(d):
    assert d.is_snapshot_lost(Exception("anything")) is False


def test_snapshot_disable_sql_none(d):
    assert d.snapshot_disable_sql() is None


def test_snapshot_enable_sql_none_without_snapshot(d):
    assert d.snapshot_enable_sql({"label": "x", "value": "0/16B6360"}) is None
    assert d.snapshot_enable_sql("0/16B6360") is None
    assert d.snapshot_enable_sql(None) is None


def test_snapshot_enable_sql_with_snapshot(d):
    wm = {"value": "0/16B6360", "snapshot": "00000003-00001B92-1"}
    assert d.snapshot_enable_sql(wm) == (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; "
        "SET TRANSACTION SNAPSHOT '00000003-00001B92-1'"
    )


def test_snapshot_enable_sql_rejects_injection(d):
    assert d.snapshot_enable_sql({"snapshot": "x'; DROP TABLE foo;--"}) is None


# ---------------------------------------------------------------------------
# _summarize_plan + helpers (pure, direct)
# ---------------------------------------------------------------------------
def test_is_disjoint_node_classification(d):
    assert d._is_disjoint_node("Seq Scan") is False
    assert d._is_disjoint_node("Index Scan") is True
    assert d._is_disjoint_node("Index Only Scan") is True
    assert d._is_disjoint_node("Tid Range Scan") is True
    assert d._is_disjoint_node("") is False


def test_rank_split_columns_orders_by_distinctness(d):
    rows = [
        {"column_name": "low", "n_distinct": 2},
        {"column_name": "unknown", "n_distinct": 1},
        {"column_name": "high", "n_distinct": 9000},
        {"column_name": "frac_unique", "n_distinct": -1.0},
        {"column_name": "null_stat", "n_distinct": None},
    ]
    ranked = postgres_boundaries._rank_split_columns(rows)
    # fraction-based (-1.0 == unique) ranks highest, then 9000, then 2;
    # n_distinct 1 / NULL are dropped.
    assert ranked == ["frac_unique", "high", "low"]
