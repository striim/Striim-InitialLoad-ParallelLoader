"""Pure (no-DB) tests for source_dialect module."""

import pytest
from source_dialect import (
    Boundary,
    Candidate,
    PlanSummary,
    SourceDialect,
    TimingResult,
    get_dialect,
)

# ---------------------------------------------------------------------------
# get_dialect: routes each engine alias to its dialect (drivers are lazy, so
# instantiation succeeds without psycopg2/pyodbc/jaydebeapi installed)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,expected_cls,expected_engine",
    [
        ("postgres", "PostgresDialect", "postgres"),
        ("pg", "PostgresDialect", "postgres"),
        ("postgresql", "PostgresDialect", "postgres"),
        ("sqlserver", "SqlServerDialect", "sqlserver"),
        ("mssql", "SqlServerDialect", "sqlserver"),
        ("jdbc", "JdbcDialect", "jdbc"),
        ("oracle", "OracleDialect", "oracle"),
    ],
)
def test_get_dialect_routes_engine(name, expected_cls, expected_engine):
    d = get_dialect(name)
    assert type(d).__name__ == expected_cls
    assert d.name == expected_engine


def test_unknown_engine_raises_value_error():
    with pytest.raises(ValueError):
        get_dialect("mysql")


# ---------------------------------------------------------------------------
# SourceDialect is abstract — cannot be instantiated directly
# ---------------------------------------------------------------------------


def test_source_dialect_is_abstract():
    with pytest.raises(TypeError):
        SourceDialect()


# ---------------------------------------------------------------------------
# Dataclass construction
# ---------------------------------------------------------------------------


def test_candidate_construct():
    c = Candidate("physical")
    assert c.kind == "physical"
    assert c.key is None
    assert c.label == ""
    assert c.meta == {}


def test_boundary_construct():
    b = Boundary(lo="a", hi="b")
    assert b.lo == "a"
    assert b.hi == "b"
    assert b.name is None


def test_plan_summary_construct():
    ps = PlanSummary("X", True)
    assert ps.access_path == "X"
    assert ps.is_disjoint is True
    assert ps.full_scans == []
    assert ps.raw == ""


# ---------------------------------------------------------------------------
# TimingResult computed properties
# ---------------------------------------------------------------------------


def test_timing_result_rows_per_sec():
    t = TimingResult(rows=1000, bytes=2000, seconds=2.0)
    assert t.rows_per_sec == 500.0


def test_timing_result_bytes_per_sec():
    t = TimingResult(rows=1000, bytes=2000, seconds=2.0)
    assert t.bytes_per_sec == 1000.0


def test_timing_result_zero_seconds_no_zerodiv():
    t = TimingResult(rows=0, bytes=0, seconds=0)
    assert t.rows_per_sec == 0.0
    assert t.bytes_per_sec == 0.0


# ---------------------------------------------------------------------------
# Engine-aware reconcile — concrete base defaults
# ---------------------------------------------------------------------------


class _StubDialect(SourceDialect):
    """Minimal concrete dialect implementing the abstract surface as no-ops, so
    the concrete base defaults (parse_owner_table / snapshot_* / is_snapshot_lost)
    can be exercised without a real engine."""

    def get_connection(self):  # pragma: no cover - not called
        raise NotImplementedError

    def validate_table(self, conn, owner, table):  # pragma: no cover
        ...

    def is_partitioned(self, conn, owner, table):  # pragma: no cover
        return False

    def list_partitions(self, conn, owner, table, sub=False):  # pragma: no cover
        return []

    def discover_candidates(self, conn, owner, table):  # pragma: no cover
        return []

    def boundaries(self, conn, candidate, n):  # pragma: no cover
        return []

    def render_predicate(self, candidate, boundary, alias=None):  # pragma: no cover
        return ""

    def segment_size_bytes(self, conn, owner, table):  # pragma: no cover
        return 0

    def capture_watermark(self, conn):  # pragma: no cover
        return {}

    def explain(self, conn, sql):  # pragma: no cover
        return None

    def limited(self, sql, n):  # pragma: no cover
        return sql


def test_base_snapshot_enable_default_none():
    assert _StubDialect().snapshot_enable_sql({"value": 123}) is None


def test_base_snapshot_disable_default_none():
    assert _StubDialect().snapshot_disable_sql() is None


def test_base_is_snapshot_lost_default_false():
    assert _StubDialect().is_snapshot_lost(Exception("ORA-01555 x")) is False


def test_base_parse_owner_table_generic():
    # generic default: split on the last '.', no upper-casing
    assert _StubDialect().parse_owner_table("pay.cm_fb") == ("pay", "cm_fb")
    assert _StubDialect().parse_owner_table("CM_FB") == (None, "CM_FB")


def test_base_parse_owner_table_rejects_bad_ident():
    with pytest.raises(ValueError):
        _StubDialect().parse_owner_table("bad-name.t")


def test_base_parallel_hint_returns_none():
    # Engines with no inline-hint concept inherit the base default: no parallel hint.
    assert _StubDialect().parallel_hint(4) is None
    assert _StubDialect().parallel_hint(1) is None
