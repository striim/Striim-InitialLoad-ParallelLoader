import pytest
import split_runner as sr
from source_dialect import Boundary, Candidate  # noqa: F401


def test_resolve_strategy_auto_partitioned():
    assert sr.resolve_strategy("auto", True) == "partition"


def test_resolve_strategy_auto_not_partitioned():
    assert sr.resolve_strategy("auto", False) == "rowid"


def test_resolve_strategy_partition_on_unpartitioned_raises():
    with pytest.raises(sr.SplitRunError):
        sr.resolve_strategy("partition", False)


def test_resolve_strategy_rowid_forced():
    assert sr.resolve_strategy("rowid", True) == "rowid"


def test_build_lines_rowid():
    """Oracle opens the tails: first slice has no lower bound, last has no upper
    bound, middle slices stay closed BETWEEN — so no row outside [lo, hi] is missed."""
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    lines = sr.build_lines(
        q,
        "SALES",
        "T",
        "t",
        "rowid",
        [("lo1", "hi1"), ("lo2", "hi2"), ("lo3", "hi3")],
        "SALES.TGT",
        False,
    )
    assert len(lines) == 3
    assert "t.ROWID <= 'hi1'|SALES.TGT" in lines[0]
    assert "t.ROWID BETWEEN 'lo2' AND 'hi2'|SALES.TGT" in lines[1]
    assert "t.ROWID >= 'lo3'|SALES.TGT" in lines[2]


def test_build_lines_rowid_single_slice_covers_all():
    """One ROWID slice => whole-table predicate (both tails open) so nothing is missed."""
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    lines = sr.build_lines(
        q, "SALES", "T", "t", "rowid", [("lo1", "hi1")], "SALES.TGT", False
    )
    assert len(lines) == 1
    assert lines[0] == "SELECT * FROM SALES.T t WHERE 1=1|SALES.TGT"


def test_build_lines_partition():
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    lines = sr.build_lines(
        q, "SALES", "T", "t", "partition", ["P1", "P2"], "SALES.TGT", False
    )
    assert len(lines) == 2
    assert "SALES.T PARTITION (P1) t" in lines[0]


# --- column strategy tests (pure: no DB) ---


def test_build_lines_column_basic():
    """Column strategy: complete gap-free cover (open ends + IS NULL), ~SPLIT~ replaced."""
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    boundaries = [
        Boundary(lo=None, hi=1000),
        Boundary(lo=1000, hi=2000),
        Boundary(lo=2000, hi=None),
        Boundary(is_null=True),
    ]
    lines = sr.build_lines(
        q, "SALES", "T", None, "column", boundaries, "SALES.TGT", False, column="CREATED_ID"
    )
    assert len(lines) == 4
    # open-below first slice
    assert "CREATED_ID < 1000" in lines[0]
    # internal half-open slice
    assert "CREATED_ID >= 1000 AND CREATED_ID < 2000" in lines[1]
    # open-above last range slice
    assert "CREATED_ID >= 2000" in lines[2]
    # explicit NULL cover so no rows are dropped
    assert "CREATED_ID IS NULL" in lines[3]
    for ln in lines:
        assert ln.endswith("|SALES.TGT")
        assert "~SPLIT~" not in ln


def test_build_lines_column_alias_qualified():
    """Column strategy with alias: predicate uses alias.COLUMN."""
    q = "SELECT * FROM SALES.T s WHERE ~SPLIT~"
    boundaries = [Boundary(lo=0, hi=500), Boundary(is_null=True)]
    lines = sr.build_lines(
        q, "SALES", "T", "s", "column", boundaries, "SALES.TGT", False, column="CREATED_ID"
    )
    assert len(lines) == 2
    assert "s.CREATED_ID >= 0 AND s.CREATED_ID < 500" in lines[0]
    assert lines[0].endswith("|SALES.TGT")
    assert "s.CREATED_ID IS NULL" in lines[1]


def test_resolve_strategy_column_not_partitioned():
    assert sr.resolve_strategy("column", False) == "column"


def test_resolve_strategy_column_partitioned():
    """column strategy is always honored regardless of partition status."""
    assert sr.resolve_strategy("column", True) == "column"


# --- physical coalesce gating (per-dialect coalesce_physical_ranges flag) ---


class _FakePhysicalDialect:
    """Minimal dialect for build_lines' physical path: a coalesce flag plus a
    half-open predicate renderer. Lets us prove the gating without a real engine."""

    def __init__(self, coalesce):
        self.coalesce_physical_ranges = coalesce

    def render_predicate(self, candidate, boundary, alias=None):
        col = f"{alias}.k" if alias else "k"
        return f"{col} >= {boundary.lo} AND {col} < {boundary.hi}"


def test_build_lines_physical_half_open_not_coalesced():
    """Half-open dialect (coalesce_physical_ranges=False): N adjacent gap-free
    bands (lo_next == hi_prev) stay N separate queryfile lines (no merge)."""
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    ranges = [(0, 10), (10, 20), (20, 30)]
    lines = sr.build_lines(
        q,
        "SALES",
        "T",
        "t",
        "rowid",
        ranges,
        "SALES.TGT",
        False,
        dialect=_FakePhysicalDialect(coalesce=False),
    )
    assert len(lines) == 3
    assert "t.k >= 0 AND t.k < 10|SALES.TGT" in lines[0]
    assert "t.k >= 10 AND t.k < 20|SALES.TGT" in lines[1]
    assert "t.k >= 20 AND t.k < 30|SALES.TGT" in lines[2]


def test_build_lines_physical_inclusive_still_coalesced():
    """Oracle-like dialect (coalesce_physical_ranges=True): the same adjacent
    ranges still merge into one slice (existing ROWID behavior is unchanged)."""
    q = "SELECT * FROM SALES.T t WHERE ~SPLIT~"
    ranges = [(0, 10), (10, 20), (20, 30)]
    lines = sr.build_lines(
        q,
        "SALES",
        "T",
        "t",
        "rowid",
        ranges,
        "SALES.TGT",
        False,
        dialect=_FakePhysicalDialect(coalesce=True),
    )
    assert len(lines) == 1
    assert "t.k >= 0 AND t.k < 30|SALES.TGT" in lines[0]
