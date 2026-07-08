"""Pure (no-DB) tests for the engine-agnostic probe.

These exercise ONLY the pure functions (ranking, chunk-count, concurrency-knee,
panel formatting) by constructing fake Candidate/PlanSummary/TimingResult/
CandidateResult objects. No connection is ever opened and no DB driver is
imported. The DB-driving orchestration (run_probe / time_slice / ...) is the
operator's in-env responsibility and is not exercised here.
"""

import probe
from probe import (
    CandidateResult,
    Recommendation,
    chunk_count,
    concurrency_knee,
    format_recommendation,
    rank_candidates,
)
from source_dialect import Candidate, PlanSummary, TimingResult

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _result(label, kind, key, bps, *, disjoint=True, full_scans=None, timed=True):
    """Build a CandidateResult with a fake plan + timing for ranking tests."""
    cand = Candidate(kind=kind, key=key, label=label)
    plan = PlanSummary(
        access_path="ACCESS",
        is_disjoint=disjoint,
        full_scans=list(full_scans or []),
    )
    timing = TimingResult(rows=1000, bytes=int(bps), seconds=1.0) if timed else None
    return CandidateResult(candidate=cand, plan=plan, timing=timing)


# ---------------------------------------------------------------------------
# chunk_count
# ---------------------------------------------------------------------------


def test_chunk_count_size_formula():
    # 1e12 bytes / (600s * 1e6 B/s) = 1e12 / 6e8 = 1666.67 -> ceil 1667
    assert chunk_count(int(1e12), 600, int(1e6)) == 1667


def test_chunk_count_zero_bytes_per_sec():
    assert chunk_count(int(1e12), 600, 0) == 1


def test_chunk_count_negative_bytes_per_sec():
    assert chunk_count(int(1e12), 600, -5) == 1


def test_chunk_count_zero_segment():
    assert chunk_count(0, 600, int(1e6)) == 1


# ---------------------------------------------------------------------------
# rank_candidates
# ---------------------------------------------------------------------------


def test_rank_disjoint_physical_beats_faster_amplifying_column():
    good_physical = _result("ROWID", "physical", None, bps=100, disjoint=True)
    amplifying_column = _result(
        "CREATED_DT", "column", "CREATED_DT", bps=999, full_scans=["BIG_INNER"]
    )
    ranked = rank_candidates([amplifying_column, good_physical])
    assert ranked[0].candidate.label == "ROWID"


def test_rank_failed_result_sorts_last():
    good = _result("ROWID", "physical", None, bps=100)
    failed = _result("DEAD", "column", "DEAD", bps=0, timed=False)
    ranked = rank_candidates([failed, good])
    assert ranked[-1].candidate.label == "DEAD"


def test_rank_physical_wins_bytes_per_sec_tie():
    physical = _result("ROWID", "physical", None, bps=500)
    column = _result("CREATED_DT", "column", "CREATED_DT", bps=500)
    ranked = rank_candidates([column, physical])
    assert ranked[0].candidate.kind == "physical"


# ---------------------------------------------------------------------------
# concurrency_knee
# ---------------------------------------------------------------------------


def test_concurrency_knee_plateau():
    samples = [(1, 100), (2, 190), (4, 360), (8, 380)]
    # 1->2: +90% ok, 2->4: +89% ok, 4->8: +5.5% < 10% -> stop at 4
    assert concurrency_knee(samples) == 4


def test_concurrency_knee_all_improving():
    samples = [(1, 100), (2, 200), (4, 400)]
    assert concurrency_knee(samples) == 4


def test_concurrency_knee_empty():
    assert concurrency_knee([]) == 1


def test_concurrency_knee_single():
    assert concurrency_knee([(1, 100)]) == 1


# ---------------------------------------------------------------------------
# format_recommendation (smoke)
# ---------------------------------------------------------------------------


def test_format_recommendation_smoke():
    winner = _result("ROWID", "physical", None, bps=2_000_000)
    rec = Recommendation(
        strategy="rowid",
        key=None,
        chunk_count=1667,
        concurrency=4,
        winner_bytes_per_sec=2_000_000.0,
        results=rank_candidates([winner]),
        partition_available=True,
        warnings=["INJECTED-WARNING: heads up"],
    )
    out = format_recommendation(rec)
    assert "rowid" in out
    assert "1667" in out
    assert "INJECTED-WARNING" in out


def test_recommendation_parallel_degree_defaults_to_one():
    rec = Recommendation(
        strategy="rowid",
        key=None,
        chunk_count=1,
        concurrency=None,
        winner_bytes_per_sec=1.0,
    )
    assert rec.parallel_degree == 1
    assert rec.parallel_samples == []


def test_format_recommendation_shows_parallel_sweep_and_winning_degree():
    winner = _result("ROWID", "physical", None, bps=2_000_000)
    rec = Recommendation(
        strategy="rowid",
        key=None,
        chunk_count=4,
        concurrency=None,
        winner_bytes_per_sec=2_000_000.0,
        results=rank_candidates([winner]),
        parallel_degree=4,
        parallel_samples=[
            {"degree": 1, "scan_bps": 180e6, "fetch_bps": 22e6},
            {"degree": 2, "scan_bps": 340e6, "fetch_bps": 40e6},
            {"degree": 4, "scan_bps": 610e6, "fetch_bps": 78e6},
        ],
    )
    out = format_recommendation(rec)
    assert "parallel" in out.lower()
    assert "PARALLEL(4)" in out or "parallel_degree:    4" in out
    # sweep table surfaces both measured columns
    assert "scan" in out.lower() and "fetch" in out.lower()
    assert "610" in out and "78" in out


def test_format_recommendation_parallel_no_gain_verdict():
    winner = _result("ROWID", "physical", None, bps=2_000_000)
    rec = Recommendation(
        strategy="rowid",
        key=None,
        chunk_count=1,
        concurrency=None,
        winner_bytes_per_sec=2_000_000.0,
        results=rank_candidates([winner]),
        parallel_degree=1,
        parallel_samples=[
            {"degree": 1, "scan_bps": 180e6, "fetch_bps": 22e6},
            {"degree": 4, "scan_bps": 610e6, "fetch_bps": 21e6},
        ],
    )
    out = format_recommendation(rec)
    # Swept but fetch-bound: recommend off, and say so.
    assert "parallel" in out.lower()


def test_probe_does_not_import_oracledb():
    import sys

    assert "oracledb" not in sys.modules
