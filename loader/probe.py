"""Engine-agnostic split-predicate probe / bake-off.

Drives everything through a `SourceDialect` (never names Oracle). It races the
candidate split keys discovered by the dialect, EXPLAINs + bounded-times one
representative slice per candidate, ranks them by measured throughput restricted
to disjoint, non-amplifying plans (§6.4), then recommends a chunk count (§6.5)
and — in adaptive depth — a concurrency knee (§6.6). It only RECOMMENDS; it never
writes `queryfile.txt` (§6.8).

Import-safety: NO DB driver (`oracledb`) and NO Striim imports here. The only
engine-specific behavior comes from the injected dialect. The pure functions
(`chunk_count`, `rank_candidates`, `concurrency_knee`, `format_recommendation`)
are unit-tested directly; the DB-driving orchestration is operator/in-env.
"""

import math
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

import config
import query_split
from source_dialect import (  # noqa: F401 - Candidate/PlanSummary/TimingResult used as type hints
    Candidate,
    PlanSummary,
    TimingResult,
    get_dialect,
)

# ===========================================================================
# Result / recommendation dataclasses
# ===========================================================================


@dataclass
class CandidateResult:
    """Everything measured for one candidate split key."""

    candidate: "Candidate"
    plan: Optional["PlanSummary"] = None
    timing: Optional["TimingResult"] = None
    error: Optional[str] = None


@dataclass
class Recommendation:
    strategy: str  # "rowid" | "column"
    key: Optional[str]  # column name for column strategy, else None
    chunk_count: int
    concurrency: Optional[int]  # None => keep configured CONCURRENT_APPS_MAX
    winner_bytes_per_sec: float
    results: list = field(default_factory=list)  # all CandidateResult, ranked
    partition_available: bool = False
    warnings: list = field(default_factory=list)
    parallel_degree: int = 1  # Oracle PARALLEL(n) to inject; 1 => no parallelism
    # [{"degree", "scan_bps", "fetch_bps"}] ascending by degree — the sweep measurements.
    parallel_samples: list = field(default_factory=list)


# ===========================================================================
# Pure functions (unit-tested directly)
# ===========================================================================


def chunk_count(segment_bytes, target_slice_seconds, bytes_per_sec):
    """Slices needed so each runs ~target_slice_seconds at the measured throughput."""
    if bytes_per_sec <= 0 or segment_bytes <= 0:
        return 1
    slice_bytes = target_slice_seconds * bytes_per_sec
    return max(1, math.ceil(segment_bytes / slice_bytes))


def _is_good(result):
    """A candidate is usable iff it timed AND its plan is a disjoint, non-amplifying scan."""
    return bool(
        result.timing is not None
        and result.plan is not None
        and result.plan.is_disjoint
        and not result.plan.full_scans
    )


def rank_candidates(results):
    """Best first: usable candidates by measured bytes/sec; physical breaks ties; failures last."""

    def sort_key(r):
        bps = r.timing.bytes_per_sec if r.timing else 0.0
        is_physical = 1 if r.candidate.kind == "physical" else 0
        return (1 if _is_good(r) else 0, bps, is_physical)

    return sorted(results, key=sort_key, reverse=True)


def concurrency_knee(samples, min_gain=0.10):
    """samples: [(k, aggregate_bytes_per_sec)] ascending by k. Return the largest k whose
    step still improved throughput by >= min_gain (the plateau/knee)."""
    if not samples:
        return 1
    best_k = samples[0][0]
    prev = samples[0][1]
    for k, bps in samples[1:]:
        if prev <= 0:
            break
        if (bps - prev) / prev < min_gain:
            break
        best_k = k
        prev = bps
    else:
        best_k = samples[-1][0]
    return best_k


def _verdict(result):
    """good | AMPLIFYING | failed — for the per-candidate table."""
    if result.timing is None or result.plan is None:
        return "failed"
    if result.plan.full_scans:
        return "AMPLIFYING"
    if not result.plan.is_disjoint:
        return "non-disjoint"
    return "good"


def format_recommendation(rec):
    """Render a readable, greppable recommendation panel (PURE — no I/O)."""
    lines = []
    lines.append("=" * 64)
    lines.append("PROBE RECOMMENDATION")
    lines.append("=" * 64)
    key_disp = rec.key if rec.key else "-"
    lines.append(f"  strategy:        {rec.strategy}")
    lines.append(f"  key:             {key_disp}")
    lines.append(f"  chunk_count:     {rec.chunk_count}")
    lines.append(f"  parallel_degree: {rec.parallel_degree}")
    if rec.concurrency is None:
        lines.append("  concurrency:     keep configured")
    else:
        lines.append(f"  concurrency:     {rec.concurrency}")
    lines.append(f"  winner_MB_per_sec: {rec.winner_bytes_per_sec / 1e6:.2f}")
    lines.append("-" * 64)
    lines.append(
        f"  {'label':<20} {'kind':<9} {'MB/s':>9}  {'access_path':<24} verdict"
    )
    for r in rec.results:
        mb = (r.timing.bytes_per_sec / 1e6) if r.timing else 0.0
        access = (r.plan.access_path if r.plan else "") or "-"
        verdict = _verdict(r)
        if r.error:
            # Surface WHY a candidate failed (e.g. ORA-00933) instead of a silent "failed".
            verdict = f"{verdict}: {r.error.splitlines()[0][:60]}"
        lines.append(
            f"  {r.candidate.label:<20} {r.candidate.kind:<9} {mb:>9.2f}  "
            f"{access:<24} {verdict}"
        )
    lines.append("-" * 64)
    if rec.parallel_samples:
        lines.append("  PARALLEL sweep (avg over runs):")
        lines.append(f"    {'degree':>6}  {'scan_MB/s':>10}  {'fetch_MB/s':>11}")
        for s in rec.parallel_samples:
            lines.append(
                f"    {s['degree']:>6}  {s['scan_bps'] / 1e6:>10.1f}  "
                f"{s['fetch_bps'] / 1e6:>11.1f}"
            )
        if rec.parallel_degree > 1:
            lines.append(
                f"    -> recommend PARALLEL({rec.parallel_degree}) "
                "(end-to-end fetch improved)."
            )
        else:
            lines.append(
                "    -> parallel won't help this load (fetch-bound) — recommend "
                "parallel_degree=1."
            )
        lines.append("-" * 64)
    if rec.partition_available:
        lines.append(
            "  partition: table IS partitioned — consider --strategy partition "
            "(deterministic, no probe needed)."
        )
    if rec.warnings:
        lines.append("  warnings:")
        for w in rec.warnings:
            lines.append(f"    - {w}")
    lines.append("=" * 64)
    return "\n".join(lines)


# ===========================================================================
# DB-driving orchestration (write it; do NOT run it here — operator runs in-env)
# ===========================================================================


def time_slice(dialect, conn, slice_sql, sample_rows, time_budget_seconds):
    """Bounded timing fetch: run dialect.limited(slice_sql, sample_rows), count rows +
    approx bytes, stopping at sample_rows OR when wall-clock hits time_budget_seconds.
    """
    limited_sql = dialect.limited(slice_sql, sample_rows)
    rows = 0
    nbytes = 0
    start = time.time()
    with conn.cursor() as cur:
        cur.execute(limited_sql)
        for row in cur:
            rows += 1
            nbytes += sum(len(str(v)) for v in row if v is not None)
            if rows >= sample_rows:
                break
            if (time.time() - start) >= time_budget_seconds:
                break
    seconds = max(time.time() - start, 1e-6)
    return TimingResult(rows=rows, bytes=nbytes, seconds=seconds)


def evaluate_candidate(
    dialect,
    conn,
    query,
    candidate,
    alias,
    probe_chunks,
    sample_rows,
    time_budget,
):
    """EXPLAIN + bounded-time one representative slice for a candidate. Never raises."""
    try:
        bounds = dialect.boundaries(conn, candidate, probe_chunks)
        if not bounds:
            return CandidateResult(candidate=candidate, error="no boundaries")
        predicate = dialect.render_predicate(candidate, bounds[0], alias=alias)
        slice_sql = query_split.inject_predicate(query, predicate)
        plan = dialect.explain(conn, slice_sql)
        timing = time_slice(dialect, conn, slice_sql, sample_rows, time_budget)
        return CandidateResult(candidate=candidate, plan=plan, timing=timing)
    except Exception as e:  # noqa: BLE001 - bake-off must survive a bad candidate
        return CandidateResult(candidate=candidate, error=str(e))


def concurrency_ramp(
    dialect,
    candidate,
    query,
    alias,
    owner,
    table,
    max_concurrency,
    sample_rows,
    time_budget,
):
    """Replay the winner across k=1,2,4,...<=max_concurrency parallel connections, each on a
    DISTINCT boundary slice, and measure aggregate bytes/sec. Returns [(k, agg_bytes_per_sec)].
    """
    ks = []
    k = 1
    while k <= max_concurrency:
        ks.append(k)
        k *= 2
    if not ks or ks[-1] != max_concurrency:
        # include the ceiling itself if the doubling sequence skipped it
        if max_concurrency >= 1 and (not ks or ks[-1] < max_concurrency):
            ks.append(max_concurrency)

    # Enough distinct boundaries so each of the up-to-max parallel apps gets its own slice.
    max_k = ks[-1] if ks else 1
    probe_conn = dialect.get_connection()
    try:
        bounds = dialect.boundaries(probe_conn, candidate, max_k)
    finally:
        probe_conn.close()
    if not bounds:
        return []

    def measure_one(boundary):
        conn = dialect.get_connection()
        try:
            predicate = dialect.render_predicate(candidate, boundary, alias=alias)
            slice_sql = query_split.inject_predicate(query, predicate)
            return time_slice(dialect, conn, slice_sql, sample_rows, time_budget)
        finally:
            conn.close()

    samples = []
    for k in ks:
        slices = [bounds[i % len(bounds)] for i in range(k)]
        with ThreadPoolExecutor(max_workers=k) as pool:
            timings = list(pool.map(measure_one, slices))
        agg = sum(t.bytes_per_sec for t in timings)
        samples.append((k, agg))
    return samples


def _time_scan(conn, count_sql):
    """Wall-clock a server-side COUNT(*) over the (hinted) slice — a scan-cost proxy.

    Returns (rows, seconds). COUNT forces the row source to be produced with almost no
    client marshalling, so with a PARALLEL hint inside the inline view this reflects
    parallel *scan* speed rather than the fetch. It is a PROXY: Oracle may satisfy the
    count via the cheapest access path, so read it as a trend, not an absolute.
    """
    start = time.time()
    with conn.cursor() as cur:
        cur.execute(count_sql)
        row = cur.fetchone()
    seconds = max(time.time() - start, 1e-6)
    return (row[0] if row else 0), seconds


def sweep_parallel_degrees(
    dialect,
    candidate,
    query,
    alias,
    degrees,
    runs,
    sample_rows,
    time_budget,
    probe_chunks=16,
):
    """Measure server-side scan and end-to-end fetch throughput for the winner slice at
    each PARALLEL degree, averaged over `runs` (first run discarded as warm-up when
    runs>1). Returns [{"degree","scan_bps","fetch_bps"}] ascending by degree, or [] when
    the dialect emits no parallel hint (non-Oracle) or the candidate has no boundaries.
    """
    conn = dialect.get_connection()
    try:
        bounds = dialect.boundaries(conn, candidate, probe_chunks)
        if not bounds:
            return []
        predicate = dialect.render_predicate(candidate, bounds[0], alias=alias)
        base_slice = query_split.inject_predicate(query, predicate)
    finally:
        conn.close()

    samples = []
    warmup = 1 if runs > 1 else 0
    for d in degrees:
        hint = dialect.parallel_hint(d)  # None for d<=1 or unsupported engine
        slice_sql = query_split.inject_hint(base_slice, hint) if hint else base_slice
        count_sql = f"SELECT COUNT(*) FROM ({slice_sql})"
        scan_bps_runs, fetch_bps_runs = [], []
        for r in range(runs + warmup):
            conn = dialect.get_connection()
            try:
                fetch = time_slice(dialect, conn, slice_sql, sample_rows, time_budget)
                rows, scan_secs = _time_scan(conn, count_sql)
            finally:
                conn.close()
            if r < warmup:
                continue  # prime caches, don't record
            fetch_bps_runs.append(fetch.bytes_per_sec)
            # Attribute the fetch run's bytes/row to the counted rows so scan and fetch
            # are both MB/s; falls back to fetch bytes if the sample had no rows.
            bytes_per_row = (fetch.bytes / fetch.rows) if fetch.rows else 0.0
            scan_bps_runs.append((rows * bytes_per_row) / scan_secs)

        def _avg(xs):
            return sum(xs) / len(xs) if xs else 0.0

        samples.append(
            {"degree": d, "scan_bps": _avg(scan_bps_runs), "fetch_bps": _avg(fetch_bps_runs)}
        )
    return samples


def run_probe(
    query,
    owner,
    table,
    alias=None,
    depth="bakeoff",
    dialect=None,
    *,
    target_slice_seconds=None,
    sample_rows=None,
    time_budget_seconds=None,
    max_concurrency=None,
    probe_chunks=16,
    parallel_sweep=False,
    parallel_degrees=None,
    parallel_runs=None,
):
    """Run the bake-off and return a Recommendation. DB-driving — operator runs in-env."""
    # Map config defaults when tunables not passed.
    if target_slice_seconds is None:
        target_slice_seconds = config.PROBE_TARGET_SLICE_SECONDS
    if sample_rows is None:
        sample_rows = config.PROBE_SAMPLE_ROWS
    if time_budget_seconds is None:
        time_budget_seconds = config.PROBE_TIME_BUDGET_SECONDS
    if max_concurrency is None:
        max_concurrency = config.PROBE_MAX_CONCURRENCY
    if parallel_degrees is None:
        parallel_degrees = config.PROBE_PARALLEL_DEGREES
    if parallel_runs is None:
        parallel_runs = config.PROBE_PARALLEL_RUNS

    # Reject a hostile/malformed --alias before it is interpolated into probe SQL.
    alias = query_split.validate_identifier(alias, "alias")

    dialect = dialect or get_dialect()
    conn = dialect.get_connection()
    try:
        dialect.validate_table(conn, owner, table)

        all_candidates = dialect.discover_candidates(conn, owner, table)
        if depth == "lightweight":
            candidates = [c for c in all_candidates if c.kind == "physical"]
        else:
            candidates = all_candidates

        results = [
            evaluate_candidate(
                dialect,
                conn,
                query,
                c,
                alias,
                probe_chunks,
                sample_rows,
                time_budget_seconds,
            )
            for c in candidates
        ]
        ranked = rank_candidates(results)

        warnings = []
        winner = ranked[0]
        if not _is_good(winner):
            # No disjoint plan confirmed — fall back to the physical candidate.
            physical = next(
                (r for r in ranked if r.candidate.kind == "physical"), winner
            )
            winner = physical
            warnings.append(
                "No disjoint, non-amplifying plan was confirmed; defaulting to the "
                "physical (ROWID) candidate — verify the EXPLAIN before running."
            )

        # Chunk count from the winner's measured throughput.
        segment_bytes = dialect.segment_size_bytes(conn, owner, table)
        winner_bps = winner.timing.bytes_per_sec if winner.timing else 0.0
        cc = chunk_count(segment_bytes, target_slice_seconds, winner_bps)

        # Concurrency knee — adaptive depth only.
        concurrency = None
        if depth == "adaptive":
            samples = concurrency_ramp(
                dialect,
                winner.candidate,
                query,
                alias,
                owner,
                table,
                max_concurrency,
                sample_rows,
                time_budget_seconds,
            )
            if samples:
                concurrency = concurrency_knee(samples)

        # Parallel-degree sweep — opt-in, Oracle-only (dialect.parallel_hint gates it).
        # Recommend the fetch-throughput knee: parallel only "wins" if it speeds the
        # end-to-end fetch (what the loader pays), not merely the server-side scan.
        parallel_degree = 1
        parallel_samples = []
        if parallel_sweep and dialect.parallel_hint(2) is not None:
            parallel_samples = sweep_parallel_degrees(
                dialect,
                winner.candidate,
                query,
                alias,
                parallel_degrees,
                parallel_runs,
                sample_rows,
                time_budget_seconds,
                probe_chunks,
            )
            if parallel_samples:
                parallel_degree = concurrency_knee(
                    [(s["degree"], s["fetch_bps"]) for s in parallel_samples]
                )

        # Partition note (deterministic alternative).
        partition_available = dialect.is_partitioned(conn, owner, table)
        if partition_available:
            warnings.append(
                "Table is partitioned — --strategy partition is deterministic "
                "(no probe needed) and may be preferable."
            )

        # Amplification audit: name each candidate that full-scans inner tables.
        for r in results:
            if r.plan is not None and r.plan.full_scans:
                warnings.append(
                    f"AMPLIFYING: candidate {r.candidate.label!r} full-scans "
                    f"{', '.join(r.plan.full_scans)} (join amplification)."
                )

        strategy = "rowid" if winner.candidate.kind == "physical" else "column"
        return Recommendation(
            strategy=strategy,
            key=winner.candidate.key,
            chunk_count=cc,
            concurrency=concurrency,
            winner_bytes_per_sec=winner_bps,
            results=ranked,
            partition_available=partition_available,
            warnings=warnings,
            parallel_degree=parallel_degree,
            parallel_samples=parallel_samples,
        )
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
