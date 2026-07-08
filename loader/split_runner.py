"""Orchestrates `manage.py split`: discover boundaries -> render -> write queryfile."""

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

import config
import query_split as qs
from source_dialect import Boundary, Candidate, get_dialect


class SplitRunError(Exception):
    pass


def resolve_strategy(requested, partitioned):
    if requested == "auto":
        return "partition" if partitioned else "rowid"
    if requested == "partition" and not partitioned:
        raise SplitRunError(
            "--strategy partition requested but the table is not partitioned"
        )
    if requested == "column":
        return "column"
    return requested


def load_query(args):
    if args.query_file:
        with open(args.query_file) as f:
            return f.read().strip()
    if args.query:
        return args.query.strip()
    raise SplitRunError("Provide --query-file or --query")


def write_split_watermark(
    log_path, watermark, queryfile, table, default_label="Oracle SCN"
):
    """Write <logdir>/split_watermark.json (the split-time watermark). Returns path or None. Never raises."""
    value = (watermark or {}).get("value")
    if value in (None, ""):
        return None
    d = os.path.dirname(log_path) or "."
    path = os.path.join(d, "split_watermark.json")
    payload = {
        "label": (watermark or {}).get("label", default_label),
        "value": str(value),
        "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "queryfile": queryfile,
        "table": table,
    }
    try:
        os.makedirs(d, exist_ok=True)
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        return None
    return path


def build_lines(
    query,
    owner,
    table,
    alias,
    strategy,
    boundaries,
    target,
    sub,
    column=None,
    dialect=None,
):
    if dialect is None:
        dialect = get_dialect()
    lines = []
    if strategy == "partition":
        # Each engine renders a partition slice its own way (Oracle TABLE PARTITION,
        # PostgreSQL child-relation swap, SQL Server $PARTITION predicate). The spec
        # objects come from dialect.partition_specs (see run_split).
        for spec in boundaries:
            lines.append(
                dialect.render_partition_line(
                    query, owner, table, spec, target, alias=alias, sub=sub
                )
            )
    elif strategy == "column":
        cand = Candidate(
            kind="column", key=column, meta={"owner": owner, "table": table}
        )
        for b in boundaries:
            pred = dialect.render_predicate(cand, b, alias=alias)
            lines.append(f"{qs.inject_predicate(query, pred)}|{target}")
    else:
        cand = Candidate(
            kind="physical", key=None, meta={"owner": owner, "table": table}
        )
        # Oracle ROWID ranges are inclusive -> coalesce adjacent/overlapping pieces.
        # Half-open dialects (ctid / clustered-key, lo_next == hi_prev) must NOT coalesce
        # or every adjacent slice merges into one (parallelism collapses to one slice).
        ranges = (
            qs.coalesce_ranges(boundaries)
            if getattr(dialect, "coalesce_physical_ranges", True)
            else boundaries
        )
        # Open the first slice's lower bound and the last slice's upper bound (when the
        # dialect opts in) so rows below the min / above the max boundary are captured.
        open_tails = getattr(dialect, "open_physical_tails", False)
        last = len(ranges) - 1
        for idx, (lo, hi) in enumerate(ranges):
            if alias is None and re.search(r"\bJOIN\b", query, re.IGNORECASE):
                raise qs.SplitError(
                    "Join query needs the driving table's alias to qualify ROWID; "
                    "pass --alias or reference the table as OWNER.TABLE <alias>."
                )
            eff_lo = None if (open_tails and idx == 0) else lo
            eff_hi = None if (open_tails and idx == last) else hi
            b = Boundary(lo=eff_lo, hi=eff_hi, label="ROWID")
            pred = dialect.render_predicate(cand, b, alias=alias)
            lines.append(f"{qs.inject_predicate(query, pred)}|{target}")
    return lines


def discover_and_build(
    dialect,
    conn,
    query,
    owner,
    table,
    target,
    strategy_req,
    chunks,
    alias=None,
    column=None,
    sub=False,
    explain=False,
):
    """Discover boundaries for ONE query on an already-open connection and render
    its queryfile lines. Returns (resolved_strategy, lines).

    Shared by run_split (single driving query) and batch_split.run_batch (many
    queries fanned out over one connection). Keeping this connection-agnostic lets
    the batch path reuse a single connection + one split-time watermark for the
    whole batch instead of reconnecting per query.
    """
    # Reject hostile / malformed user input before it is interpolated into SQL or
    # emitted into a queryfile line. Covers both run_split and batch_split.run_batch.
    alias = qs.validate_identifier(alias, "alias")
    column = qs.validate_identifier(column, "column", allow_qualified=True)
    target = qs.validate_target(target)

    partitioned = dialect.is_partitioned(conn, owner, table)
    strategy = resolve_strategy(strategy_req, partitioned)

    if strategy == "partition":
        boundaries = dialect.partition_specs(conn, owner, table, sub=sub)
        if not boundaries:
            raise SplitRunError(f"No partitions found for {owner}.{table}")
        # Resolve the driving alias so engines that qualify a column in the
        # partition predicate (SQL Server $PARTITION) bind to the right table;
        # Oracle / PostgreSQL renderers ignore it.
        resolved_alias = alias or qs.detect_alias(query, owner, table)
    elif strategy == "column":
        if not column:
            raise SplitRunError("--strategy column requires --column COL")
        resolved_alias = alias or qs.detect_alias(query, owner, table)
        _cand = Candidate(
            kind="column", key=column, meta={"owner": owner, "table": table}
        )
        boundaries = dialect.boundaries(conn, _cand, chunks)
    else:
        resolved_alias = alias or qs.detect_alias(query, owner, table)
        _cand = Candidate(
            kind="physical", key=None, meta={"owner": owner, "table": table}
        )
        # Physical boundaries come back as Boundary objects; build_lines coalesces
        # adjacent ROWID ranges (operating on (lo, hi) tuples) before rendering.
        boundaries = [(b.lo, b.hi) for b in dialect.boundaries(conn, _cand, chunks)]

    lines = build_lines(
        query,
        owner,
        table,
        resolved_alias,
        strategy,
        boundaries,
        target,
        sub,
        column=column,
        dialect=dialect,
    )

    if explain:
        _explain_first(dialect, conn, lines)
    return strategy, lines


def run_split(args, source_engine=None):
    dialect = get_dialect(source_engine)

    owner, table = dialect.parse_owner_table(args.table)
    query = load_query(args)

    # Optional Oracle PARALLEL hint (from the probe's parallel_degree recommendation):
    # inject once into the base SELECT so every generated slice inherits it, regardless
    # of split strategy. No-op for degree<=1 or engines without a parallel hint.
    hint = dialect.parallel_hint(getattr(args, "parallel", 1) or 1)
    if hint:
        query = qs.inject_hint(query, hint)

    conn = dialect.get_connection()
    try:
        # Capture ONE watermark BEFORE boundary discovery so a row inserted between the
        # split and the load-start watermark is missed by neither IL nor CDC.
        try:
            _wm = dialect.capture_watermark(conn)
        except Exception:
            _wm = None
        _sw = write_split_watermark(
            config.LOG_OUTPUT_PATH,
            _wm,
            os.path.basename(args.output),
            args.table,
            default_label=dialect.watermark_label,
        )
        if _sw:
            print(f"[splitter] split-time watermark sidecar -> {_sw}")

        strategy, lines = discover_and_build(
            dialect,
            conn,
            query,
            owner,
            table,
            args.target,
            args.strategy,
            args.chunks,
            alias=args.alias,
            column=args.column,
            sub=args.subpartitions,
            explain=args.explain,
        )
    finally:
        conn.close()

    with open(args.output, "w") as f:
        f.write(qs.format_lines(lines))

    print(f"[splitter] strategy={strategy} slices={len(lines)} -> {args.output}")
    if args.assort:
        _run_assort(args.output)
    return 0


def _explain_first(dialect, conn, lines):
    sql = lines[0].split("|")[0]
    plan = dialect.explain(conn, sql)
    print("[splitter] EXPLAIN PLAN for slice #1:")
    for line in plan.raw.splitlines():
        print(f"    {line}")


def _run_assort(output):
    _repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _assort = os.path.join(_repo_root, "tools", "make_assorted_queryfile.py")
    subprocess.run(
        [
            sys.executable,
            _assort,
            "--input",
            output,
            "--output",
            "queryfile-assorted.txt",
        ],
        check=True,
        cwd=os.path.dirname(os.path.abspath(output)) or ".",
    )
