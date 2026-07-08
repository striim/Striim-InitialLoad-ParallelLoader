"""Fan a pasted batch of queries into one queryfile.

Each pasted line is `SQL|TARGET` (the same format the loader reads). Lines that
contain a ``~SPLIT~`` token are *splittable*: they get probed (optional) and split
into N slice lines. Lines with no token are *pass-through*: emitted verbatim as a
single load job. The whole batch shares ONE source connection and ONE split-time
watermark, so it behaves like a single `split` run that happens to cover many
driving queries.

The interactive collection (paste loop, per-query driving-table prompts) lives in
manage.sh; this module consumes an already-structured list of entries so the logic
stays pure enough to unit-test with the fake dialects in tests/.
"""

import os

import config
import query_split as qs
from probe import run_probe
from source_dialect import get_dialect
from split_runner import (
    SplitRunError,
    _run_assort,
    discover_and_build,
    write_split_watermark,
)

DELIM = config.QUERY_FILE_DELIMITER


class BatchError(Exception):
    pass


def split_query_target(raw):
    """Split one pasted ``SQL|TARGET`` line into ``(query, target)``.

    The loader parses the queryfile with ``csv`` (delimiter ``|``), taking field 0
    as the query and field 1 as the target, so a query may NOT contain a raw ``|``
    (this includes Oracle ``||`` string concatenation). Enforce that here rather
    than let a stray delimiter silently truncate the query at load time.
    """
    parts = raw.split(DELIM)
    if len(parts) < 2 or not parts[-1].strip():
        raise BatchError(
            f"line has no '{DELIM}TARGET' suffix: {raw!r}. "
            f"Each line must be `SQL{DELIM}OWNER.TARGET`."
        )
    if len(parts) > 2:
        raise BatchError(
            f"line has more than one '{DELIM}': {raw!r}. Queries may not contain "
            f"'{DELIM}' (including Oracle '||') — the loader splits on it."
        )
    return parts[0].strip(), parts[1].strip()


def is_splittable(query):
    """A query is splittable if it carries the ~SPLIT~ token."""
    return qs.SPLIT_TOKEN in query


def build_batch_lines(
    entries,
    dialect,
    conn,
    output,
    depth="bakeoff",
    probe=True,
    default_strategy="auto",
    default_chunks=16,
    explain=False,
    on_summary=None,
):
    """Turn structured entries into queryfile lines on an already-open connection.

    ``entries`` is a list of dicts:
        {"line": "SQL|TARGET", "needs_split": bool,
         "table": "OWNER.T" | None, "alias": str | None,
         "strategy": str | None, "column": str | None, "chunks": int | None}

    Returns the flat list of ``SQL|TARGET`` lines (split slices + pass-through).
    ``on_summary(text)`` receives one human-readable line per entry, if given.
    Captures one split-time watermark before the first boundary discovery.
    """
    all_lines = []
    watermark_captured = False

    for i, entry in enumerate(entries, start=1):
        raw = (entry.get("line") or "").strip()
        if not raw:
            continue
        query, target = split_query_target(raw)

        # A line flagged splittable by the wizard must actually carry the token;
        # if not, treat it as pass-through (don't guess a predicate location).
        needs_split = bool(entry.get("needs_split")) and is_splittable(query)

        if not needs_split:
            all_lines.append(f"{query}{DELIM}{target}")
            if on_summary:
                on_summary(f"#{i}: pass-through -> {target}")
            continue

        table = entry.get("table")
        if not table:
            raise BatchError(
                f"line #{i} is splittable ({qs.SPLIT_TOKEN}) but no driving "
                f"OWNER.TABLE was provided."
            )
        owner, tbl = dialect.parse_owner_table(table)
        alias = entry.get("alias") or None

        # ONE watermark for the whole batch, captured before the first discovery so a
        # row inserted between the split and the load-start watermark is missed by
        # neither IL nor CDC (mirrors run_split).
        if not watermark_captured:
            try:
                _wm = dialect.capture_watermark(conn)
            except Exception:
                _wm = None
            _sw = write_split_watermark(
                config.LOG_OUTPUT_PATH,
                _wm,
                os.path.basename(output),
                table,
                default_label=dialect.watermark_label,
            )
            if _sw:
                print(f"[batch] split-time watermark sidecar -> {_sw}")
            watermark_captured = True

        if probe:
            rec = run_probe(
                query,
                owner,
                tbl,
                alias=alias,
                depth=depth,
                dialect=dialect,
                probe_chunks=entry.get("chunks") or default_chunks,
            )
            strategy_req = rec.strategy
            column = rec.key
            chunks = rec.chunk_count
            print(
                f"[batch] #{i} probe -> strategy={strategy_req} "
                f"column={column or '-'} chunks={chunks}"
            )
        else:
            strategy_req = entry.get("strategy") or default_strategy
            column = entry.get("column")
            chunks = entry.get("chunks") or default_chunks

        strategy, lines = discover_and_build(
            dialect,
            conn,
            query,
            owner,
            tbl,
            target,
            strategy_req,
            chunks,
            alias=alias,
            column=column,
            sub=False,
            explain=explain,
        )
        all_lines.extend(lines)
        if on_summary:
            col_disp = f"({column})" if column else ""
            on_summary(f"#{i}: {strategy}{col_disp} x{len(lines)} -> {target}")

    return all_lines


def run_batch(
    entries,
    output,
    source_engine=None,
    depth="bakeoff",
    probe=True,
    default_strategy="auto",
    default_chunks=16,
    explain=False,
    assort=False,
):
    """Fan a batch of pasted queries into ``output``. Returns 0 on success."""
    if not entries:
        raise BatchError("no queries provided")

    dialect = get_dialect(source_engine)
    summary = []
    conn = dialect.get_connection()
    try:
        all_lines = build_batch_lines(
            entries,
            dialect,
            conn,
            output,
            depth=depth,
            probe=probe,
            default_strategy=default_strategy,
            default_chunks=default_chunks,
            explain=explain,
            on_summary=summary.append,
        )
    finally:
        conn.close()

    if not all_lines:
        raise BatchError("no queryfile lines produced (all input lines were blank).")

    with open(output, "w") as f:
        f.write(qs.format_lines(all_lines))

    print(f"[batch] {len(entries)} queries -> {len(all_lines)} lines -> {output}")
    for s in summary:
        print(f"  {s}")
    if assort:
        _run_assort(output)
    return 0
