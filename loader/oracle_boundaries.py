"""Oracle boundary discovery for the splitter: partition + ROWID (3-method fallback).

ROWID order: DBMS_PARALLEL_EXECUTE -> dba_extents -> NTILE-by-rowid. Falls back on
privilege errors only. All identifiers are validated before interpolation; all
data values use bind variables.
"""

import math
import os
import re

_IDENT = re.compile(r"^[A-Za-z0-9_$#]+$")


class BoundaryError(Exception):
    pass


def validate_ident(name):
    if not name or not _IDENT.match(name):
        raise BoundaryError(f"Invalid Oracle identifier: {name!r}")
    return name


def parse_owner_table(qualified):
    parts = (qualified or "").split(".")
    if len(parts) != 2:
        raise BoundaryError(f"--table must be OWNER.TABLE, got {qualified!r}")
    return validate_ident(parts[0].strip().upper()), validate_ident(
        parts[1].strip().upper()
    )


def _is_priv_error(e):
    s = str(e)
    return any(
        code in s for code in ("ORA-01031", "ORA-00942", "PLS-", "insufficient priv")
    )


def is_partitioned(conn, owner, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT partitioned FROM all_tables WHERE owner = :o AND table_name = :t",
            o=owner,
            t=table,
        )
        row = cur.fetchone()
    if row is None:
        raise BoundaryError(
            f"Table {owner}.{table} not visible to ORACLE_USER (existence/privilege)."
        )
    return row[0] == "YES"


def list_partitions(conn, owner, table, sub=False):
    with conn.cursor() as cur:
        if sub:
            cur.execute(
                "SELECT subpartition_name FROM all_tab_subpartitions "
                "WHERE table_owner = :o AND table_name = :t ORDER BY subpartition_position",
                o=owner,
                t=table,
            )
        else:
            cur.execute(
                "SELECT partition_name FROM all_tab_partitions "
                "WHERE table_owner = :o AND table_name = :t ORDER BY partition_position",
                o=owner,
                t=table,
            )
        return [r[0] for r in cur.fetchall()]


def rowid_ranges(conn, owner, table, n):
    """Return [(lo, hi)] ROWID ranges using the first method the account can run."""
    last = None
    for method in (_ranges_dbms_parallel, _ranges_dba_extents, _ranges_ntile):
        try:
            ranges = method(conn, owner, table, n)
            if ranges:
                print(
                    f"[splitter] rowid ranges via {method.__name__} -> {len(ranges)} slices"
                )
                return ranges
        except Exception as e:  # noqa: BLE001 - fall back only on privilege errors
            last = e
            if _is_priv_error(e):
                print(
                    f"[splitter] {method.__name__} unavailable ({e}); trying next method"
                )
                continue
            raise
    raise BoundaryError(f"No ROWID chunking method succeeded (last error: {last})")


def _segment_blocks(conn, owner, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT blocks FROM all_tables WHERE owner = :o AND table_name = :t",
            o=owner,
            t=table,
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] else 0


def _ranges_dbms_parallel(conn, owner, table, n):
    task = f"PLL_{owner}_{table}_{os.getpid()}"[:128]
    blocks = _segment_blocks(conn, owner, table)
    chunk_size = max(1, math.ceil(blocks / n)) if blocks else 1000
    with conn.cursor() as cur:
        try:
            cur.callproc("DBMS_PARALLEL_EXECUTE.CREATE_TASK", [task])
            cur.callproc(
                "DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID",
                [task, owner, table, False, chunk_size],
            )
            cur.execute(
                "SELECT start_rowid, end_rowid FROM user_parallel_execute_chunks "
                "WHERE task_name = :task ORDER BY chunk_id",
                task=task,
            )
            return [(str(lo), str(hi)) for lo, hi in cur.fetchall()]
        finally:
            try:
                cur.callproc("DBMS_PARALLEL_EXECUTE.DROP_TASK", [task])
            except Exception:
                pass


# dba_extents grouping (adapted from oracle_rowsplit.sql) -> (lo_rowid, hi_rowid) per group.
_DBA_EXTENTS_SQL = """
SELECT
  dbms_rowid.rowid_create(1, data_object_id, lo_fno, lo_block, 0)            AS lo_rowid,
  dbms_rowid.rowid_create(1, data_object_id, hi_fno, hi_block, 10000)        AS hi_rowid
FROM (
  SELECT DISTINCT grp,
    first_value(relative_fno) OVER (PARTITION BY grp ORDER BY relative_fno, block_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) lo_fno,
    first_value(block_id) OVER (PARTITION BY grp ORDER BY relative_fno, block_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) lo_block,
    last_value(relative_fno) OVER (PARTITION BY grp ORDER BY relative_fno, block_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) hi_fno,
    last_value(block_id + blocks - 1) OVER (PARTITION BY grp ORDER BY relative_fno, block_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) hi_block
  FROM (
    SELECT relative_fno, block_id, blocks,
      trunc((SUM(blocks) OVER (ORDER BY relative_fno, block_id) - 0.01)
            / (SUM(blocks) OVER () / :n)) grp
    FROM dba_extents
    WHERE segment_name = :t AND owner = :o
  )
),
(SELECT data_object_id FROM dba_objects WHERE object_name = :t AND owner = :o)
ORDER BY lo_rowid
"""


def _ranges_dba_extents(conn, owner, table, n):
    with conn.cursor() as cur:
        cur.execute(_DBA_EXTENTS_SQL, n=n, t=table, o=owner)
        return [(str(lo), str(hi)) for lo, hi in cur.fetchall()]


def _ranges_ntile(conn, owner, table, n):
    print(
        f"[splitter] WARNING: _ranges_ntile will full-scan {owner}.{table} — "
        f"DBMS_PARALLEL_EXECUTE and dba_extents both unavailable. "
        f"This may be slow on large tables."
    )
    # OWNER.TABLE is validated upstream; identifiers cannot be bound.
    sql = (
        f"SELECT MIN(rid), MAX(rid) FROM ("
        f"  SELECT ROWID rid, NTILE(:n) OVER (ORDER BY ROWID) bucket FROM {owner}.{table}"
        f") GROUP BY bucket ORDER BY bucket"
    )
    with conn.cursor() as cur:
        cur.execute(sql, n=n)
        return [(str(lo), str(hi)) for lo, hi in cur.fetchall()]


# ---------------------------------------------------------------------------
# Column-range split-key discovery + segment sizing
# ---------------------------------------------------------------------------

_SPLITTABLE_TYPES = ("NUMBER", "FLOAT", "DATE")  # plus anything starting with TIMESTAMP


def _is_splittable_type(data_type):
    """Return True if *data_type* is usable as a column-range split key."""
    dt = (data_type or "").upper()
    return dt in _SPLITTABLE_TYPES or dt.startswith("TIMESTAMP")


def _rank_split_columns(rows):
    """Pure: pick + order good column-range split keys.

    rows: iterable of dicts {column_name, data_type, num_distinct, indexed(0/1)}.
    Keep numeric/date/timestamp columns with num_distinct > 1; order by
    (indexed desc, num_distinct desc). Return a list of column names.
    """
    good = [
        r
        for r in rows
        if _is_splittable_type(r.get("data_type"))
        and int(r.get("num_distinct") or 0) > 1
    ]
    good.sort(
        key=lambda r: (int(r.get("indexed") or 0), int(r.get("num_distinct") or 0)),
        reverse=True,
    )
    return [r["column_name"] for r in good]


def discover_split_columns(conn, owner, table):
    """Query all_tab_columns/all_tab_col_statistics and return ranked split-key names.

    Returns a list of column names ordered by (indexed desc, num_distinct desc),
    keeping only numeric/date/timestamp columns with num_distinct > 1.
    """
    sql = (
        "SELECT c.column_name, c.data_type, NVL(s.num_distinct, 0) AS num_distinct, "
        "       CASE WHEN EXISTS (SELECT 1 FROM all_ind_columns i "
        "                         WHERE i.table_owner = :o AND i.table_name = :t "
        "                           AND i.column_name = c.column_name) THEN 1 ELSE 0 END AS indexed "
        "FROM all_tab_columns c "
        "LEFT JOIN all_tab_col_statistics s "
        "  ON s.owner = c.owner AND s.table_name = c.table_name AND s.column_name = c.column_name "
        "WHERE c.owner = :o AND c.table_name = :t "
        "  AND (c.data_type IN ('NUMBER','FLOAT','DATE') OR c.data_type LIKE 'TIMESTAMP%')"
    )
    with conn.cursor() as cur:
        cur.execute(sql, o=owner, t=table)
        rows = [
            {
                "column_name": r[0],
                "data_type": r[1],
                "num_distinct": r[2],
                "indexed": r[3],
            }
            for r in cur.fetchall()
        ]
    return _rank_split_columns(rows)


def segment_size_bytes(conn, owner, table):
    """Return the segment size in bytes for *owner.table*.

    Tries dba_segments first (requires DBA or SELECT ANY DICTIONARY).
    On privilege error falls back to all_tables.blocks × 8192 (documented approximation).
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT NVL(SUM(bytes),0) FROM dba_segments "
                "WHERE owner = :o AND segment_name = :t",
                o=owner,
                t=table,
            )
            row = cur.fetchone()
        if row and row[0]:
            return int(row[0])
    except Exception as e:  # noqa: BLE001 - privilege fallback only
        if not _is_priv_error(e):
            raise
    # Fallback: all_tables.blocks × assumed 8K block (documented approximation).
    return _segment_blocks(conn, owner, table) * 8192


def slices_from_cuts(cuts):
    """Pure: gap-free, half-open ranges from sorted cut points.
    Returns [(lo, hi), ...] with None for the open first-low and last-high ends.
    [] cuts -> [(None, None)] (a single 'all non-null' slice)."""
    cuts = list(cuts)
    if not cuts:
        return [(None, None)]
    out = [(None, cuts[0])]
    for i in range(len(cuts) - 1):
        out.append((cuts[i], cuts[i + 1]))
    out.append((cuts[-1], None))
    return out


def column_range_boundaries(conn, owner, table, col, n, sample_pct=0.1):
    """Return a COMPLETE, gap-free column-range cover as [(lo, hi), ...].

    Fetches per-bucket MINs from a sampled NTILE, derives internal cut points
    (dropping bucket 1's min, which is open-below), and returns contiguous
    half-open ranges with None sentinels for the open first-low / last-high ends
    via slices_from_cuts(). These ranges cover every non-null value; the dialect
    adds a separate IS NULL slice so the overall cover loses no rows.

    Uses SAMPLE to keep the boundary scan bounded — never a full scan.
    On trillion-row tables keep sample_pct small (0.01–0.1).
    Raw Python values are returned; the dialect is responsible for formatting
    them into SQL literals appropriate for the source engine.

    Histogram-based boundaries (USER_TAB_HISTOGRAMS) are a future optimisation
    that would avoid the sample scan entirely for tables with fresh statistics.

    col, owner, table are validated against _IDENT before interpolation.
    n and sample_pct are numeric, not bound (safe via int()/float() coercion).
    """
    validate_ident(col)
    validate_ident(owner)
    validate_ident(table)
    n = int(n)
    pct = float(sample_pct)
    if not (0 < pct <= 100):
        raise BoundaryError(f"sample_pct out of range: {sample_pct!r}")
    sql = (
        f"SELECT MIN(v) FROM ("
        f"  SELECT {col} AS v, NTILE({n}) OVER (ORDER BY {col}) AS bucket"
        f"  FROM {owner}.{table} SAMPLE ({pct})"
        f"  WHERE {col} IS NOT NULL"
        f") GROUP BY bucket ORDER BY bucket"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        mins = [r[0] for r in cur.fetchall() if r[0] is not None]
    cuts = []
    for v in mins[1:]:  # bucket 1 is open-below, so skip its min
        if not cuts or v != cuts[-1]:
            cuts.append(v)
    return slices_from_cuts(cuts)
