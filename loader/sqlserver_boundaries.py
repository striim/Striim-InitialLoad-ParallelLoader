"""SQL Server boundary discovery + SHOWPLAN parsing for the splitter.

Import-safe WITHOUT pyodbc: nothing here imports a DB driver. The dialect's
get_connection() owns the only pyodbc import (lazy). Catalog queries here take an
open DBAPI connection (pyodbc-style ``?`` placeholders, positional params).

Boundary discovery uses NTILE over a BOUNDED sample (TABLESAMPLE + TOP) so the
scan never reads the whole table — safe on very large tables. All identifiers are
validated against a strict pattern before interpolation; data values are bound.

summarize_showplan() is PURE (no DB): it parses an estimated SHOWPLAN_XML string
and distils it onto the probe's PlanSummary shape. It is namespace-agnostic and
NEVER raises — any parse failure degrades to a generic result.
"""

import re
import xml.etree.ElementTree as ET

# Strict identifier pattern. SQL Server allows more in bracketed identifiers, but
# we interpolate these into NTILE/sample SQL (identifiers can't be bound), so we
# keep it conservative to prevent injection. Names with spaces are rejected.
_IDENT = re.compile(r"^[A-Za-z0-9_$#@]+$")

# Bound the boundary-discovery sample so we never full-scan a huge table.
DEFAULT_SAMPLE_ROWS = 200_000

# A FULL scan only signals join amplification when the inner table is large; tiny
# lookup tables are cheap to scan and would otherwise collapse the bake-off.
FULL_SCAN_AMPLIFY_ROWS = 1_000_000

# SHOWPLAN physical operators that read a table (the candidate "access path").
_ACCESS_OPS = frozenset(
    {
        "Clustered Index Seek",
        "Clustered Index Scan",
        "Index Seek",
        "Index Scan",
        "Table Scan",
        "Columnstore Index Scan",
        "RID Lookup",
        "Key Lookup",
    }
)

# Operators that read a table in full (amplification signal when the table is big).
_SCAN_OPS = frozenset(
    {
        "Clustered Index Scan",
        "Index Scan",
        "Table Scan",
        "Columnstore Index Scan",
    }
)

# Elements that mark a pushed-down range/seek predicate (=> a disjoint range scan).
_SEEK_PREDICATE_TAGS = frozenset(
    {"SeekPredicates", "SeekPredicateNew", "SeekPredicate"}
)


class BoundaryError(Exception):
    pass


def validate_ident(name):
    if not name or not _IDENT.match(name):
        raise BoundaryError(f"Invalid SQL Server identifier: {name!r}")
    return name


def parse_owner_table(qualified):
    """Split ``SCHEMA.TABLE`` (or bare ``TABLE`` -> dbo) into ``(schema, table)``.

    SQL Server is case-preserving; we do NOT upper-case. The default schema is
    ``dbo`` when the spec has no dot. Both parts are validated.
    """
    s = (qualified or "").strip()
    if "." in s:
        schema, table = s.rsplit(".", 1)
        schema = schema.strip()
    else:
        schema, table = "dbo", s
    table = table.strip()
    return validate_ident(schema), validate_ident(table)


def _obj_name(owner, table):
    """Bracket-quoted ``[schema].[table]`` for OBJECT_ID(...) / interpolation."""
    schema = validate_ident(owner or "dbo")
    validate_ident(table)
    return f"[{schema}].[{table}]"


# ---------------------------------------------------------------------------
# Catalog queries (sys.* / INFORMATION_SCHEMA)
# ---------------------------------------------------------------------------


def is_partitioned(conn, owner, table):
    obj = _obj_name(owner, table)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM sys.partitions "
            "WHERE object_id = OBJECT_ID(?) AND partition_number > 1",
            obj,
        )
        row = cur.fetchone()
    return bool(row and row[0] and int(row[0]) > 0)


def list_partitions(conn, owner, table, sub=False):
    """Partition numbers in order. SQL Server has no subpartitions; ``sub`` is ignored."""
    obj = _obj_name(owner, table)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT partition_number FROM sys.partitions "
            "WHERE object_id = OBJECT_ID(?) AND index_id IN (0, 1) "
            "ORDER BY partition_number",
            obj,
        )
        return [r[0] for r in cur.fetchall()]


def clustered_key_columns(conn, owner, table):
    """Ordered key column names of the clustered index (index_id = 1).

    Returns [] for a heap (no clustered index) so the dialect omits the physical
    candidate and relies on column candidates.
    """
    obj = _obj_name(owner, table)
    sql = (
        "SELECT c.name "
        "FROM sys.indexes i "
        "JOIN sys.index_columns ic "
        "  ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        "JOIN sys.columns c "
        "  ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE i.object_id = OBJECT_ID(?) AND i.index_id = 1 "
        "  AND ic.is_included_column = 0 "
        "ORDER BY ic.key_ordinal"
    )
    with conn.cursor() as cur:
        cur.execute(sql, obj)
        return [r[0] for r in cur.fetchall()]


# Numeric / temporal types usable as a column-range split key.
_SPLITTABLE_TYPES = (
    "int",
    "bigint",
    "smallint",
    "tinyint",
    "numeric",
    "decimal",
    "money",
    "smallmoney",
    "float",
    "real",
    "date",
    "datetime",
    "datetime2",
    "smalldatetime",
    "datetimeoffset",
)


def discover_split_columns(conn, owner, table):
    """Indexed, splittable (numeric/temporal) key columns, distinct.

    These become up to 3 column candidates. The leading clustered-index column is
    already covered by the physical candidate, so it is excluded here.
    """
    obj = _obj_name(owner, table)
    type_list = ", ".join("'%s'" % t for t in _SPLITTABLE_TYPES)
    sql = (
        "SELECT DISTINCT c.name "
        "FROM sys.index_columns ic "
        "JOIN sys.columns c "
        "  ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "JOIN sys.types ty ON ty.user_type_id = c.user_type_id "
        "WHERE ic.object_id = OBJECT_ID(?) AND ic.key_ordinal > 0 "
        "  AND ic.is_included_column = 0 "
        f"  AND ty.name IN ({type_list}) "
        "ORDER BY c.name"
    )
    with conn.cursor() as cur:
        cur.execute(sql, obj)
        cols = [r[0] for r in cur.fetchall()]
    lead = clustered_key_columns(conn, owner, table)
    lead_set = {lead[0]} if lead else set()
    return [c for c in cols if c not in lead_set]


def partition_function_and_column(conn, owner, table):
    """Return ``(partition_function_name, partitioning_column_name)`` for a range-
    partitioned table, or ``(None, None)`` when it is not partitioned.

    Resolves through the table's clustered/heap index (``index_id`` 0 or 1): its
    data space maps to a partition scheme, which maps to a partition function; the
    index column with ``partition_ordinal = 1`` is the partitioning key. These two
    names feed a ``$PARTITION.[func](col) = N`` predicate. The table is validated /
    bound via OBJECT_ID.
    """
    obj = _obj_name(owner, table)
    sql = (
        "SELECT pf.name AS func_name, c.name AS col_name "
        "FROM sys.indexes i "
        "JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id "
        "JOIN sys.partition_functions pf ON pf.function_id = ps.function_id "
        "JOIN sys.index_columns ic "
        "  ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        "  AND ic.partition_ordinal = 1 "
        "JOIN sys.columns c "
        "  ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE i.object_id = OBJECT_ID(?) AND i.index_id IN (0, 1)"
    )
    with conn.cursor() as cur:
        cur.execute(sql, obj)
        row = cur.fetchone()
    if not row:
        return (None, None)
    return (row[0], row[1])


def segment_size_bytes(conn, owner, table):
    """Approx allocated bytes: SUM(used_page_count) * 8192 (8 KB pages)."""
    obj = _obj_name(owner, table)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT SUM(used_page_count) * 8192 FROM sys.dm_db_partition_stats "
            "WHERE object_id = OBJECT_ID(?)",
            obj,
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] else 0


# ---------------------------------------------------------------------------
# Column-range / clustered-key boundary discovery (bounded NTILE)
# ---------------------------------------------------------------------------


def slices_from_cuts(cuts):
    """Pure: gap-free, half-open ranges from sorted cut points.

    Returns [(lo, hi), ...] with None for the open first-low and last-high ends.
    [] cuts -> [(None, None)] (a single 'all non-null' slice).
    """
    cuts = list(cuts)
    if not cuts:
        return [(None, None)]
    out = [(None, cuts[0])]
    for i in range(len(cuts) - 1):
        out.append((cuts[i], cuts[i + 1]))
    out.append((cuts[-1], None))
    return out


def ntile_boundaries(conn, owner, table, col, n, sample=None):
    """Return a COMPLETE, gap-free column-range cover as [(lo, hi), ...].

    Divides the rows N ways with ``NTILE(n) OVER (ORDER BY col)`` over a BOUNDED
    sample (``TABLESAMPLE (sample ROWS)`` capped by ``TOP``), takes the per-bucket
    MINs, drops bucket 1's min (open-below), and returns contiguous half-open
    ranges via slices_from_cuts(). Half-open ``>= lo AND < hi`` stays disjoint and
    gap-free even when the key has duplicates at a cut point.

    Used for both the physical (leading clustered key) and column candidates. The
    bounded sample keeps the scan safe on very large tables; the dialect adds a
    separate IS NULL slice for nullable column candidates so no rows are lost.

    Raw Python values are returned; the dialect formats them into T-SQL literals.
    col/owner/table are validated before interpolation; n/sample are coerced ints.
    """
    validate_ident(col)
    validate_ident(owner)
    validate_ident(table)
    n = int(n)
    sample = int(sample if sample is not None else DEFAULT_SAMPLE_ROWS)
    fq = _obj_name(owner, table)
    sql = (
        f"SELECT MIN(v) FROM ("
        f"  SELECT v, NTILE({n}) OVER (ORDER BY v) AS bucket FROM ("
        f"    SELECT TOP ({sample}) {col} AS v "
        f"    FROM {fq} TABLESAMPLE ({sample} ROWS) "
        f"    WHERE {col} IS NOT NULL"
        f"  ) s"
        f") g GROUP BY bucket ORDER BY bucket"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        mins = [r[0] for r in cur.fetchall() if r[0] is not None]
    cuts = []
    for v in mins[1:]:  # bucket 1 is open-below, so skip its min
        if not cuts or v != cuts[-1]:
            cuts.append(v)
    return slices_from_cuts(cuts)


# ---------------------------------------------------------------------------
# SHOWPLAN_XML parsing (pure, namespace-agnostic, never raises)
# ---------------------------------------------------------------------------


def _local(tag):
    """Strip the XML namespace: '{...}RelOp' -> 'RelOp'."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _has_seek_predicate(el):
    for sub in el.iter():
        if _local(sub.tag) in _SEEK_PREDICATE_TAGS:
            return True
    return False


def _object_name(el):
    """First <Object> Table name under *el*, brackets stripped ('' if none)."""
    for sub in el.iter():
        if _local(sub.tag) == "Object":
            name = sub.get("Table") or sub.get("Index") or ""
            return name.strip("[]")
    return ""


def summarize_showplan(xml_text, amplify_rows=FULL_SCAN_AMPLIFY_ROWS):
    """Parse an estimated SHOWPLAN_XML string. NEVER raises.

    Returns a 4-tuple (access_path, is_disjoint, full_scans, raw):
      * access_path  - the driving physical op (first table-access RelOp in
                       document order: Index Seek / Clustered Index Scan / ...).
      * is_disjoint  - True for a Seek or a scan carrying a pushed range/seek
                       predicate; False for a plain full Scan.
      * full_scans   - tables read by a full Scan whose EstimateRows exceeds
                       *amplify_rows* (join-amplification signal).
      * raw          - the plan text (truncated for logging).
    Any parse failure degrades to a generic ('(showplan ...)', False, [], raw).
    """
    raw = (xml_text or "")[:8000]
    try:
        root = ET.fromstring(xml_text)
    except Exception:  # noqa: BLE001 - malformed/empty plan -> generic, never raise
        return ("(showplan parse failed)", False, [], raw)

    relops = [el for el in root.iter() if _local(el.tag) == "RelOp"]
    if not relops:
        return ("(no RelOp in showplan)", False, [], raw)

    driving = next((el for el in relops if el.get("PhysicalOp") in _ACCESS_OPS), None)
    if driving is None:
        driving = relops[0]
    access_path = driving.get("PhysicalOp") or driving.get("LogicalOp") or ""

    is_disjoint = "Seek" in access_path or _has_seek_predicate(driving)

    full_scans = []
    for el in relops:
        if el.get("PhysicalOp") in _SCAN_OPS:
            try:
                est = float(el.get("EstimateRows") or 0)
            except (TypeError, ValueError):
                est = 0.0
            if est > amplify_rows:
                name = _object_name(el)
                if name and name not in full_scans:
                    full_scans.append(name)

    return (access_path, is_disjoint, full_scans, raw)
