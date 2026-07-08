"""PostgreSQL boundary discovery + catalog helpers for the splitter / probe.

Mirrors ``oracle_boundaries`` but for PostgreSQL:

* PHYSICAL: ``ctid`` block ranges derived from ``pg_class.relpages`` (the planner's
  page estimate). Each slice is a half-open page band ``[lo, hi)`` rendered later as
  ``ctid >= '(lo,0)'::tid AND ctid < '(hi,0)'::tid``.
* COLUMN: equi-depth cut points from ``percentile_disc`` over the driving column,
  turned into gap-free half-open ranges (the dialect adds the IS NULL cover).
  ``percentile_disc`` (not ``percentile_cont``) is used so date/timestamp/text split
  keys work — it returns real existing column values for any sortable type, whereas
  ``percentile_cont`` only accepts numeric/interval ORDER BY and errors otherwise.

Import-safe WITHOUT psycopg2 — nothing here imports the driver. These helpers
receive an already-open DBAPI connection (the dialect opens it lazily in
``get_connection``). Identifiers are validated before any interpolation; every
data value is passed as a bound parameter.
"""

import re

# Unquoted PostgreSQL identifiers: start with a letter/underscore, then
# letters/digits/underscore/$. Validated before being interpolated into SQL text.
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# pg_class.relpages is unknown (0) for never-analyzed tables; fall back to a single
# full-table ctid band up to the max BlockNumber so existing rows are still covered.
_UNKNOWN_PAGES = 4_294_967_295  # 0xFFFFFFFF — wider than any real heap.


class PostgresError(Exception):
    """Raised for missing tables / invalid identifiers (PG analogue of BoundaryError)."""


def validate_ident(name):
    """Validate an unquoted PG identifier before interpolation; raise otherwise."""
    if not name or not _IDENT.match(name):
        raise PostgresError(f"Invalid PostgreSQL identifier: {name!r}")
    return name


def _schema(owner):
    """Owner -> schema name; default to ``public`` when owner is None/empty."""
    return owner if owner else "public"


# ---------------------------------------------------------------------------
# existence / partitioning (all values bound — no interpolation)
# ---------------------------------------------------------------------------
def table_exists(conn, owner, table):
    schema = _schema(owner)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, table),
        )
        return cur.fetchone() is not None


def is_partitioned(conn, owner, table):
    """True for a declaratively partitioned parent (pg_partitioned_table)."""
    schema = _schema(owner)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 "
            "FROM pg_partitioned_table pt "
            "JOIN pg_class c ON c.oid = pt.partrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s",
            (schema, table),
        )
        return cur.fetchone() is not None


def list_partitions(conn, owner, table, sub=False):
    """Child partition relnames of a partitioned parent, in name order.

    ``sub`` is accepted for interface parity with Oracle (PG sub-partitioning is
    just nested declarative partitions); it is not used here.
    """
    schema = _schema(owner)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT child.relname "
            "FROM pg_inherits i "
            "JOIN pg_class parent ON parent.oid = i.inhparent "
            "JOIN pg_namespace pn ON pn.oid = parent.relnamespace "
            "JOIN pg_class child ON child.oid = i.inhrelid "
            "WHERE pn.nspname = %s AND parent.relname = %s "
            "ORDER BY child.relname",
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# column split-key discovery (indexed numeric/temporal columns, n_distinct > 1)
# ---------------------------------------------------------------------------
def _distinct_usable(n_distinct):
    """Map pg_stats.n_distinct to (usable, score).

    pg_stats semantics: ``> 0`` is an estimated distinct count; ``< 0`` is the
    negative fraction of distinct rows (e.g. -1 == unique) and signals high
    cardinality; 0/NULL is unknown. A column is usable when it has > 1 distinct
    value (positive) or is fraction-based (negative).
    """
    if n_distinct is None:
        return (False, 0.0)
    try:
        nd = float(n_distinct)
    except (TypeError, ValueError):
        return (False, 0.0)
    if nd < 0:
        # Fraction of rows -> treat as highest cardinality (great split key).
        return (True, 1e18 + (-nd))
    if nd > 1:
        return (True, nd)
    return (False, nd)


def _rank_split_columns(rows):
    """Pure: order usable split keys by distinctness desc; dedupe by name.

    rows: iterable of dicts {column_name, n_distinct}. Returns column names.
    """
    best = {}
    for r in rows:
        usable, score = _distinct_usable(r.get("n_distinct"))
        if not usable:
            continue
        name = r["column_name"]
        if name not in best or score > best[name]:
            best[name] = score
    return [name for name, _ in sorted(best.items(), key=lambda kv: -kv[1])]


def discover_split_columns(conn, owner, table):
    """Ranked indexed numeric/temporal split-key column names.

    Joins pg_index/pg_attribute (indexed columns), pg_type (typcategory N=numeric,
    D=date/time — both range-amenable), and pg_stats (n_distinct). All values bound.
    """
    schema = _schema(owner)
    sql = (
        "SELECT a.attname AS column_name, s.n_distinct AS n_distinct "
        "FROM pg_class c "
        "JOIN pg_namespace nsp ON nsp.oid = c.relnamespace "
        "JOIN pg_index ix ON ix.indrelid = c.oid "
        "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(ix.indkey) "
        "JOIN pg_type ty ON ty.oid = a.atttypid "
        "LEFT JOIN pg_stats s "
        "  ON s.schemaname = nsp.nspname AND s.tablename = c.relname AND s.attname = a.attname "
        "WHERE nsp.nspname = %s AND c.relname = %s "
        "  AND a.attnum > 0 AND NOT a.attisdropped "
        "  AND ty.typcategory IN ('N', 'D')"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        rows = [{"column_name": r[0], "n_distinct": r[1]} for r in cur.fetchall()]
    return _rank_split_columns(rows)


# ---------------------------------------------------------------------------
# physical (ctid) page ranges
# ---------------------------------------------------------------------------
def relpages(conn, owner, table):
    """pg_class.relpages (planner page estimate) for owner.table; 0 if unknown."""
    schema = _schema(owner)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relpages FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s",
            (schema, table),
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def ctid_page_ranges(conn, owner, table, n):
    """Divide ``relpages`` into up to ``n`` equal, gap-free half-open page bands.

    Returns ``[(page_lo, page_hi), ...]`` with concrete int bounds (never None —
    the shared coalesce step in split_runner does ``max(prev_hi, hi)`` and would
    choke on None). The final band's hi == relpages; rows written into pages added
    after the planner estimate are caught by the CDC watermark, not this band.
    Empty (lo == hi) bands are dropped when relpages < n.
    """
    pages = relpages(conn, owner, table)
    n = max(1, int(n))
    if pages <= 0:
        # Never-analyzed / unknown: one full-table band up to the max BlockNumber.
        return [(0, _UNKNOWN_PAGES)]
    edges = [(i * pages) // n for i in range(n + 1)]
    edges[-1] = pages
    ranges = []
    for i in range(n):
        lo, hi = edges[i], edges[i + 1]
        if lo >= hi:
            continue  # empty bucket when pages < n
        ranges.append((lo, hi))
    return ranges or [(0, pages)]


# ---------------------------------------------------------------------------
# column equi-depth ranges (percentile_disc)
# ---------------------------------------------------------------------------
def slices_from_cuts(cuts):
    """Pure: gap-free half-open ranges from sorted internal cut points.

    Returns ``[(lo, hi), ...]`` with None for the open first-low / last-high ends.
    ``[]`` cuts -> ``[(None, None)]`` (a single 'all non-null' slice).
    """
    cuts = list(cuts)
    if not cuts:
        return [(None, None)]
    out = [(None, cuts[0])]
    for i in range(len(cuts) - 1):
        out.append((cuts[i], cuts[i + 1]))
    out.append((cuts[-1], None))
    return out


def column_range_boundaries(conn, owner, table, col, n):
    """Gap-free half-open column cover via ``percentile_disc``.

    Computes n-1 internal cut points at fractions 1/n..(n-1)/n with
    ``percentile_disc(ARRAY[...]) WITHIN GROUP (ORDER BY col)``, de-dups equal
    cuts, and returns contiguous half-open ranges (open first-low / last-high).
    Raw Python values come back; the dialect formats them into SQL literals.

    Identifiers (col, schema, table) are validated before interpolation; the
    fraction array is built from int(n) only (no user data interpolated).

    ``percentile_disc`` (discrete) is used over ``percentile_cont`` because it
    accepts ANY sortable ORDER BY type — date/timestamp/text split keys included —
    and returns real existing column values. ``percentile_cont`` only accepts
    numeric/interval expressions and errors on date/timestamp keys.
    """
    schema = validate_ident(_schema(owner))
    validate_ident(table)
    validate_ident(col)
    n = int(n)
    if n <= 1:
        return slices_from_cuts([])
    fracs = [i / n for i in range(1, n)]
    arr = "ARRAY[" + ",".join("{:.10g}".format(f) for f in fracs) + "]"
    sql = (
        f"SELECT percentile_disc({arr}) WITHIN GROUP (ORDER BY {col}) "
        f"FROM {schema}.{table} WHERE {col} IS NOT NULL"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    vals = list(row[0]) if row and row[0] is not None else []
    cuts = []
    for v in vals:
        if v is None:
            continue
        if not cuts or v != cuts[-1]:
            cuts.append(v)
    return slices_from_cuts(cuts)


# ---------------------------------------------------------------------------
# sizing
# ---------------------------------------------------------------------------
def segment_size_bytes(conn, owner, table):
    """Main-fork on-disk size in bytes via pg_relation_size (table+schema bound)."""
    schema = _schema(owner)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_relation_size(quote_ident(%s) || '.' || quote_ident(%s))",
            (schema, table),
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0
