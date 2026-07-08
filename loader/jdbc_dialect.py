"""Generic, best-effort JDBC SourceDialect for the probe / splitter.

Import-safe WITHOUT JayDeBeApi / JPype / Java: nothing here imports `jaydebeapi`
at module load time. Only `get_connection()` touches it, and it does so lazily —
mirroring `oracle_client.get_connection()`'s lazy-import pattern. This module
(and its tests) therefore import clean on a box that has no JDBC stack at all.

This dialect is intentionally BEST-EFFORT. It targets *any* JDBC-capable source
(SAP HANA, MySQL, DB2, Teradata, ...) with minimal configuration, accepting a
less-optimized split in exchange for portability:

  * No portable catalog → no `validate_table` via system tables (we probe with a
    1-row SELECT instead), no partition awareness, and no physical/ROWID split.
  * Column-range splitting only, with the split column introspected from
    result-set metadata (java.sql.Types) when possible, else operator-supplied.
  * No portable EXPLAIN and no portable segment sizing.

The operator is expected to pass an explicit chunk/slice count (sizing returns 0)
and, when introspection can't find a numeric/date column, an explicit split
column.
"""

import datetime
import logging
from decimal import Decimal

import config
from source_dialect import Boundary, Candidate, PlanSummary, SourceDialect

logger = logging.getLogger("parallelloader.jdbc_dialect")


class JdbcDialectError(Exception):
    """Configuration / connectivity / validation failure for the JDBC dialect."""


# java.sql.Types codes for columns that can be linearly range-divided (numeric +
# temporal). JayDeBeApi reports cursor.description[i][1] as the integer
# java.sql.Types code returned by ResultSetMetaData.getColumnType().
_RANGE_JDBC_TYPES = frozenset(
    {
        -7,  # BIT
        -6,  # TINYINT
        -5,  # BIGINT
        5,  # SMALLINT
        4,  # INTEGER
        2,  # NUMERIC
        3,  # DECIMAL
        6,  # FLOAT
        7,  # REAL
        8,  # DOUBLE
        91,  # DATE
        92,  # TIME
        93,  # TIMESTAMP
        2013,  # TIME_WITH_TIMEZONE
        2014,  # TIMESTAMP_WITH_TIMEZONE
    }
)

# Fallback substrings when a driver reports a type name/object instead of an int.
_RANGE_TYPE_NAME_TOKENS = (
    "INT",
    "NUM",
    "DEC",
    "FLOAT",
    "REAL",
    "DOUBLE",
    "DATE",
    "TIME",
    "TIMESTAMP",
)


class JdbcDialect(SourceDialect):
    name = "jdbc"
    watermark_label = "JDBC watermark"
    # Generic JDBC splits on half-open column ranges, never inclusive ROWID chunks.
    coalesce_physical_ranges = False

    # -- helpers ------------------------------------------------------------
    @staticmethod
    def _qualify(owner, table):
        """``owner.table`` (or bare ``table`` when owner is falsy)."""
        return f"{owner}.{table}" if owner else f"{table}"

    # -- connection ---------------------------------------------------------
    def get_connection(self):
        """Open a JDBC connection via JayDeBeApi (lazy import — not installed here).

        Mirrors ``oracle_client.get_connection``: validate config, then import the
        driver lazily so this module stays import-safe without a Java/JDBC stack.
        """
        driver = getattr(config, "JDBC_DRIVER_CLASS", "")
        url = getattr(config, "JDBC_URL", "")
        jar = getattr(config, "JDBC_JAR_PATH", "")
        user = getattr(config, "JDBC_USER", "")
        pwd = getattr(config, "JDBC_PASSWORD", "")
        missing = [
            n for n, v in (("JDBC_DRIVER_CLASS", driver), ("JDBC_URL", url)) if not v
        ]
        if missing:
            raise JdbcDialectError("Missing JDBC settings: " + ", ".join(missing))
        try:
            import jaydebeapi
        except ImportError as e:
            raise JdbcDialectError(
                "JayDeBeApi not installed; run: pip install JayDeBeApi JPype1 "
                "(requires Java on PATH)"
            ) from e
        return jaydebeapi.connect(
            driver, url, {"user": user, "password": pwd}, jar or None
        )

    # -- validation (best-effort, no portable catalog) ----------------------
    def validate_table(self, conn, owner, table):
        """Best-effort: probe with a 1-row SELECT (no portable catalog to query).

        Raises ``JdbcDialectError`` with a clear message if the table can't be
        read (missing object, privilege, or a SQL/identifier the source rejects).
        """
        fq = self._qualify(owner, table)
        probe_sql = self.limited(f"SELECT * FROM {fq}", 1)
        try:
            cur = conn.cursor()
            try:
                cur.execute(probe_sql)
                cur.fetchone()
            finally:
                cur.close()
        except Exception as e:  # noqa: BLE001 - surface as a clear dialect error
            raise JdbcDialectError(
                f"JDBC table {fq} is not readable by the configured account "
                f"(existence / privilege / SQL): {e}"
            ) from e

    # -- partitions (no portable catalog) -----------------------------------
    def is_partitioned(self, conn, owner, table):
        """No portable partition catalog across JDBC sources -> always False."""
        return False

    def list_partitions(self, conn, owner, table, sub=False):
        """No portable partition catalog across JDBC sources -> empty list."""
        return []

    # -- candidate discovery (column-range only, best-effort) ---------------
    def discover_candidates(self, conn, owner, table):
        """Best-effort column-range candidates only — NO physical/ROWID candidate.

        Introspects result-set metadata of ``SELECT * FROM t WHERE 1=0`` and
        returns up to 2 numeric/date columns as ``Candidate(kind="column", ...)``.
        If nothing range-divisible can be found (or introspection fails), returns
        ``[]`` and logs that an explicit split column is required.
        """
        fq = self._qualify(owner, table)
        try:
            cur = conn.cursor()
            try:
                cur.execute(f"SELECT * FROM {fq} WHERE 1=0")
                description = list(cur.description or [])
            finally:
                cur.close()
        except Exception as e:  # noqa: BLE001 - degrade to "needs explicit column"
            logger.warning(
                "JDBC discover_candidates: could not introspect %s (%s); an "
                "explicit split column is required.",
                fq,
                e,
            )
            return []

        cols = self._range_columns(description)
        if not cols:
            logger.warning(
                "JDBC discover_candidates: no numeric/date column found on %s via "
                "result-set metadata; an explicit split column is required.",
                fq,
            )
            return []
        return [
            Candidate(
                kind="column",
                key=col,
                label=col,
                meta={"owner": owner, "table": table},
            )
            for col in cols[:2]
        ]

    def _range_columns(self, description):
        """Pure: column names from a DBAPI description that look range-divisible."""
        out = []
        for entry in description or []:
            if not entry:
                continue
            name = entry[0]
            type_code = entry[1] if len(entry) > 1 else None
            if name and self._is_range_type(type_code):
                out.append(name)
        return out

    @staticmethod
    def _is_range_type(type_code):
        """Pure: True if a JDBC type code/name is numeric or temporal.

        Primary path: ``type_code`` is an int java.sql.Types code (what JayDeBeApi
        reports). Fallback: some drivers surface a type *name*/object — match a few
        substrings. ``bool`` is excluded (it is an ``int`` subclass).
        """
        if isinstance(type_code, bool) or type_code is None:
            return False
        if isinstance(type_code, int):
            return type_code in _RANGE_JDBC_TYPES
        name = str(type_code).upper()
        return any(tok in name for tok in _RANGE_TYPE_NAME_TOKENS)

    # -- boundaries (column only) -------------------------------------------
    def boundaries(self, conn, candidate, n):
        """Up to n half-open column ranges from ``MIN(col)/MAX(col)`` + IS NULL cover.

        Defensive: if the column has no usable [min, max] (empty / all-NULL /
        single value) or isn't linearly divisible, fall back to a SINGLE
        whole-table slice (rendered as ``1=1``) and log a warning.
        """
        if candidate.kind != "column":
            raise ValueError(
                f"JdbcDialect only splits on columns; got kind={candidate.kind!r}"
            )
        owner = candidate.meta.get("owner")
        table = candidate.meta.get("table")
        col = candidate.key
        fq = self._qualify(owner, table)
        lo, hi = self._min_max(conn, fq, col)

        if lo is None or hi is None or lo == hi:
            logger.warning(
                "JDBC boundaries: %s.%s has no usable [min,max] range "
                "(min=%r, max=%r); using a single whole-table slice.",
                fq,
                col,
                lo,
                hi,
            )
            return [Boundary(lo=None, hi=None, label=col)]

        cuts = self._cut_points(lo, hi, n)
        if not cuts:
            logger.warning(
                "JDBC boundaries: column %s on %s is not range-divisible "
                "(min=%r, max=%r); using a single whole-table slice.",
                col,
                fq,
                lo,
                hi,
            )
            return [Boundary(lo=None, hi=None, label=col)]

        bnds = []
        prev = None  # first slice is open below to catch the exact MIN
        for cut in cuts:
            bnds.append(Boundary(lo=prev, hi=cut, label=col))
            prev = cut
        bnds.append(Boundary(lo=prev, hi=None, label=col))  # open above -> exact MAX
        bnds.append(Boundary(is_null=True, label=col))  # explicit NULL cover
        return bnds

    def _min_max(self, conn, fq, col):
        """MIN/MAX of ``col`` over the table (returns (None, None) on empty)."""
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT MIN({col}), MAX({col}) FROM {fq}")
            row = cur.fetchone()
        finally:
            cur.close()
        if not row:
            return (None, None)
        return (row[0], row[1])

    @staticmethod
    def _cut_points(lo, hi, n):
        """Pure: n-1 interior cut points dividing [lo, hi] into n equal slices.

        Supports int / float / Decimal and date / datetime (via timedelta). Returns
        ``None`` for any other (non-range-divisible) type, and de-duplicates so a
        tiny range never yields zero-width slices. ``bool`` is treated as
        non-divisible.
        """
        n = max(int(n), 1)
        if n <= 1:
            return []
        if isinstance(lo, bool) or isinstance(hi, bool):
            return None

        cuts = []
        if isinstance(lo, int) and isinstance(hi, int):
            span = hi - lo
            cuts = [lo + span * i // n for i in range(1, n)]
        elif isinstance(lo, (int, float, Decimal)) and isinstance(
            hi, (int, float, Decimal)
        ):
            span = hi - lo
            for i in range(1, n):
                frac = Decimal(i) / Decimal(n) if isinstance(span, Decimal) else i / n
                cuts.append(lo + span * frac)
        elif isinstance(lo, (datetime.date, datetime.datetime)) and isinstance(
            hi, (datetime.date, datetime.datetime)
        ):
            span = hi - lo  # timedelta
            cuts = [lo + span * (i / n) for i in range(1, n)]
        else:
            return None

        deduped = []
        for c in cuts:
            if c not in deduped:
                deduped.append(c)
        return deduped

    # -- predicate rendering (pure) -----------------------------------------
    def render_predicate(self, candidate, boundary, alias=None):
        """Return only the predicate text; the caller injects it at ~SPLIT~.

        Column semantics (half-open, plus an explicit NULL cover):
          * ``is_null``                 -> ``col IS NULL``
          * ``lo is None and hi is None`` -> ``1=1`` (whole-table single slice;
            includes NULLs — this is the boundaries() fallback)
          * ``lo is None``              -> ``col < hi``
          * ``hi is None``              -> ``col >= lo``
          * both present                -> ``col >= lo AND col < hi``

        Literals come from the GENERIC ``_format_literal``. NOTE: literal
        formatting is the known-hard, per-vendor-divergent part of a generic JDBC
        dialect (date/timestamp syntax especially); the defaults here are a
        reasonable baseline, not a guarantee for every source.
        """
        if candidate.kind != "column":
            raise ValueError(
                f"JdbcDialect only renders column predicates; "
                f"got kind={candidate.kind!r}"
            )
        key = candidate.key
        col = f"{alias}.{key}" if alias and "." not in key else key
        if getattr(boundary, "is_null", False):
            return f"{col} IS NULL"
        lo, hi = boundary.lo, boundary.hi
        if lo is None and hi is None:
            return "1=1"  # whole-table cover (keeps NULLs in this single slice)
        if lo is None:
            return f"{col} < {self._format_literal(hi)}"
        if hi is None:
            return f"{col} >= {self._format_literal(lo)}"
        return (
            f"{col} >= {self._format_literal(lo)} "
            f"AND {col} < {self._format_literal(hi)}"
        )

    def _format_literal(self, v):
        """Pure: format a Python value as a generic SQL literal (no DB).

        Defaults (intentionally vendor-neutral):
          * int / float / Decimal -> raw (``42``, ``1.5``, ``3.14``)
          * bool                  -> ``1`` / ``0`` (guarded: bool is an int subclass)
          * str / other           -> single-quoted, ``'`` doubled to ``''``
          * date                  -> ``'YYYY-MM-DD'``
          * datetime              -> ``'YYYY-MM-DD HH:MM:SS'``

        Literal formatting is the known-hard, per-vendor-divergent part of a
        generic JDBC dialect — temporal syntax in particular varies widely (Oracle
        wants ``TO_DATE(...)``, others accept ANSI strings or ``{ts '...'}``
        escapes). These are a reasonable default; vendor-specific sources may need
        a tailored dialect.
        """
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, (int, float, Decimal)):
            return str(v)
        if isinstance(v, datetime.datetime):
            return f"'{v:%Y-%m-%d %H:%M:%S}'"
        if isinstance(v, datetime.date):
            return f"'{v:%Y-%m-%d}'"
        s = str(v).replace("'", "''")
        return f"'{s}'"

    # -- sizing (no portable size) ------------------------------------------
    def segment_size_bytes(self, conn, owner, table):
        """No portable segment-size query across JDBC sources -> 0.

        Returns 0 (never raises) to signal "unknown"; the operator should pass an
        explicit chunk/slice count rather than relying on size-based math.
        """
        return 0

    # -- CDC watermark (best-effort) ----------------------------------------
    def capture_watermark(self, conn):
        """Run ``config.JDBC_WATERMARK_SQL`` if set; never raise.

        Returns ``{"label": watermark_label, "value": <first cell or None>}``. When
        no watermark SQL is configured (or the query fails) the value is None and a
        one-line warning is logged so the operator sets the CDC start point by hand.
        """
        sql = (getattr(config, "JDBC_WATERMARK_SQL", "") or "").strip()
        if not sql:
            logger.warning(
                "No JDBC_WATERMARK_SQL configured; the CDC start point must be "
                "set manually."
            )
            return {"label": self.watermark_label, "value": None}
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                row = cur.fetchone()
            finally:
                cur.close()
            return {"label": self.watermark_label, "value": row[0] if row else None}
        except Exception as e:  # noqa: BLE001 - best-effort, degrade to value=None
            logger.warning(
                "JDBC watermark query failed (%s); the CDC start point must be "
                "set manually.",
                e,
            )
            return {"label": self.watermark_label, "value": None}

    # -- EXPLAIN (stub) -----------------------------------------------------
    def explain(self, conn, sql):
        """Stub: EXPLAIN syntax/output is too DB-specific to generalize.

        Returns a fixed ``PlanSummary`` so the probe treats the slice as disjoint
        (the splitter already guarantees disjoint ranges) without claiming any
        plan insight.
        """
        return PlanSummary(
            access_path="(JDBC — no EXPLAIN)",
            is_disjoint=True,
            full_scans=[],
            raw="",
        )

    # -- bounded timing wrap (pure) -----------------------------------------
    def limited(self, sql, n):
        """Wrap ``sql`` to fetch at most ``n`` rows, per ``JDBC_ROW_LIMIT_SYNTAX``.

        Read at call time so it can be configured per source:
          * ``rownum`` (default) -> ``SELECT * FROM (<sql>) WHERE ROWNUM <= n``
          * ``limit``            -> ``SELECT * FROM (<sql>) _p LIMIT n``
          * ``top``              -> ``SELECT TOP (n) * FROM (<sql>) _p``
          * ``fetch``            -> ``SELECT * FROM (<sql>) _p FETCH FIRST n ROWS ONLY``

        Any unknown value falls back to ``rownum``.
        """
        n = int(n)
        syntax = getattr(config, "JDBC_ROW_LIMIT_SYNTAX", "rownum") or "rownum"
        syntax = syntax.strip().lower()
        if syntax == "limit":
            return f"SELECT * FROM ({sql}) _p LIMIT {n}"
        if syntax == "top":
            return f"SELECT TOP ({n}) * FROM ({sql}) _p"
        if syntax == "fetch":
            return f"SELECT * FROM ({sql}) _p FETCH FIRST {n} ROWS ONLY"
        return f"SELECT * FROM ({sql}) WHERE ROWNUM <= {n}"

    # -- engine-aware reconcile (no snapshot for a generic JDBC source) ------
    def snapshot_enable_sql(self, watermark):
        """No portable point-in-time snapshot across JDBC sources -> None.

        Returning None makes engine-aware reconcile fall back to live counts.
        """
        return None

    def snapshot_disable_sql(self):
        """No snapshot to release (see snapshot_enable_sql) -> None."""
        return None

    def is_snapshot_lost(self, exc):
        """No snapshot concept here, so one can never be lost -> False."""
        return False
