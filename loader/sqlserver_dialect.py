"""SQL Server concrete SourceDialect for the probe / splitter.

Import-safe WITHOUT pyodbc: nothing here imports ``pyodbc`` at module load time.
Only ``get_connection()`` touches it, and it does so lazily, mirroring
``oracle_client.get_connection()``. All catalog / boundary / SHOWPLAN-parse work
delegates to ``sqlserver_boundaries``; identifiers are validated there before
interpolation and data values are bound. The bake-off covers PHYSICAL (the leading
clustered-index key column) and COLUMN candidates — both are WHERE predicates
injected at ~SPLIT~.

Snapshot reconcile: SQL Server cannot pin a session to an arbitrary past LSN, so
snapshot_enable_sql/snapshot_disable_sql return None and is_snapshot_lost is False.
manage.py's reconcile flow already handles None by warning and falling back to live
(not snapshot-anchored) counts.
"""

import datetime
from decimal import Decimal

import sqlserver_boundaries
from sqlserver_boundaries import BoundaryError
from source_dialect import Boundary, Candidate, PlanSummary, SourceDialect

# A FULL scan only signals join amplification when the inner table is large.
FULL_SCAN_AMPLIFY_ROWS = sqlserver_boundaries.FULL_SCAN_AMPLIFY_ROWS


class SqlServerDialect(SourceDialect):
    name = "sqlserver"
    watermark_label = "SQL Server LSN"
    # Clustered-key ranges are HALF-OPEN and gap-free (lo_next == hi_prev) -> never coalesce.
    coalesce_physical_ranges = False

    # -- connection ---------------------------------------------------------
    def get_connection(self):
        """Return a pyodbc connection built from config.SQLSERVER_*.

        ``pyodbc`` is imported lazily HERE only — the module and tests import
        clean without it installed.
        """
        import config

        import pyodbc

        host = config.SQLSERVER_HOST
        port = config.SQLSERVER_PORT
        db = config.SQLSERVER_DATABASE
        user = config.SQLSERVER_USER
        pwd = config.SQLSERVER_PASSWORD
        driver = config.SQLSERVER_DRIVER
        missing = [
            n
            for n, v in (
                ("SQLSERVER_HOST", host),
                ("SQLSERVER_DATABASE", db),
                ("SQLSERVER_USER", user),
            )
            if not v
        ]
        if missing:
            raise BoundaryError("Missing SQL Server settings: " + ", ".join(missing))
        conn_str = (
            f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={db};"
            f"UID={user};PWD={pwd};TrustServerCertificate=yes"
        )
        return pyodbc.connect(conn_str)

    # -- validation ---------------------------------------------------------
    def validate_table(self, conn, owner, table):
        schema = owner or "dbo"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                schema,
                table,
            )
            if cur.fetchone() is None:
                raise BoundaryError(
                    f"Table {schema}.{table} not visible to SQLSERVER_USER "
                    "(existence/privilege)."
                )

    # -- partitions (delegate) ---------------------------------------------
    def is_partitioned(self, conn, owner, table):
        return sqlserver_boundaries.is_partitioned(conn, owner or "dbo", table)

    def list_partitions(self, conn, owner, table, sub=False):
        return sqlserver_boundaries.list_partitions(
            conn, owner or "dbo", table, sub=sub
        )

    # -- candidate discovery -----------------------------------------------
    def discover_candidates(self, conn, owner, table):
        """Physical (leading clustered key) first, then up to 3 indexed columns.

        On a heap (no clustered index) the physical candidate is omitted and only
        column candidates are returned. No partition candidate here — the probe
        checks is_partitioned separately.
        """
        schema = owner or "dbo"
        candidates = []
        keycols = sqlserver_boundaries.clustered_key_columns(conn, schema, table)
        if keycols:
            candidates.append(
                Candidate(
                    kind="physical",
                    key=keycols[0],  # range over the leading clustered key column
                    label=",".join(keycols),
                    meta={"owner": schema, "table": table, "key_cols": keycols},
                )
            )
        for col in sqlserver_boundaries.discover_split_columns(conn, schema, table)[:3]:
            candidates.append(
                Candidate(
                    kind="column",
                    key=col,
                    label=col,
                    meta={"owner": schema, "table": table},
                )
            )
        return candidates

    def boundaries(self, conn, candidate, n):
        owner = candidate.meta["owner"]
        table = candidate.meta["table"]
        key = candidate.key
        ranges = sqlserver_boundaries.ntile_boundaries(conn, owner, table, key, n)
        bnds = [Boundary(lo=lo, hi=hi, label=key) for lo, hi in ranges]
        if candidate.kind == "column":
            # Column keys may be nullable -> add an explicit IS NULL cover so the
            # overall slice cover loses no rows. The clustered key (physical) is
            # NOT NULL, so it needs no IS NULL slice.
            bnds.append(Boundary(is_null=True, label=key))
        return bnds

    # -- predicate rendering (pure) ----------------------------------------
    def render_predicate(self, candidate, boundary, alias=None):
        """Return only the predicate text; the caller injects it at ~SPLIT~.

        Physical (clustered key) and column candidates share the same half-open
        key-range shape; column additionally emits an IS NULL cover slice.
        """
        if candidate.kind not in ("physical", "column"):
            raise ValueError(f"unsupported candidate kind: {candidate.kind!r}")
        key = candidate.key
        col = f"{alias}.{key}" if alias and key and "." not in key else key
        if getattr(boundary, "is_null", False):
            return f"{col} IS NULL"
        lo, hi = boundary.lo, boundary.hi
        if lo is None and hi is None:
            return f"{col} IS NOT NULL"
        if lo is None:
            return f"{col} < {self._format_literal(hi)}"
        if hi is None:
            return f"{col} >= {self._format_literal(lo)}"
        return (
            f"{col} >= {self._format_literal(lo)} "
            f"AND {col} < {self._format_literal(hi)}"
        )

    def _format_literal(self, v):
        """Pure: format a Python value as a T-SQL literal (no DB).

        ints/floats/Decimal -> raw; str -> N'...' (unicode, '' escaping);
        date -> 'YYYY-MM-DD'; datetime -> 'YYYY-MM-DDTHH:MM:SS[.fff]' (ISO 8601,
        milliseconds when sub-second precision is present).
        """
        if isinstance(v, bool):  # bool is a subclass of int; keep it explicit
            return "1" if v else "0"
        if isinstance(v, (int, float, Decimal)):
            return str(v)
        # datetime is a subclass of date -> check datetime first.
        if isinstance(v, datetime.datetime):
            base = f"{v:%Y-%m-%dT%H:%M:%S}"
            if v.microsecond:
                base += f".{v.microsecond // 1000:03d}"  # ms (datetime precision)
            return f"'{base}'"
        if isinstance(v, datetime.date):
            return f"'{v:%Y-%m-%d}'"
        s = str(v).replace("'", "''")
        return f"N'{s}'"

    # -- sizing (delegate) --------------------------------------------------
    def segment_size_bytes(self, conn, owner, table):
        return sqlserver_boundaries.segment_size_bytes(conn, owner or "dbo", table)

    # -- CDC watermark ------------------------------------------------------
    def capture_watermark(self, conn):
        """Capture an LSN; never raise (degrade to value=None).

        Prefers the CDC max LSN (sys.fn_cdc_get_max_lsn); when CDC is not enabled
        that returns NULL, so we fall back to the database timestamp (@@DBTS).
        Binary LSN/timestamp values are returned as a hex string.
        """
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
                row = cur.fetchone()
            val = row[0] if row else None
            if val is not None:
                return {"label": self.watermark_label, "value": _to_hex(val)}
        except Exception:  # noqa: BLE001 - CDC unavailable -> fall back to @@DBTS
            pass
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT @@DBTS")
                row = cur.fetchone()
            val = row[0] if row else None
            return {"label": self.watermark_label, "value": _to_hex(val)}
        except Exception:  # noqa: BLE001 - watermark.capture_and_log logs the warning
            return {"label": self.watermark_label, "value": None}

    # -- EXPLAIN ------------------------------------------------------------
    def explain(self, conn, sql):
        """Estimated plan via SET SHOWPLAN_XML ON; <sql>; SET SHOWPLAN_XML OFF.

        With SHOWPLAN_XML ON the query is NOT executed — the result is the
        estimated plan as an XML string. Parsing is defensive: any cursor or XML
        error degrades to a generic PlanSummary (NEVER raises).
        """
        xml_text = None
        try:
            cur = conn.cursor()
            try:
                cur.execute("SET SHOWPLAN_XML ON")
                cur.execute(sql)
                xml_text = _read_showplan(cur)
            finally:
                try:
                    cur.execute("SET SHOWPLAN_XML OFF")
                except Exception:  # noqa: BLE001 - best-effort reset
                    pass
                try:
                    cur.close()
                except Exception:  # noqa: BLE001 - best-effort close
                    pass
        except Exception:  # noqa: BLE001 - any failure -> generic plan, never raise
            return self._generic_plan()
        return self._summarize_plan(xml_text)

    def _summarize_plan(self, xml_text):
        """Pure: distil a SHOWPLAN_XML string into a PlanSummary."""
        access_path, is_disjoint, full_scans, raw = (
            sqlserver_boundaries.summarize_showplan(
                xml_text, amplify_rows=FULL_SCAN_AMPLIFY_ROWS
            )
        )
        return PlanSummary(
            access_path=access_path,
            is_disjoint=is_disjoint,
            full_scans=full_scans,
            raw=raw,
        )

    def _generic_plan(self):
        return PlanSummary(
            access_path="(showplan unavailable)",
            is_disjoint=False,
            full_scans=[],
            raw="",
        )

    # -- bounded timing wrap (pure) ----------------------------------------
    def limited(self, sql, n):
        # SQL Server requires the derived table to be aliased.
        return f"SELECT TOP ({int(n)}) * FROM ({sql}) _probe"

    # -- engine-aware reconcile (snapshot / identifier overrides) -----------
    def parse_owner_table(self, spec):
        """SQL Server SCHEMA.TABLE parsing (case-preserving; default schema dbo)."""
        owner, table = super().parse_owner_table(spec)
        return (owner or "dbo", table)

    def snapshot_enable_sql(self, watermark):
        """SQL Server cannot time-travel to an arbitrary past LSN.

        Returns None so manage.py's reconcile warns and falls back to LIVE counts
        (not snapshot-anchored).
        """
        return None

    def snapshot_disable_sql(self):
        """No session snapshot to release (see snapshot_enable_sql)."""
        return None

    def is_snapshot_lost(self, exc):
        """No point-in-time snapshot concept -> one is never lost."""
        return False

    # -- partition rendering ($PARTITION function predicate) ----------------
    def partition_specs(self, conn, owner, table, sub=False):
        """Bundle the partition function + partitioning column (one catalog lookup)
        with each partition number, so ``render_partition_line`` stays pure.

        Returns a list of ``{"number", "func", "col"}`` dicts. ``sub`` is ignored
        (SQL Server has no subpartitions). Raises if the table has no resolvable
        partition function/column (i.e. it is not range-partitioned).
        """
        schema = owner or "dbo"
        func, col = sqlserver_boundaries.partition_function_and_column(
            conn, schema, table
        )
        if not func or not col:
            raise BoundaryError(
                f"{schema}.{table} has no resolvable partition function/column "
                "(is it range-partitioned?)."
            )
        numbers = sqlserver_boundaries.list_partitions(conn, schema, table)
        return [{"number": int(n), "func": func, "col": col} for n in numbers]

    def render_partition_line(
        self, query, owner, table, spec, target, alias=None, sub=False
    ):
        """Scan exactly one partition via ``$PARTITION.[func](col) = N`` injected at
        ~SPLIT~. ``spec`` is a ``{"number", "func", "col"}`` dict from
        ``partition_specs``; the column is alias-qualified when ``alias`` is given.
        """
        import query_split as qs

        func = spec["func"]
        col = spec["col"]
        number = int(spec["number"])
        colref = f"{alias}.{col}" if alias and "." not in col else col
        pred = f"$PARTITION.[{func}]({colref}) = {number}"
        return f"{qs.inject_predicate(query, pred)}|{target}"


def _read_showplan(cur):
    """Concatenate the SHOWPLAN_XML result rows into one string (defensive)."""
    try:
        rows = cur.fetchall()
    except Exception:  # noqa: BLE001 - no result set -> empty plan
        return ""
    parts = [str(r[0]) for r in rows if r and r[0]]
    return "".join(parts)


def _to_hex(v):
    """Render a binary LSN/timestamp as a hex string; pass others through as str."""
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "0x" + bytes(v).hex().upper()
    return str(v)
