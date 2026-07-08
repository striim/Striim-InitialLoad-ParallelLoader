"""PostgreSQL concrete SourceDialect for the probe / splitter.

Import-safe WITHOUT psycopg2: nothing here imports the driver at module load
time. Only ``get_connection()`` touches it, and it does so lazily — mirroring
``oracle_client.get_connection()`` — so this module imports clean even if the
driver is absent. All catalog / boundary SQL delegates to ``postgres_boundaries``;
identifiers are validated there before interpolation and data values are bound.

The probe bake-off covers PHYSICAL (``ctid`` block bands) and COLUMN candidates —
both are WHERE predicates injected at ~SPLIT~. Partition splitting stays in the
existing split path, so render_predicate never handles partitions.
"""

import datetime
import json
from decimal import Decimal

import config
import postgres_boundaries
from postgres_boundaries import PostgresError
from source_dialect import Boundary, Candidate, PlanSummary, SourceDialect

# A Seq Scan only signals join amplification when the relation is large; tiny
# lookup tables are cheap to full-scan and would otherwise collapse the bake-off.
FULL_SCAN_AMPLIFY_ROWS = 1_000_000


class PostgresDialect(SourceDialect):
    name = "postgres"
    watermark_label = "PostgreSQL WAL LSN"
    # ctid page bands are HALF-OPEN and gap-free (lo_next == hi_prev) -> never coalesce.
    coalesce_physical_ranges = False

    # -- connection (lazy psycopg2) ----------------------------------------
    def get_connection(self):
        """Open a read-only PostgreSQL connection from config.SOURCE_PG_*.

        psycopg2 is imported HERE (not at module top) so the module stays
        import-safe without the driver, exactly like oracle_client.
        """
        import psycopg2  # lazy: keep module import clean without the driver

        conn = psycopg2.connect(
            host=config.SOURCE_PG_HOST,
            port=config.SOURCE_PG_PORT,
            dbname=config.SOURCE_PG_DATABASE,
            user=config.SOURCE_PG_USER,
            password=config.SOURCE_PG_PASSWORD,
            sslmode=config.SOURCE_PG_SSLMODE,
        )
        conn.autocommit = True  # read-only probing; no write txns to commit
        return conn

    def validate_table(self, conn, owner, table):
        if not postgres_boundaries.table_exists(conn, owner, table):
            schema = owner or "public"
            raise PostgresError(
                f"Table {schema}.{table} not visible to SOURCE_PG_USER (existence/privilege)."
            )

    # -- partitions (delegate) ---------------------------------------------
    def is_partitioned(self, conn, owner, table):
        return postgres_boundaries.is_partitioned(conn, owner, table)

    def list_partitions(self, conn, owner, table, sub=False):
        return postgres_boundaries.list_partitions(conn, owner, table, sub=sub)

    # -- candidate discovery -----------------------------------------------
    def discover_candidates(self, conn, owner, table):
        """Physical (ctid) first, then up to the top 3 indexed column keys."""
        candidates = [
            Candidate(
                kind="physical",
                key=None,
                label="ctid",
                meta={"owner": owner, "table": table},
            )
        ]
        for col in postgres_boundaries.discover_split_columns(conn, owner, table)[:3]:
            candidates.append(
                Candidate(
                    kind="column",
                    key=col,
                    label=col,
                    meta={"owner": owner, "table": table},
                )
            )
        return candidates

    def boundaries(self, conn, candidate, n):
        owner = candidate.meta.get("owner")
        table = candidate.meta.get("table")
        if candidate.kind == "physical":
            ranges = postgres_boundaries.ctid_page_ranges(conn, owner, table, n)
            return [Boundary(lo=lo, hi=hi, label="ctid") for lo, hi in ranges]
        if candidate.kind == "column":
            ranges = postgres_boundaries.column_range_boundaries(
                conn, owner, table, candidate.key, n
            )
            bnds = [Boundary(lo=lo, hi=hi, label=candidate.key) for lo, hi in ranges]
            bnds.append(Boundary(is_null=True, label=candidate.key))
            return bnds
        raise ValueError(f"unsupported candidate kind: {candidate.kind!r}")

    # -- predicate rendering (pure) ----------------------------------------
    def render_predicate(self, candidate, boundary, alias=None):
        """Return only the predicate text; the caller injects it at ~SPLIT~."""
        if candidate.kind == "physical":
            col = f"{alias}.ctid" if alias else "ctid"
            lo, hi = boundary.lo, boundary.hi
            if lo is None and hi is None:
                return f"{col} IS NOT NULL"  # defensive; physical always has bounds
            if lo is None:
                return f"{col} < '({int(hi)},0)'::tid"
            if hi is None:
                return f"{col} >= '({int(lo)},0)'::tid"
            return f"{col} >= '({int(lo)},0)'::tid AND {col} < '({int(hi)},0)'::tid"
        if candidate.kind == "column":
            key = candidate.key
            col = f"{alias}.{key}" if alias and "." not in key else key
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
        raise ValueError(f"unsupported candidate kind: {candidate.kind!r}")

    def _format_literal(self, v):
        """Pure: format a Python value as a PostgreSQL SQL literal (no DB)."""
        if isinstance(v, bool):  # bool is an int subclass — handle first
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float, Decimal)):
            return str(v)
        if isinstance(v, datetime.datetime):  # subclass of date — handle first
            if v.microsecond:
                return f"'{v:%Y-%m-%d %H:%M:%S}.{v.microsecond:06d}'"
            return f"'{v:%Y-%m-%d %H:%M:%S}'"
        if isinstance(v, datetime.date):
            return f"'{v:%Y-%m-%d}'"
        s = str(v).replace("'", "''")
        return f"'{s}'"

    # -- sizing (delegate) --------------------------------------------------
    def segment_size_bytes(self, conn, owner, table):
        return postgres_boundaries.segment_size_bytes(conn, owner, table)

    # -- CDC watermark ------------------------------------------------------
    def capture_watermark(self, conn):
        """Capture the current WAL LSN; never raise (degrade to value=None).

        Best-effort: also export a transaction snapshot id under ``"snapshot"`` so
        reconcile can try to pin to a consistent point-in-time. PG snapshots are
        session/transaction-bound and short-lived, so this usually isn't reusable
        at reconcile time — hence reconcile degrades to live counts.
        """
        result = {"label": self.watermark_label, "value": None}
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_current_wal_lsn()::text")
                row = cur.fetchone()
            if row:
                result["value"] = row[0]
        except Exception:  # noqa: BLE001 - watermark.capture_and_log logs the warning
            return result
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_export_snapshot()")
                row = cur.fetchone()
            if row and row[0]:
                result["snapshot"] = row[0]
        except Exception:  # noqa: BLE001 - snapshot export is opportunistic
            pass
        return result

    # -- EXPLAIN ------------------------------------------------------------
    def explain(self, conn, sql):
        """EXPLAIN (FORMAT JSON); summarize. Never raise — degrade gracefully."""
        try:
            with conn.cursor() as cur:
                cur.execute("EXPLAIN (FORMAT JSON) " + sql)
                row = cur.fetchone()
            plan_json = row[0] if row else None
            return self._summarize_plan(plan_json)
        except Exception:  # noqa: BLE001 - probe must survive an EXPLAIN failure
            return PlanSummary(
                access_path="(EXPLAIN unavailable)",
                is_disjoint=False,
                full_scans=[],
                raw="",
            )

    def _summarize_plan(self, plan_json):
        """Pure: distil an ``EXPLAIN (FORMAT JSON)`` payload into a PlanSummary.

        psycopg2 may hand back the json column already-deserialized (list/dict) or
        as a string; both are accepted. The driving scan node's "Node Type" is the
        access path; Index/Tid scans are disjoint (range/seek), a Seq Scan is not.
        Seq Scans over a large "Plan Rows" relation are flagged as amplification.
        """
        raw = ""
        try:
            if isinstance(plan_json, (bytes, bytearray)):
                plan_json = plan_json.decode("utf-8", "replace")
            if isinstance(plan_json, str):
                raw = plan_json
                data = json.loads(plan_json)
            else:
                data = plan_json
                try:
                    raw = json.dumps(data, separators=(",", ":"), default=str)
                except Exception:  # noqa: BLE001
                    raw = str(data)
            top = data[0] if isinstance(data, list) and data else data
            plan = top.get("Plan", top) if isinstance(top, dict) else {}
            if not isinstance(plan, dict):
                plan = {}
            driving = self._driving_scan_type(plan)
            return PlanSummary(
                access_path=driving,
                is_disjoint=self._is_disjoint_node(driving),
                full_scans=self._collect_full_scans(plan),
                raw=raw,
            )
        except Exception:  # noqa: BLE001 - any malformed plan degrades, never raises
            return PlanSummary(
                access_path="(unparsed plan)",
                is_disjoint=False,
                full_scans=[],
                raw=raw or "",
            )

    @staticmethod
    def _walk(node):
        """Yield this plan node and all nested child nodes (depth-first, top first)."""
        if not isinstance(node, dict):
            return
        yield node
        for child in node.get("Plans", []) or []:
            yield from PostgresDialect._walk(child)

    def _driving_scan_type(self, plan):
        """First scan node's "Node Type" (descends past Limit/Gather/Aggregate)."""
        for node in self._walk(plan):
            nt = node.get("Node Type", "") or ""
            if "Scan" in nt:
                return nt
        return plan.get("Node Type", "") if isinstance(plan, dict) else ""

    @staticmethod
    def _is_disjoint_node(node_type):
        """True for Index/Tid (range/seek) scans; False for a Seq Scan."""
        nt = (node_type or "").lower()
        if "seq scan" in nt:
            return False
        return ("index" in nt) or ("tid" in nt)

    def _collect_full_scans(self, plan):
        """Relations seq-scanned with a large estimated row count (amplification)."""
        out = []
        for node in self._walk(plan):
            if node.get("Node Type") == "Seq Scan":
                try:
                    rows = int(node.get("Plan Rows") or 0)
                except (TypeError, ValueError):
                    rows = 0
                if rows > FULL_SCAN_AMPLIFY_ROWS:
                    rel = node.get("Relation Name") or node.get("Alias") or ""
                    if rel:
                        out.append(rel)
        return out

    # -- bounded timing wrap (pure) ----------------------------------------
    def limited(self, sql, n):
        return f"SELECT * FROM ({sql}) _probe LIMIT {int(n)}"

    # -- engine-aware reconcile (snapshot overrides) ------------------------
    def snapshot_enable_sql(self, watermark):
        """Best-effort PG snapshot pin from a captured ``pg_export_snapshot()`` id.

        PG snapshots are session/transaction-bound and only valid for the lifetime
        of the exporting transaction, so the captured id is usually stale by
        reconcile time. When a snapshot id IS present and well-formed, return SQL to
        pin a fresh REPEATABLE READ transaction to it; otherwise return None so the
        caller falls back to live counts.
        """
        snap = watermark.get("snapshot") if isinstance(watermark, dict) else None
        if not snap:
            return None
        # Snapshot ids look like "00000003-00001B92-1" (hex + hyphens). Reject
        # anything else to keep the interpolation injection-safe.
        import re

        if not re.match(r"^[0-9A-Fa-f/\-]+$", str(snap)):
            return None
        return (
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; "
            f"SET TRANSACTION SNAPSHOT '{snap}'"
        )

    def snapshot_disable_sql(self):
        """No explicit release needed — the snapshot dies with its transaction."""
        return None

    def is_snapshot_lost(self, exc):
        """PG has no SCN-style 'snapshot too old' for this flow — never lost."""
        return False

    # -- partition rendering (declarative child relations) ------------------
    def render_partition_line(
        self, query, owner, table, spec, target, alias=None, sub=False
    ):
        """Scan exactly one declarative partition by swapping the parent reference
        ``OWNER.TABLE`` for the child relation ``OWNER.<child>``.

        ``spec`` is the child relation name from ``list_partitions`` (the base
        ``partition_specs`` default). PostgreSQL has no inline partition syntax and no
        subpartitions, so ``sub`` is unused; the table alias is preserved by the
        table-token swap, so ``alias`` is unused too. The query must reference the
        parent as ``schema.table`` (owner defaults to ``public``).
        """
        import query_split as qs

        schema = owner or "public"
        return qs.render_child_table_line(query, schema, table, spec, target)
