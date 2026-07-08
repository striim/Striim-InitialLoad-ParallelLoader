"""Oracle concrete SourceDialect for the probe / splitter.

Import-safe WITHOUT python-oracledb: nothing here imports `oracledb` at module
load time. Only `get_connection()` touches it, and it does so lazily via
`oracle_client.get_connection()`. All boundary discovery / sizing delegates to
`oracle_boundaries`; identifiers are validated there before interpolation and
data values are bound. The probe bake-off covers PHYSICAL (ROWID) and COLUMN
candidates — both are WHERE predicates injected at ~SPLIT~. Partition splitting
stays in the existing split path, so render_predicate never handles partitions.
"""

import datetime
import uuid
from decimal import Decimal

import oracle_boundaries
from oracle_boundaries import BoundaryError
from source_dialect import Boundary, Candidate, PlanSummary, SourceDialect

# A FULL scan only signals join amplification when the inner table is large; tiny
# lookup tables are cheap to full-scan and would otherwise collapse the bake-off.
FULL_SCAN_AMPLIFY_ROWS = 1_000_000


class OracleDialect(SourceDialect):
    name = "oracle"
    watermark_label = "Oracle SCN"

    # ROWID chunks cover the segment as of chunking time; opening the first/last tails
    # (render_predicate handles the None bounds) also captures rows in blocks allocated
    # below the min / above the max ROWID after chunking.
    open_physical_tails = True

    # -- connection ---------------------------------------------------------
    def get_connection(self):
        import oracle_client

        return oracle_client.get_connection()

    def validate_table(self, conn, owner, table):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM all_tables WHERE owner = :o AND table_name = :t",
                o=owner,
                t=table,
            )
            if cur.fetchone() is None:
                raise BoundaryError(
                    f"Table {owner}.{table} not visible to ORACLE_USER (existence/privilege)."
                )

    # -- partitions (delegate) ---------------------------------------------
    def is_partitioned(self, conn, owner, table):
        return oracle_boundaries.is_partitioned(conn, owner, table)

    def list_partitions(self, conn, owner, table, sub=False):
        return oracle_boundaries.list_partitions(conn, owner, table, sub=sub)

    # -- candidate discovery -----------------------------------------------
    def discover_candidates(self, conn, owner, table):
        """Physical (ROWID) first, then up to the top 3 column-range keys.

        No partition candidate here — the probe checks is_partitioned separately.
        """
        candidates = [
            Candidate(
                kind="physical",
                key=None,
                label="ROWID",
                meta={"owner": owner, "table": table},
            )
        ]
        for col in oracle_boundaries.discover_split_columns(conn, owner, table)[:3]:
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
        owner = candidate.meta["owner"]
        table = candidate.meta["table"]
        if candidate.kind == "physical":
            ranges = oracle_boundaries.rowid_ranges(conn, owner, table, n)
            return [Boundary(lo=lo, hi=hi, label="ROWID") for lo, hi in ranges]
        if candidate.kind == "column":
            ranges = oracle_boundaries.column_range_boundaries(
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
            rowid = f"{alias}.ROWID" if alias else "ROWID"
            lo, hi = boundary.lo, boundary.hi
            # Open tails (set by build_lines on the first/last slice): drop the lower
            # bound on the first slice and the upper bound on the last so no row is
            # missed. Both None => a single slice covering the whole table.
            if lo is None and hi is None:
                return "1=1"
            if lo is None:
                return f"{rowid} <= '{hi}'"
            if hi is None:
                return f"{rowid} >= '{lo}'"
            return f"{rowid} BETWEEN '{lo}' AND '{hi}'"
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
            return f"{col} >= {self._format_literal(lo)} AND {col} < {self._format_literal(hi)}"
        raise ValueError(f"unsupported candidate kind: {candidate.kind!r}")

    def _format_literal(self, v):
        """Pure: format a Python value as an Oracle SQL literal (no DB)."""
        if isinstance(v, (int, float, Decimal)):
            return str(v)
        if (
            isinstance(v, datetime.datetime)
            and v.tzinfo is not None
            and v.utcoffset() is not None
        ):
            total = int(v.utcoffset().total_seconds())
            sign = "+" if total >= 0 else "-"
            total = abs(total)
            hh, mm = total // 3600, (total % 3600) // 60
            return (
                "TO_TIMESTAMP_TZ('"
                f"{v:%Y-%m-%d %H:%M:%S}.{v.microsecond:06d} {sign}{hh:02d}:{mm:02d}"
                "','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')"
            )
        if isinstance(v, datetime.datetime):
            if v.microsecond:
                return (
                    "TO_TIMESTAMP('"
                    f"{v:%Y-%m-%d %H:%M:%S}.{v.microsecond:06d}"
                    "','YYYY-MM-DD HH24:MI:SS.FF6')"
                )
            return f"TO_DATE('{v:%Y-%m-%d %H:%M:%S}','YYYY-MM-DD HH24:MI:SS')"
        if isinstance(v, datetime.date):
            return f"TO_DATE('{v:%Y-%m-%d}','YYYY-MM-DD')"
        s = str(v).replace("'", "''")
        return f"'{s}'"

    # -- parallel-query hint ------------------------------------------------
    def parallel_hint(self, degree):
        """Oracle statement-level PARALLEL hint text, or None for degree <= 1."""
        try:
            d = int(degree)
        except (TypeError, ValueError):
            return None
        return f"PARALLEL({d})" if d > 1 else None

    # -- sizing (delegate) --------------------------------------------------
    def segment_size_bytes(self, conn, owner, table):
        return oracle_boundaries.segment_size_bytes(conn, owner, table)

    # -- CDC watermark ------------------------------------------------------
    def capture_watermark(self, conn):
        """Capture the source SCN; never raise (degrade to value=None)."""
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_scn FROM v$database")
                row = cur.fetchone()
            return {"label": self.watermark_label, "value": row[0] if row else None}
        except Exception:  # noqa: BLE001 - fall back to flashback, then warn
            pass
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM dual")
                row = cur.fetchone()
            return {"label": self.watermark_label, "value": row[0] if row else None}
        except Exception:  # noqa: BLE001 - watermark.capture_and_log logs the warning
            return {"label": self.watermark_label, "value": None}

    # -- EXPLAIN ------------------------------------------------------------
    def explain(self, conn, sql):
        # Isolate each EXPLAIN with a unique statement_id: PLAN_TABLE rows persist for
        # the session, so the probe's successive EXPLAINs would otherwise mix together.
        sid = "PLL_" + uuid.uuid4().hex[:24]
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN PLAN SET STATEMENT_ID = '{sid}' FOR {sql}")
            cur.execute(
                "SELECT id, operation, options, object_name, cardinality FROM plan_table "
                "WHERE statement_id = :sid ORDER BY id",
                sid=sid,
            )
            rows = cur.fetchall()
            try:
                cur.execute("DELETE FROM plan_table WHERE statement_id = :sid", sid=sid)
            except Exception:  # noqa: BLE001 - cleanup is best-effort
                pass
        return self._summarize_plan(rows)

    def _summarize_plan(self, rows):
        """Pure: distil plan_table rows (id, operation, options, object_name, cardinality)."""
        access_path = ""
        for _id, op, opt, _obj, _card in rows:
            if op == "TABLE ACCESS":
                access_path = f"{op} {opt or ''}".strip()
                break
        if not access_path and rows:
            first = next((r for r in rows if r[0] == 0), rows[0])
            access_path = f"{first[1]} {first[2] or ''}".strip()

        full_scans = [
            obj
            for _id, op, opt, obj, card in rows
            if op == "TABLE ACCESS"
            and (opt or "").upper() == "FULL"
            and (card or 0) > FULL_SCAN_AMPLIFY_ROWS
        ]

        is_disjoint = any(
            "ROWID" in (opt or "").upper()
            or "PARTITION" in (opt or "").upper()
            or (op == "INDEX" and "RANGE" in (opt or "").upper())
            for _id, op, opt, _obj, _card in rows
        )

        raw = "\n".join(
            f"{op} {opt or ''} {obj or ''}".rstrip()
            for _id, op, opt, obj, _card in rows
        )
        return PlanSummary(
            access_path=access_path,
            is_disjoint=is_disjoint,
            full_scans=full_scans,
            raw=raw,
        )

    # -- bounded timing wrap (pure) ----------------------------------------
    def limited(self, sql, n):
        return f"SELECT * FROM ({sql}) WHERE ROWNUM <= {int(n)}"

    # -- engine-aware reconcile (snapshot / identifier overrides) -----------
    def parse_owner_table(self, spec):
        """Oracle OWNER.TABLE parsing (uppercases; allows $ and # in idents)."""
        return oracle_boundaries.parse_owner_table(spec)

    def snapshot_enable_sql(self, watermark):
        """Oracle flashback PL/SQL pinning the session to the captured SCN.

        DBMS_FLASHBACK (rather than inline ``AS OF SCN``) applies the SCN to the
        whole query, including joins and inline views. Returns None when the SCN
        is missing so the caller falls back to live counts.
        """
        scn = watermark["value"] if isinstance(watermark, dict) else watermark
        if not scn:
            return None
        return f"BEGIN DBMS_FLASHBACK.ENABLE_AT_SYSTEM_CHANGE_NUMBER({int(scn)}); END;"

    def snapshot_disable_sql(self):
        """Oracle PL/SQL releasing session-level flashback (call in try/finally)."""
        return "BEGIN DBMS_FLASHBACK.DISABLE; END;"

    def is_snapshot_lost(self, exc):
        """ORA-01555 (snapshot too old) — by string or exc.args[0].code == 1555."""
        if "ORA-01555" in str(exc):
            return True
        args = getattr(exc, "args", None)
        if args:
            return getattr(args[0], "code", None) == 1555
        return False
