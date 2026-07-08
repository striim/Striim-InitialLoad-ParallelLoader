"""Source-database dialect abstraction for the splitter / probe.

Oracle, PostgreSQL, SQL Server, and generic JDBC are all implemented; get_dialect()
routes each engine to its concrete SourceDialect. Every engine-specific operation
(boundary discovery, partitions, sizing, watermark, EXPLAIN, row-limiting, predicate
text) lives behind SourceDialect so the probe, the CLI, and the live board stay
engine-agnostic. No DB drivers are imported in this module; concrete dialects import
their driver lazily.
"""

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

# Standard-identifier validation for the generic parse_owner_table default.
_GENERIC_IDENT = re.compile(r"^[A-Za-z0-9_$#]+$")


@dataclass
class Candidate:
    """A way to split the driving table into disjoint slices."""

    kind: str  # "physical" | "column" | "partition"
    key: Optional[str] = None  # column ref for "column"; None for physical/partition
    label: str = ""  # human label for logs ("ROWID", "CREATED_DT", "PARTITION")
    meta: dict = field(default_factory=dict)


@dataclass
class Boundary:
    """One slice's boundary. Opaque to the probe; only render_predicate interprets it.
    physical/column use (lo, hi); partition uses name."""

    lo: Any = None
    hi: Any = None
    name: Optional[str] = None  # partition / subpartition name
    label: str = ""
    is_null: bool = False  # column branch: this slice is the explicit IS NULL cover


@dataclass
class PlanSummary:
    """Result of EXPLAIN on one representative slice."""

    access_path: str  # driving access path, e.g. "TABLE ACCESS BY ROWID RANGE"
    is_disjoint: bool  # True = disjoint range/partition scan (good)
    full_scans: list = field(
        default_factory=list
    )  # large inner tables full-scanned (amplification signal)
    raw: str = ""  # raw plan text, for logging


@dataclass
class TimingResult:
    rows: int
    bytes: int
    seconds: float

    @property
    def rows_per_sec(self):
        return self.rows / self.seconds if self.seconds else 0.0

    @property
    def bytes_per_sec(self):
        return self.bytes / self.seconds if self.seconds else 0.0


class SourceDialect(ABC):
    """Engine-specific operations the probe/splitter need. One concrete subclass per engine."""

    name: str = ""
    watermark_label: str = ""  # e.g. "Oracle SCN", "Postgres WAL LSN", "SQL Server LSN"

    # Physical-range bounds semantics. Oracle ROWID ranges are INCLUSIVE chunks that
    # benefit from coalescing adjacent/overlapping pieces. Half-open range dialects
    # (ctid / clustered-key, lo_next == hi_prev) must NOT coalesce or every adjacent
    # slice merges into one. Default True preserves Oracle; half-open dialects set False.
    coalesce_physical_ranges = True

    # Open the physical-range tails so nothing outside the sampled extremes is missed:
    # the FIRST slice drops its lower bound and the LAST slice drops its upper bound, so
    # rows in blocks allocated below the min / above the max boundary (e.g. inserted
    # between chunking and the read) are still captured. Default False keeps closed
    # ranges; engines whose physical predicate handles open (None) bounds opt in.
    open_physical_tails = False

    @abstractmethod
    def get_connection(self):
        """Return a read-only DBAPI connection from config."""

    @abstractmethod
    def validate_table(self, conn, owner, table) -> None:
        """Raise if OWNER.TABLE is not visible to the account."""

    @abstractmethod
    def is_partitioned(self, conn, owner, table) -> bool: ...

    @abstractmethod
    def list_partitions(self, conn, owner, table, sub=False) -> list:
        """Partition (or subpartition) names in order."""

    @abstractmethod
    def discover_candidates(self, conn, owner, table) -> list:
        """Candidate split keys: physical range, indexed column range(s), partition."""

    @abstractmethod
    def boundaries(self, conn, candidate, n) -> list:
        """Up to n disjoint Boundary objects for the candidate, via bounded/sampled methods."""

    @abstractmethod
    def render_predicate(self, candidate, boundary, alias=None) -> str:
        """Dialect-specific predicate text to inject at ~SPLIT~ for this slice."""

    @abstractmethod
    def segment_size_bytes(self, conn, owner, table) -> int:
        """Approx segment size in bytes, for chunk-count math."""

    @abstractmethod
    def capture_watermark(self, conn) -> dict:
        """CDC catch-up marker: {"label": <watermark_label>, "value": <scn/lsn>}."""

    @abstractmethod
    def explain(self, conn, sql) -> PlanSummary: ...

    @abstractmethod
    def limited(self, sql, n) -> str:
        """Wrap sql so a timing probe fetches at most n rows (subquery wrap)."""

    # -- engine-aware reconcile (concrete defaults; subclasses may override) -
    def parse_owner_table(self, spec):
        """Split ``OWNER.TABLE`` (or bare ``TABLE``) into ``(owner, table)``.

        Generic default: strip, split on the LAST '.', and validate each part as
        a standard identifier (``^[A-Za-z0-9_$#]+$``). ``owner`` is None when the
        spec has no dot. Engine subclasses (e.g. Oracle) override to apply their
        own identifier rules. Raises ValueError on a malformed identifier.
        """
        s = (spec or "").strip()
        if "." in s:
            owner, table = s.rsplit(".", 1)
            owner = owner.strip()
        else:
            owner, table = None, s
        table = table.strip()
        if owner is not None and not _GENERIC_IDENT.match(owner):
            raise ValueError(f"invalid owner identifier: {owner!r}")
        if not _GENERIC_IDENT.match(table):
            raise ValueError(f"invalid table identifier: {table!r}")
        return (owner, table)

    def snapshot_enable_sql(self, watermark):
        """Engine SQL to pin the session to a consistent point-in-time snapshot.

        Powers engine-aware reconcile: ``watermark`` is the captured
        ``{"label", "value"}`` marker (or its raw value). Default returns None,
        meaning this engine offers no consistent snapshot, so the caller falls
        back to live counts.
        """
        return None

    def snapshot_disable_sql(self):
        """Engine SQL to release the point-in-time snapshot enabled above.

        Default returns None (no snapshot to release); the caller skips it.
        """
        return None

    def is_snapshot_lost(self, exc):
        """Whether ``exc`` means the point-in-time snapshot expired mid-reconcile.

        Powers engine-aware reconcile fallback to live counts. Default False —
        engines with no snapshot concept never lose one.
        """
        return False

    # -- parallel-query hint (concrete default; engines override) -----------
    def parallel_hint(self, degree):
        """Inner optimizer-hint text requesting parallel query at ``degree`` (no ``/*+ */``
        wrapper), or None when parallelism does not apply.

        Powers the probe's parallel-degree sweep. Default None — an engine with no
        inline parallel-hint concept never emits one, so the sweep is skipped for it.
        Degree <= 1 (or None) means "no parallelism"; concrete engines return None then.
        """
        return None

    # -- partition rendering (concrete defaults; engines override as needed) -
    def partition_specs(self, conn, owner, table, sub=False):
        """Opaque per-partition specs handed back to ``render_partition_line``.

        Default: the partition names from ``list_partitions`` — correct for engines
        that address a partition by name (Oracle) or by child-relation name
        (PostgreSQL). Engines needing extra catalog metadata to address a partition
        (e.g. SQL Server's partition function + partitioning column) override this to
        bundle that metadata into each spec while ``conn`` is still open, so that
        ``render_partition_line`` stays pure (no DB).
        """
        return self.list_partitions(conn, owner, table, sub=sub)

    def render_partition_line(
        self, query, owner, table, spec, target, alias=None, sub=False
    ):
        """One ``query|target`` line scanning exactly the partition named by ``spec``.

        Default (Oracle): rewrite the driving table reference to
        ``OWNER.TABLE PARTITION (spec)`` (``SUBPARTITION`` when ``sub``). Engines whose
        partitions are not addressed by an inline partition-extended table reference
        override this — PostgreSQL swaps in the child relation, SQL Server injects a
        ``$PARTITION`` predicate. ``alias`` is provided for engines that must qualify a
        column; the Oracle default ignores it. ``spec`` is whatever ``partition_specs``
        returned for this engine.
        """
        import query_split as qs

        return qs.render_partition_line(query, owner, table, spec, target, sub=sub)


def get_dialect(name=None) -> SourceDialect:
    """Return the SourceDialect for `name` (defaults to config.SOURCE_DB_TYPE)."""
    import config

    name = (name or getattr(config, "SOURCE_DB_TYPE", "oracle")).lower()
    if name == "oracle":
        from oracle_dialect import OracleDialect

        return OracleDialect()
    if name in ("postgres", "postgresql", "pg"):
        from postgres_dialect import PostgresDialect

        return PostgresDialect()
    if name in ("sqlserver", "mssql"):
        from sqlserver_dialect import SqlServerDialect

        return SqlServerDialect()
    if name == "jdbc":
        from jdbc_dialect import JdbcDialect

        return JdbcDialect()
    raise ValueError(f"unknown SOURCE_DB_TYPE: {name!r}")
