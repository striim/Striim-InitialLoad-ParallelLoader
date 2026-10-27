"""
Live integration tests for the chunking logic against Docker databases.

Run with:  pytest tests/test_chunking_live.py -v
Skip slow Oracle coverage scan: pytest tests/test_chunking_live.py -v -m "not slow"

Credentials (Docker):
  Oracle  : localhost:1521/ORCL, miner/miner, schema SALES
  Postgres: localhost:5433/webaction, waction/w@ct10n, schema public
  MSSQL   : 127.0.0.1:1433/qauser, sa/QAtestuser1, schema dbo
"""

import pytest

import oracle_boundaries as ob
import postgres_boundaries as pb
import query_split as qs
import sqlserver_boundaries as sb
from oracle_dialect import OracleDialect
from postgres_dialect import PostgresDialect
from source_dialect import Candidate
from sqlserver_dialect import SqlServerDialect

# ---------------------------------------------------------------------------
# A nested JSON-aggregation Oracle query: orders with their lines, line notes and
# order attributes.
# Tables are schema-qualified so miner (not SALES) can resolve them.
# The query has no outer WHERE clause — inject_predicate will add one.
# ---------------------------------------------------------------------------
_COMPLEX_ORACLE_QUERY = """SELECT
    s.ORDER_ID,
    s.CUSTOMER_ID,
    s.REGION,
    s.STATUS,
    (SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'line_id'    VALUE r.LINE_ID,
                    'product_id' VALUE r.PRODUCT_ID,
                    'quantity'   VALUE r.QUANTITY,
                    'notes'      VALUE (
                        SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'note_id'   VALUE ar.NOTE_ID,
                                'note_text' VALUE ar.NOTE_TEXT
                            )
                        )
                        FROM SALES.LINE_NOTES ar
                        WHERE ar.LINE_ID = r.LINE_ID
                    )
                )
            )
     FROM SALES.ORDER_LINES r
     WHERE r.ORDER_ID = s.ORDER_ID
    ) AS LINES_JSON,
    (SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'attr_key'   VALUE m.ATTR_KEY,
                    'attr_value' VALUE m.ATTR_VALUE,
                    'store_name' VALUE m.STORE_NAME
                )
            )
     FROM SALES.ORDER_ATTRS m
     WHERE m.ORDER_ID = s.ORDER_ID
    ) AS ATTRS_JSON
FROM SALES.ORDERS s"""


# ---------------------------------------------------------------------------
# Connection fixtures (module-scoped — one connection per test session).
# A missing driver or an unreachable database skips that engine's tests.
# ---------------------------------------------------------------------------


def _connect_or_skip(engine, connect):
    try:
        return connect()
    except Exception as exc:  # noqa: BLE001 - any connect failure means "not available here"
        pytest.skip(f"{engine} not reachable: {exc}")


@pytest.fixture(scope="module")
def ora_conn():
    oracledb = pytest.importorskip("oracledb")
    conn = _connect_or_skip(
        "Oracle", lambda: oracledb.connect(user="miner", password="miner", dsn="localhost:1521/ORCL")
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = _connect_or_skip(
        "Postgres",
        lambda: psycopg2.connect(
            host="localhost",
            port=5433,
            dbname="webaction",
            user="waction",
            password="w@ct10n",
            connect_timeout=5,
        ),
    )
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def ss_conn():
    pyodbc = pytest.importorskip("pyodbc")
    conn = _connect_or_skip(
        "SQL Server",
        lambda: pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;"
            "DATABASE=qauser;UID=sa;PWD=QAtestuser1;TrustServerCertificate=yes",
            timeout=10,
        ),
    )
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helper: count rows for each boundary slice and verify they sum to the full table
# ---------------------------------------------------------------------------

def _ora_count(conn, table_fq, predicate):
    """Return COUNT(*) for ``table_fq`` filtered by ``predicate``."""
    sql = f"SELECT COUNT(*) FROM {table_fq} WHERE {predicate}"
    with conn.cursor() as c:
        c.execute(sql)
        return c.fetchone()[0]


def _pg_count(conn, table_fq, predicate):
    sql = f"SELECT COUNT(*) FROM {table_fq} WHERE {predicate}"
    with conn.cursor() as c:
        c.execute(sql)
        return c.fetchone()[0]


def _ss_count(conn, table_fq, predicate, params=()):
    sql = f"SELECT COUNT(*) FROM {table_fq} WHERE {predicate}"
    with conn.cursor() as c:
        c.execute(sql, *params)
        return c.fetchone()[0]


# ===========================================================================
# Oracle tests
# ===========================================================================


class TestOracleChunking:
    OWNER = "SALES"
    TABLE = "ORDERS"
    TABLE_FQ = "SALES.ORDERS"

    def test_connects(self, ora_conn):
        with ora_conn.cursor() as c:
            c.execute("SELECT USER FROM dual")
            assert c.fetchone()[0] == "MINER"

    def test_discover_split_columns_runs_without_error(self, ora_conn):
        """discover_split_columns must not raise; it may return [] when Oracle stats
        show num_distinct <= 1 (stale / no DBMS_STATS gather).  TIME_CREATED is the
        only numeric column; it appears here only when stats report num_distinct > 1."""
        cols = ob.discover_split_columns(ora_conn, self.OWNER, self.TABLE)
        assert isinstance(cols, list)
        if cols:
            assert all(isinstance(c, str) for c in cols)

    def test_is_partitioned(self, ora_conn):
        result = ob.is_partitioned(ora_conn, self.OWNER, self.TABLE)
        assert isinstance(result, bool)

    @pytest.mark.slow
    def test_rowid_ranges_complete_coverage(self, ora_conn):
        """ROWID slices must collectively count every row in the table."""
        with ora_conn.cursor() as c:
            c.execute(f"SELECT COUNT(*) FROM {self.TABLE_FQ}")
            expected = c.fetchone()[0]

        ranges = ob.rowid_ranges(ora_conn, self.OWNER, self.TABLE, 4)
        assert len(ranges) >= 1

        total = sum(
            _ora_count(ora_conn, self.TABLE_FQ, f"ROWID BETWEEN '{lo}' AND '{hi}'")
            for lo, hi in ranges
        )
        assert total == expected, (
            f"ROWID chunks summed to {total}, expected {expected}"
        )

    @pytest.mark.slow
    def test_column_range_boundaries_complete_coverage(self, ora_conn):
        """Column-range slices on TIME_CREATED + IS NULL must cover every row."""
        with ora_conn.cursor() as c:
            c.execute(f"SELECT COUNT(*) FROM {self.TABLE_FQ}")
            expected = c.fetchone()[0]

        dialect = OracleDialect()
        cand = Candidate(
            kind="column",
            key="TIME_CREATED",
            label="TIME_CREATED",
            meta={"owner": self.OWNER, "table": self.TABLE},
        )
        boundaries = dialect.boundaries(ora_conn, cand, 4)

        total = 0
        for bnd in boundaries:
            pred = dialect.render_predicate(cand, bnd)
            total += _ora_count(ora_conn, self.TABLE_FQ, pred)

        assert total == expected, (
            f"Column-range chunks summed to {total}, expected {expected}"
        )

    def test_rowid_ranges_returns_multiple_slices(self, ora_conn):
        ranges = ob.rowid_ranges(ora_conn, self.OWNER, self.TABLE, 4)
        assert len(ranges) >= 2, "Expected at least 2 ROWID slices for a 3M-row table"

    def test_column_range_boundaries_gap_free(self, ora_conn):
        """slices_from_cuts invariant: each slice's hi == next slice's lo."""
        ranges = ob.column_range_boundaries(
            ora_conn, self.OWNER, self.TABLE, "TIME_CREATED", 4, sample_pct=0.1
        )
        for (_, a_hi), (b_lo, _) in zip(ranges, ranges[1:]):
            assert a_hi == b_lo, f"Gap between slices: {a_hi} != {b_lo}"

    def test_segment_size_bytes_nonzero(self, ora_conn):
        size = ob.segment_size_bytes(ora_conn, self.OWNER, self.TABLE)
        assert size > 0, "Segment size should be positive"

    # -----------------------------------------------------------------------
    # Complex multi-table JSON query — predicate injection
    # -----------------------------------------------------------------------

    def test_complex_query_alias_detected(self):
        """detect_alias must find 's' for SALES.ORDERS in the complex query."""
        alias = qs.detect_alias(_COMPLEX_ORACLE_QUERY, "SALES", "ORDERS")
        assert alias == "s", f"Expected alias 's', got {alias!r}"

    def test_complex_query_split_token_injection(self):
        """~SPLIT~ is the correct way to inject predicates into complex queries that
        contain inner subqueries with their own WHERE clauses.  inject_predicate uses
        a simple regex and cannot distinguish an outer WHERE from an inner one; the
        ~SPLIT~ token sidesteps this ambiguity entirely."""
        query_with_token = _COMPLEX_ORACLE_QUERY + "\nWHERE ~SPLIT~"
        pred = "s.ROWID BETWEEN 'AAAd0FAAHA' AND 'AAAd0FAAHAzz'"
        result = qs.inject_predicate(query_with_token, pred)
        assert pred in result
        assert "~SPLIT~" not in result
        assert "WHERE" in result

    def test_complex_query_with_split_token_replaced(self):
        """If the query has ~SPLIT~, inject_predicate replaces the token."""
        query_with_token = _COMPLEX_ORACLE_QUERY + "\nWHERE ~SPLIT~"
        pred = "s.ROWID BETWEEN 'AAA' AND 'ZZZ'"
        result = qs.inject_predicate(query_with_token, pred)
        assert pred in result
        assert "~SPLIT~" not in result

    def test_complex_query_render_rowid_line(self):
        """render_rowid_line produces a valid pipe-delimited queryfile line."""
        lo, hi = "AAAd0FAAHAHAAAB", "AAAd0FAAHAHAAAz"
        line = qs.render_rowid_line(
            _COMPLEX_ORACLE_QUERY, alias="s", lo=lo, hi=hi, target="SALES.ORDER_DOCS_TGT"
        )
        assert f"s.ROWID BETWEEN '{lo}' AND '{hi}'" in line
        assert line.endswith("|SALES.ORDER_DOCS_TGT")
        assert "~SPLIT~" not in line

    def test_complex_query_first_rowid_chunk_executes(self, ora_conn):
        """Execute one ROWID chunk of the complex query via ~SPLIT~ injection.

        Complex queries with inner subqueries must use the ~SPLIT~ token so
        inject_predicate replaces it in-place instead of appending after the last
        inner WHERE clause.  We wrap in a COUNT(*) to avoid streaming 800K JSON rows.
        """
        query_with_token = _COMPLEX_ORACLE_QUERY + "\nWHERE ~SPLIT~"
        ranges = ob.rowid_ranges(ora_conn, self.OWNER, self.TABLE, 4)
        lo, hi = ranges[0]
        pred = f"s.ROWID BETWEEN '{lo}' AND '{hi}'"
        chunked_query = qs.inject_predicate(query_with_token, pred)
        assert "~SPLIT~" not in chunked_query
        wrapped = f"SELECT COUNT(*) FROM ({chunked_query}) sub"
        with ora_conn.cursor() as c:
            c.execute(wrapped)
            count = c.fetchone()[0]
        assert count >= 0


# ===========================================================================
# PostgreSQL tests
# ===========================================================================


class TestPostgresChunking:
    OWNER = "public"
    TABLE = "column_mappings"
    TABLE_FQ = "public.column_mappings"

    def test_connects(self, pg_conn):
        with pg_conn.cursor() as c:
            c.execute("SELECT current_user")
            assert c.fetchone()[0] == "waction"

    def test_table_exists(self, pg_conn):
        assert pb.table_exists(pg_conn, self.OWNER, self.TABLE)

    def test_is_not_partitioned(self, pg_conn):
        assert pb.is_partitioned(pg_conn, self.OWNER, self.TABLE) is False

    def test_relpages_positive(self, pg_conn):
        pages = pb.relpages(pg_conn, self.OWNER, self.TABLE)
        assert pages > 0

    def test_discover_split_columns_finds_numeric_indexed(self, pg_conn):
        cols = pb.discover_split_columns(pg_conn, self.OWNER, self.TABLE)
        assert len(cols) >= 1, f"Expected at least one split column, got {cols}"
        assert "mapping_id" in cols

    def test_ctid_page_ranges_complete_coverage(self, pg_conn):
        """ctid page bands must count every row in the table.

        ANALYZE is called first to refresh pg_class.relpages; without it, stale
        planner estimates may leave a small tail of rows outside the computed bands.
        """
        with pg_conn.cursor() as c:
            c.execute(f"ANALYZE {self.TABLE_FQ}")
            c.execute(f"SELECT COUNT(*) FROM {self.TABLE_FQ}")
            expected = c.fetchone()[0]

        ranges = pb.ctid_page_ranges(pg_conn, self.OWNER, self.TABLE, 4)
        assert len(ranges) >= 1

        total = sum(
            _pg_count(
                pg_conn,
                self.TABLE_FQ,
                f"ctid >= '({lo},0)'::tid AND ctid < '({hi},0)'::tid",
            )
            for lo, hi in ranges
        )
        assert total == expected, (
            f"ctid chunks summed to {total}, expected {expected}"
        )

    def test_ctid_page_ranges_gap_free(self, pg_conn):
        """Each band's hi == next band's lo (gap-free, half-open)."""
        ranges = pb.ctid_page_ranges(pg_conn, self.OWNER, self.TABLE, 4)
        for (_, a_hi), (b_lo, _) in zip(ranges, ranges[1:]):
            assert a_hi == b_lo, f"Gap between ctid bands: {a_hi} != {b_lo}"

    def test_column_range_boundaries_complete_coverage(self, pg_conn):
        """Percentile-disc slices on mapping_id + IS NULL must cover every row."""
        with pg_conn.cursor() as c:
            c.execute(f"SELECT COUNT(*) FROM {self.TABLE_FQ}")
            expected = c.fetchone()[0]

        dialect = PostgresDialect()
        cand = Candidate(
            kind="column",
            key="mapping_id",
            label="mapping_id",
            meta={"owner": self.OWNER, "table": self.TABLE},
        )
        boundaries = dialect.boundaries(pg_conn, cand, 4)

        total = sum(
            _pg_count(pg_conn, self.TABLE_FQ, dialect.render_predicate(cand, bnd))
            for bnd in boundaries
        )
        assert total == expected, (
            f"Column-range chunks summed to {total}, expected {expected}"
        )

    def test_column_range_boundaries_gap_free(self, pg_conn):
        ranges = pb.column_range_boundaries(pg_conn, self.OWNER, self.TABLE, "mapping_id", 4)
        for (_, a_hi), (b_lo, _) in zip(ranges, ranges[1:]):
            assert a_hi == b_lo

    def test_segment_size_bytes_nonzero(self, pg_conn):
        size = pb.segment_size_bytes(pg_conn, self.OWNER, self.TABLE)
        assert size > 0

    def test_ctid_predicate_render_correct_syntax(self, pg_conn):
        """Spot-check that the rendered ctid predicate parses without DB error."""
        ranges = pb.ctid_page_ranges(pg_conn, self.OWNER, self.TABLE, 2)
        lo, hi = ranges[0]
        pred = f"ctid >= '({lo},0)'::tid AND ctid < '({hi},0)'::tid"
        with pg_conn.cursor() as c:
            c.execute(f"SELECT 1 FROM {self.TABLE_FQ} WHERE {pred} LIMIT 1")


# ===========================================================================
# SQL Server tests
# ===========================================================================


class TestSqlServerChunking:
    OWNER = "dbo"
    TABLE = "customer"
    TABLE_FQ = "[dbo].[customer]"

    def test_connects(self, ss_conn):
        with ss_conn.cursor() as c:
            c.execute("SELECT SYSTEM_USER")
            assert c.fetchone()[0] == "sa"

    def test_clustered_key_columns_finds_customer_id(self, ss_conn):
        cols = sb.clustered_key_columns(ss_conn, self.OWNER, self.TABLE)
        assert cols == ["customer_id"]

    def test_is_not_partitioned(self, ss_conn):
        assert sb.is_partitioned(ss_conn, self.OWNER, self.TABLE) is False

    def test_discover_split_columns_excludes_clustered_key(self, ss_conn):
        """discover_split_columns must not return the leading clustered key."""
        lead = sb.clustered_key_columns(ss_conn, self.OWNER, self.TABLE)[0]
        cols = sb.discover_split_columns(ss_conn, self.OWNER, self.TABLE)
        assert lead not in cols

    def test_ntile_boundaries_complete_coverage(self, ss_conn):
        """NTILE-sampled slices on customer_id + IS NULL must cover every row."""
        with ss_conn.cursor() as c:
            c.execute(f"SELECT COUNT(*) FROM {self.TABLE_FQ}")
            expected = c.fetchone()[0]

        dialect = SqlServerDialect()
        cand = Candidate(
            kind="physical",
            key="customer_id",
            label="customer_id",
            meta={"owner": self.OWNER, "table": self.TABLE},
        )
        boundaries = dialect.boundaries(ss_conn, cand, 4)

        total = 0
        for bnd in boundaries:
            pred = dialect.render_predicate(cand, bnd)
            with ss_conn.cursor() as c:
                c.execute(f"SELECT COUNT(*) FROM {self.TABLE_FQ} WHERE {pred}")
                total += c.fetchone()[0]

        assert total == expected, (
            f"NTILE chunks summed to {total}, expected {expected}"
        )

    def test_ntile_boundaries_gap_free(self, ss_conn):
        """Each slice's hi == next slice's lo (gap-free, half-open)."""
        ranges = sb.ntile_boundaries(ss_conn, self.OWNER, self.TABLE, "customer_id", 4)
        non_null = [(lo, hi) for lo, hi in ranges if lo is not None and hi is not None]
        # The first slice has lo=None (open-below); strip it to check internal edges.
        internal = [(lo, hi) for lo, hi in ranges if lo is not None]
        for (_, a_hi), (b_lo, _) in zip(internal, internal[1:]):
            assert a_hi == b_lo, f"Gap between slices: {a_hi} != {b_lo}"

    def test_segment_size_bytes_nonzero(self, ss_conn):
        size = sb.segment_size_bytes(ss_conn, self.OWNER, self.TABLE)
        assert size > 0

    def test_datetime_column_boundaries(self, ss_conn):
        """NTILE on created_at (datetime) must return valid ranges that execute."""
        ranges = sb.ntile_boundaries(ss_conn, self.OWNER, self.TABLE, "created_at", 3)
        assert len(ranges) >= 1
        dialect = SqlServerDialect()
        cand = Candidate(
            kind="column",
            key="created_at",
            label="created_at",
            meta={"owner": self.OWNER, "table": self.TABLE},
        )
        boundaries = dialect.boundaries(ss_conn, cand, 3)
        total = 0
        for bnd in boundaries:
            pred = dialect.render_predicate(cand, bnd)
            with ss_conn.cursor() as c:
                c.execute(f"SELECT COUNT(*) FROM {self.TABLE_FQ} WHERE {pred}")
                total += c.fetchone()[0]
        with ss_conn.cursor() as c:
            c.execute(f"SELECT COUNT(*) FROM {self.TABLE_FQ}")
            expected = c.fetchone()[0]
        assert total == expected
