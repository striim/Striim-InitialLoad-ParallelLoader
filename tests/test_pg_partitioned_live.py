"""
Live tests for sizing and ctid-banding a declaratively partitioned Postgres table.

Skipped unless PG_LIVE_DSN is set (a libpq connection string for a role that can
create a schema), for example:

  PG_LIVE_DSN="host=localhost port=5432 dbname=postgres user=postgres password=..." \
      pytest tests/test_pg_partitioned_live.py -v

Creates schema plt_part_test and drops it on teardown.
"""
import os

import pytest

import postgres_boundaries as pb
from postgres_dialect import PostgresDialect
from source_dialect import Candidate

pytestmark = pytest.mark.skipif(
    not os.environ.get("PG_LIVE_DSN"), reason="PG_LIVE_DSN not set"
)

SCHEMA = "plt_part_test"


@pytest.fixture(scope="module")
def conn():
    import psycopg2

    c = psycopg2.connect(os.environ["PG_LIVE_DSN"])
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {SCHEMA}")
        cur.execute(
            f"CREATE TABLE {SCHEMA}.ev (id bigint, ts timestamp, pad text) "
            "PARTITION BY RANGE (ts)"
        )
        for name, lo, hi in [
            ("ev_2024_01", "2024-01-01", "2024-02-01"),
            ("ev_2024_02", "2024-02-01", "2024-03-01"),
            ("ev_2024_03", "2024-03-01", "2024-04-01"),  # left empty
        ]:
            cur.execute(
                f"CREATE TABLE {SCHEMA}.{name} PARTITION OF {SCHEMA}.ev "
                f"FOR VALUES FROM ('{lo}') TO ('{hi}')"
            )
        cur.execute(
            f"INSERT INTO {SCHEMA}.ev "
            "SELECT g, timestamp '2024-01-01' + (g % 59) * interval '1 day', "
            "repeat('x', 100) FROM generate_series(1, 30000) g"
        )
        cur.execute(
            f"CREATE TABLE {SCHEMA}.flat AS "
            "SELECT g AS id, repeat('y', 100) AS pad FROM generate_series(1, 10000) g"
        )
        cur.execute(f"ANALYZE {SCHEMA}.ev")
        cur.execute(f"ANALYZE {SCHEMA}.flat")
    yield c
    with c.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    c.close()


def _count(conn, where):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.ev WHERE {where}")
        return cur.fetchone()[0]


def test_partitioned_size_and_pages_nonzero_live(conn):
    assert pb.segment_size_bytes(conn, SCHEMA, "ev") > 0
    assert pb.relpages(conn, SCHEMA, "ev") > 0


def test_partitioned_ctid_bands_cover_every_row_once_live(conn):
    d = PostgresDialect()
    cand = Candidate(kind="physical", key=None, meta={"owner": SCHEMA, "table": "ev"})
    bands = d.boundaries(conn, cand, 4)
    assert len(bands) > 1
    total = _count(conn, "1=1")
    assert sum(_count(conn, d.render_predicate(cand, b)) for b in bands) == total


def test_plain_table_unchanged_live(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relpages, pg_relation_size(c.oid) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = 'flat'",
            (SCHEMA,),
        )
        pages, size = cur.fetchone()
    assert pb.relpages(conn, SCHEMA, "flat") == pages
    assert pb.segment_size_bytes(conn, SCHEMA, "flat") == size


def test_pruned_partition_scan_is_disjoint_live(conn):
    ps = PostgresDialect().explain(
        conn, f"SELECT * FROM {SCHEMA}.ev e WHERE e.ts < '2024-02-01'"
    )
    assert ps.is_disjoint is True
    assert ps.access_path == "Seq Scan (1 of 3 partitions)"


def test_unpruned_partition_scan_is_not_disjoint_live(conn):
    ps = PostgresDialect().explain(conn, f"SELECT * FROM {SCHEMA}.ev e WHERE e.pad <> ''")
    assert ps.is_disjoint is False
