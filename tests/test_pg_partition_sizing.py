"""Pure (no-DB) tests: Postgres size and page estimates read leaf partitions.

A partitioned parent has no storage of its own, so relpages / segment_size_bytes
must be computed from its leaf partitions (pg_partition_tree) and still cover a
plain table. Exercised through a fake cursor; the live counterpart is
tests/test_pg_partitioned_live.py.
"""
import postgres_boundaries


class FakeCursor:
    def __init__(self, fetchone=None):
        self._one = fetchone
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._one


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_relpages_sql_reads_leaf_partitions():
    cur = FakeCursor(fetchone=(40,))
    assert postgres_boundaries.relpages(FakeConn(cur), "public", "events") == 40
    sql, params = cur.executed[0]
    assert "pg_partition_tree" in sql and "isleaf" in sql and "max(" in sql.lower()
    assert params == ("public", "events")


def test_segment_size_sql_sums_leaf_partitions():
    cur = FakeCursor(fetchone=(327680,))
    assert postgres_boundaries.segment_size_bytes(FakeConn(cur), "public", "events") == 327680
    sql, params = cur.executed[0]
    assert "pg_partition_tree" in sql and "isleaf" in sql and "sum(" in sql.lower()
    assert params == ("public", "events")


def test_relpages_and_size_none_row_is_zero():
    assert postgres_boundaries.relpages(FakeConn(FakeCursor(fetchone=(None,))), "public", "x") == 0
    assert postgres_boundaries.segment_size_bytes(FakeConn(FakeCursor(fetchone=None)), "public", "x") == 0
