"""Pure tests: Postgres plan nodes that read only the requested range are disjoint.

A Bitmap Heap Scan is driven by a bitmap index range, and a Seq Scan that reads only
some partitions of a partitioned table has been pruned to the slice. Neither is a
full scan of the table.
"""
from postgres_dialect import PostgresDialect


class ScriptedCursor:
    """Returns the next scripted fetchone() value for each execute()."""

    def __init__(self, results):
        self._results = list(results)
        self._current = None
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._current = self._results.pop(0) if self._results else None

    def fetchone(self):
        return self._current


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _seq_append_plan(partitions):
    return [{"Plan": {"Node Type": "Append", "Plans": [
        {"Node Type": "Seq Scan", "Relation Name": p, "Plan Rows": 1000}
        for p in partitions]}}]


def test_bitmap_heap_scan_is_disjoint():
    assert PostgresDialect._is_disjoint_node("Bitmap Heap Scan") is True


def test_seq_scan_still_not_disjoint():
    assert PostgresDialect._is_disjoint_node("Seq Scan") is False


def test_pruned_partition_seq_scan_is_disjoint():
    # EXPLAIN, then: total leaf partitions of the scanned relations' root = 24.
    cur = ScriptedCursor([(_seq_append_plan(["events_2024_01", "events_2024_02"]),), (24,)])
    ps = PostgresDialect().explain(FakeConn(cur), "SELECT 1")
    assert ps.is_disjoint is True
    assert ps.access_path == "Seq Scan (2 of 24 partitions)"


def test_unpruned_partition_seq_scan_is_not_disjoint():
    cur = ScriptedCursor([(_seq_append_plan(["p1", "p2", "p3"]),), (3,)])
    ps = PostgresDialect().explain(FakeConn(cur), "SELECT 1")
    assert ps.is_disjoint is False
    assert ps.access_path == "Seq Scan"


def test_plain_seq_scan_is_not_disjoint():
    plan = [{"Plan": {"Node Type": "Seq Scan", "Relation Name": "orders", "Plan Rows": 10}}]
    cur = ScriptedCursor([(plan,), (None,)])
    ps = PostgresDialect().explain(FakeConn(cur), "SELECT 1")
    assert ps.is_disjoint is False
    assert ps.access_path == "Seq Scan"
