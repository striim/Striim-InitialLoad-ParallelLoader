"""Pure tests: Postgres plan nodes that read only the requested range are disjoint.

A Bitmap Heap Scan is driven by a bitmap index range, so it is disjoint. A Seq Scan
that reads only some partitions is labelled with the partition count but stays
non-disjoint: slices on another key can each read the same whole partition.
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


def _seq_append_plan(partitions, schema=None):
    nodes = []
    for p in partitions:
        node = {"Node Type": "Seq Scan", "Relation Name": p, "Plan Rows": 1000}
        if schema:
            node["Schema"] = schema
        nodes.append(node)
    return [{"Plan": {"Node Type": "Append", "Plans": nodes}}]


def test_bitmap_heap_scan_is_disjoint():
    assert PostgresDialect._is_disjoint_node("Bitmap Heap Scan") is True


def test_seq_scan_still_not_disjoint():
    assert PostgresDialect._is_disjoint_node("Seq Scan") is False


def test_pruned_partition_seq_scan_is_labelled_but_not_disjoint():
    # EXPLAIN, then: (leaf partitions in the tree, scanned relations found in it).
    cur = ScriptedCursor([(_seq_append_plan(["events_2024_01", "events_2024_02"]),), (24, 2)])
    ps = PostgresDialect().explain(FakeConn(cur), "SELECT 1")
    assert ps.is_disjoint is False
    assert ps.access_path == "Seq Scan (2 of 24 partitions)"


def test_same_partition_for_every_slice_is_not_disjoint():
    # A query fixed to January, sliced on another key: each slice seq-scans the same
    # leaf. The label says 1 of 24, and the verdict must not call it disjoint.
    for _ in range(2):
        cur = ScriptedCursor([(_seq_append_plan(["events_2024_01"]),), (24, 1)])
        ps = PostgresDialect().explain(FakeConn(cur), "SELECT 1")
        assert ps.is_disjoint is False
        assert ps.access_path == "Seq Scan (1 of 24 partitions)"


def test_relations_are_resolved_with_their_schema():
    cur = ScriptedCursor([(_seq_append_plan(["events_2024_01"], schema="demo"),), (24, 1)])
    PostgresDialect().explain(FakeConn(cur), "SELECT 1")
    assert cur.executed[0][0].startswith("EXPLAIN (VERBOSE, FORMAT JSON) ")
    assert cur.executed[1][1] == (['"demo"."events_2024_01"'],)


def test_relation_outside_the_tree_gets_no_label():
    # A same-named relation in another schema, or one from a different tree: not counted.
    cur = ScriptedCursor([(_seq_append_plan(["a_2024_01", "b_2024_01"]),), (24, 1)])
    ps = PostgresDialect().explain(FakeConn(cur), "SELECT 1")
    assert ps.access_path == "Seq Scan"


def test_unpruned_partition_seq_scan_is_not_disjoint():
    cur = ScriptedCursor([(_seq_append_plan(["p1", "p2", "p3"]),), (3, 3)])
    ps = PostgresDialect().explain(FakeConn(cur), "SELECT 1")
    assert ps.is_disjoint is False
    assert ps.access_path == "Seq Scan"


def test_plain_seq_scan_is_not_disjoint():
    plan = [{"Plan": {"Node Type": "Seq Scan", "Relation Name": "orders", "Plan Rows": 10}}]
    cur = ScriptedCursor([(plan,), (None, None)])
    ps = PostgresDialect().explain(FakeConn(cur), "SELECT 1")
    assert ps.is_disjoint is False
    assert ps.access_path == "Seq Scan"
