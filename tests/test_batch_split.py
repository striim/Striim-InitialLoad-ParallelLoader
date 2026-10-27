"""Tests for the paste-a-batch builder (loader/batch_split.py).

The heavy path (probe + boundary discovery) needs a live source DB, so these tests
drive build_batch_lines / run_batch with probe disabled and a fake dialect — the
same pattern test_split_runner uses to prove build_lines without an engine.
"""

import json

import batch_split as bs
import pytest
from source_dialect import Boundary

# --- pure parsing --------------------------------------------------------


def test_split_query_target_basic():
    assert bs.split_query_target("SELECT 1 FROM t|SALES.TGT") == (
        "SELECT 1 FROM t",
        "SALES.TGT",
    )


def test_split_query_target_strips_whitespace():
    assert bs.split_query_target("  SELECT 1 |  SALES.TGT  ") == ("SELECT 1", "SALES.TGT")


def test_split_query_target_missing_target_raises():
    with pytest.raises(bs.BatchError):
        bs.split_query_target("SELECT 1 FROM t")


def test_split_query_target_empty_target_raises():
    with pytest.raises(bs.BatchError):
        bs.split_query_target("SELECT 1 FROM t|")


def test_split_query_target_oracle_concat_rejected():
    """A raw '|' (incl. Oracle '||') in the query is ambiguous with the delimiter
    and the loader would truncate it — reject up front."""
    with pytest.raises(bs.BatchError):
        bs.split_query_target("SELECT a||b FROM t|SALES.TGT")


def test_is_splittable():
    assert bs.is_splittable("SELECT * FROM t WHERE ~SPLIT~")
    assert not bs.is_splittable("SELECT * FROM t")


# --- fake dialect for the no-probe rowid path ----------------------------


class FakeConn:
    def close(self):
        pass


class FakeDialect:
    coalesce_physical_ranges = False
    watermark_label = "FAKE"

    def parse_owner_table(self, s):
        owner, table = s.split(".", 1)
        return owner, table

    def get_connection(self):
        return FakeConn()

    def capture_watermark(self, conn):
        return None  # -> write_split_watermark writes nothing (no value)

    def is_partitioned(self, conn, owner, table):
        return False

    def boundaries(self, conn, cand, chunks):
        return [Boundary(lo="A", hi="B"), Boundary(lo="C", hi="D")]

    def render_predicate(self, cand, boundary, alias=None):
        col = f"{alias}.ROWID" if alias else "ROWID"
        return f"{col} BETWEEN '{boundary.lo}' AND '{boundary.hi}'"


def _lines(entries, **kw):
    d = FakeDialect()
    return bs.build_batch_lines(entries, d, d.get_connection(), "queryfile.txt", **kw)


# --- fan-out logic -------------------------------------------------------


def test_passthrough_emitted_verbatim():
    entries = [
        {"line": "SELECT * FROM SALES.LOOKUP|SALES.LOOKUP_TGT", "needs_split": False}
    ]
    lines = _lines(entries, probe=False)
    assert lines == ["SELECT * FROM SALES.LOOKUP|SALES.LOOKUP_TGT"]


def test_splittable_expands_to_boundary_lines():
    entries = [
        {
            "line": "SELECT * FROM SALES.T t WHERE ~SPLIT~|SALES.TGT",
            "needs_split": True,
            "table": "SALES.T",
            "alias": "t",
        }
    ]
    lines = _lines(entries, probe=False, default_strategy="rowid")
    assert len(lines) == 2  # two fake boundaries
    assert lines[0] == "SELECT * FROM SALES.T t WHERE t.ROWID BETWEEN 'A' AND 'B'|SALES.TGT"
    assert all(ln.endswith("|SALES.TGT") for ln in lines)
    assert all("~SPLIT~" not in ln for ln in lines)


def test_mixed_batch_preserves_order_and_counts():
    entries = [
        {
            "line": "SELECT * FROM SALES.T t WHERE ~SPLIT~|SALES.TGT",
            "needs_split": True,
            "table": "SALES.T",
            "alias": "t",
        },
        {"line": "SELECT * FROM SALES.LOOKUP|SALES.LK_TGT", "needs_split": False},
    ]
    lines = _lines(entries, probe=False, default_strategy="rowid")
    assert len(lines) == 3  # 2 split slices + 1 pass-through
    assert lines[-1] == "SELECT * FROM SALES.LOOKUP|SALES.LK_TGT"


def test_needs_split_without_token_falls_back_to_passthrough():
    """Defensive: a line the wizard flagged splittable but that carries no token is
    passed through, not force-split (we don't guess a predicate location)."""
    entries = [
        {"line": "SELECT * FROM SALES.T|SALES.TGT", "needs_split": True, "table": "SALES.T"}
    ]
    lines = _lines(entries, probe=False)
    assert lines == ["SELECT * FROM SALES.T|SALES.TGT"]


def test_splittable_without_table_raises():
    entries = [
        {"line": "SELECT * FROM SALES.T t WHERE ~SPLIT~|SALES.TGT", "needs_split": True}
    ]
    with pytest.raises(bs.BatchError):
        _lines(entries, probe=False)


def test_on_summary_receives_one_line_per_entry():
    entries = [
        {
            "line": "SELECT * FROM SALES.T t WHERE ~SPLIT~|SALES.TGT",
            "needs_split": True,
            "table": "SALES.T",
            "alias": "t",
        },
        {"line": "SELECT * FROM SALES.LOOKUP|SALES.LK_TGT", "needs_split": False},
    ]
    seen = []
    _lines(entries, probe=False, default_strategy="rowid", on_summary=seen.append)
    assert len(seen) == 2
    assert "pass-through" in seen[1]


def test_blank_lines_skipped():
    entries = [
        {"line": "   ", "needs_split": False},
        {"line": "SELECT * FROM SALES.LOOKUP|SALES.LK_TGT", "needs_split": False},
    ]
    lines = _lines(entries, probe=False)
    assert lines == ["SELECT * FROM SALES.LOOKUP|SALES.LK_TGT"]


# --- run_batch (file output) ---------------------------------------------


def test_run_batch_empty_raises():
    with pytest.raises(bs.BatchError):
        bs.run_batch([], "queryfile.txt")


def test_run_batch_writes_file(tmp_path, monkeypatch):
    monkeypatch.setattr(bs, "get_dialect", lambda eng=None: FakeDialect())
    out = tmp_path / "queryfile.txt"
    entries = [
        {
            "line": "SELECT * FROM SALES.T t WHERE ~SPLIT~|SALES.TGT",
            "needs_split": True,
            "table": "SALES.T",
            "alias": "t",
        },
        {"line": "SELECT * FROM SALES.LOOKUP|SALES.LK_TGT", "needs_split": False},
    ]
    rc = bs.run_batch(entries, str(out), probe=False, default_strategy="rowid")
    assert rc == 0
    written = out.read_text().splitlines()
    assert len(written) == 3
    assert written[-1] == "SELECT * FROM SALES.LOOKUP|SALES.LK_TGT"


def test_manifest_roundtrip_shape():
    """The shape manage.sh writes must be a JSON array of entry dicts run_batch reads."""
    manifest = json.loads(
        '[{"line": "SELECT * FROM SALES.T t WHERE ~SPLIT~|SALES.TGT", '
        '"needs_split": true, "table": "SALES.T", "alias": "t"}, '
        '{"line": "SELECT * FROM SALES.LOOKUP|SALES.LK_TGT", "needs_split": false}]'
    )
    lines = _lines(manifest, probe=False, default_strategy="rowid")
    assert len(lines) == 3
