import json
import os
import threading

import pytest

from data_tinydb import AtomicFlockStorage


def test_round_trip_preserves_rows(tmp_path):
    p = str(tmp_path / "db.json")
    s = AtomicFlockStorage(p)
    s.write({"_default": {"1": {"id": 1, "status": "NEW"}}})
    assert s.read() == {"_default": {"1": {"id": 1, "status": "NEW"}}}


def test_atomic_write_no_partial_file_on_failure(tmp_path, monkeypatch):
    p = str(tmp_path / "db.json")
    s = AtomicFlockStorage(p)
    s.write({"_default": {"1": {"id": 1}}})

    import data_tinydb

    def boom(*a, **k):
        raise RuntimeError("disk full mid-write")

    monkeypatch.setattr(data_tinydb.json, "dump", boom)
    with pytest.raises(RuntimeError):
        s.write({"_default": {"2": {"id": 2}}})

    # original file intact, no leftover temp files
    with open(p) as f:
        assert json.load(f) == {"_default": {"1": {"id": 1}}}
    leftovers = [n for n in os.listdir(tmp_path) if n.startswith(".tinydb-")]
    assert leftovers == []


def test_flock_serializes_concurrent_writers(tmp_path):
    p = str(tmp_path / "db.json")
    AtomicFlockStorage(p).write({"_default": {}})

    def worker(n):
        s = AtomicFlockStorage(p)
        for i in range(40):
            data = s.read() or {"_default": {}}
            data["_default"][str(n * 100 + i)] = {"id": n * 100 + i}
            s.write(data)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # File is never corrupt (valid JSON) — the lock serialized the rewrites.
    with open(p) as f:
        json.load(f)
