import importlib

import pytest

pytest.importorskip("tinydb")

from models import QueryResult


def _qr(_id, run, status="NEW"):
    return QueryResult(
        roworder=_id,
        query="q",
        targettbl="t",
        _id=_id,
        uniquerunid=run,
        status=status,
        iscurrentrow=True,
    )


def test_delete_runid_removes_only_that_run(tmp_path, monkeypatch):
    import config

    monkeypatch.setattr(config, "STAGE_DB_LOCATION", "TinyDB")
    monkeypatch.setattr(config, "TINYDB_PATH", str(tmp_path / "pos.json"))
    import data_tinydb
    import data

    importlib.reload(data_tinydb)
    data_tinydb.write_to_tinydb([_qr(1, 100), _qr(2, 100), _qr(3, 200)])

    data.delete_runid(100)

    remaining = data_tinydb.read_from_tinydb(
        "iscurrentrow = True AND uniquerunid = 200"
    )
    assert [r.id for r in remaining] == [3]
    gone = data_tinydb.read_from_tinydb("iscurrentrow = True AND uniquerunid = 100")
    assert gone == []
