import pytest

import config
import data


def _set_qf(monkeypatch, tmp_path, text):
    p = tmp_path / "queryfile.txt"
    p.write_text(text)
    monkeypatch.setattr(config, "QUERY_FILE_PATH", str(p))
    return p


def test_missing_queryfile_friendly_error(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "QUERY_FILE_PATH", str(tmp_path / "nope.txt"))
    with pytest.raises(SystemExit) as ei:
        data.read_csv_to_query_results()
    assert "manage.py split" in str(ei.value)


def test_missing_target_names_line(monkeypatch, tmp_path):
    _set_qf(
        monkeypatch,
        tmp_path,
        "SELECT * FROM SALES.T WHERE X=1|SALES.TGT\nSELECT * FROM SALES.T WHERE X=2\n",
    )
    with pytest.raises(SystemExit) as ei:
        data.read_csv_to_query_results()
    assert "line 2" in str(ei.value)


def test_demo_queryfile_warns(monkeypatch, tmp_path, capsys):
    _set_qf(
        monkeypatch,
        tmp_path,
        "SELECT * FROM QASOURCE.ADDRESSES WHERE ADDRESS_ID = 1|QASOURCE.ADDRESSES_TEST\n",
    )
    rows = data.read_csv_to_query_results()
    assert len(rows) == 1
    assert "QASOURCE.ADDRESSES" in capsys.readouterr().out


def test_valid_queryfile_ok(monkeypatch, tmp_path):
    _set_qf(
        monkeypatch,
        tmp_path,
        "SELECT * FROM SALES.ORDERS s WHERE ~SPLIT~|SALES.ORDERS_TGT\n",
    )
    rows = data.read_csv_to_query_results()
    assert rows[0].targettbl == "SALES.ORDERS_TGT"
