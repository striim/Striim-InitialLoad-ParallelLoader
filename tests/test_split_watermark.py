import json

from split_runner import write_split_watermark


def test_write_split_watermark_payload(tmp_path):
    log = str(tmp_path / "logging" / "app.log")
    path = write_split_watermark(
        log,
        {"label": "Oracle SCN", "value": 123456},
        "queryfile.txt",
        "SALES.ORDERS",
    )
    assert path.endswith("split_watermark.json")
    with open(path) as f:
        d = json.load(f)
    assert d["value"] == "123456"
    assert d["queryfile"] == "queryfile.txt"
    assert d["table"] == "SALES.ORDERS"
    assert d["label"] == "Oracle SCN"
    assert "captured_at" in d


def test_write_split_watermark_none_when_no_scn(tmp_path):
    log = str(tmp_path / "logging" / "app.log")
    assert write_split_watermark(log, None, "queryfile.txt", "SALES.T") is None
    assert (
        write_split_watermark(
            log, {"label": "Oracle SCN", "value": None}, "queryfile.txt", "SALES.T"
        )
        is None
    )
