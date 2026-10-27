"""Tests for watermark.py — pure, no DB drivers, no network."""

import json
import logging

import pytest

import watermark


def test_format_banner_contains_expected_fields():
    banner = watermark.format_banner(
        "Oracle SCN", 12345, 100, "2026-06-27T14:03:11+00:00"
    )
    assert "Oracle SCN" in banner
    assert "12345" in banner
    assert "run 100" in banner
    assert "Set the downstream CDC reader's start point to this value." in banner
    assert "=" * 64 in banner


def test_sidecar_path():
    path = watermark.sidecar_path("/x/logging/striimautoloader.log", 100)
    assert path == "/x/logging/run_100_watermark.json"


def test_capture_and_log_happy_path(tmp_path):
    log_dir = tmp_path / "logging"
    log_dir.mkdir()
    log_file = str(log_dir / "app.log")

    result = watermark.capture_and_log(
        {"label": "Oracle SCN", "value": 999},
        100,
        "host:1521/ORCL",
        log_file,
        captured_at="2026-06-27T00:00:00+00:00",
    )

    assert result is not None
    sidecar = log_dir / "run_100_watermark.json"
    assert sidecar.exists()

    with open(sidecar) as f:
        data = json.load(f)

    assert data["run_id"] == 100
    assert data["label"] == "Oracle SCN"
    assert data["value"] == "999"  # stringified
    assert data["captured_at"] == "2026-06-27T00:00:00+00:00"
    assert data["source_dsn"] == "host:1521/ORCL"


def test_capture_and_log_none_watermark_returns_none_and_warns(tmp_path, caplog):
    log_file = str(tmp_path / "app.log")
    with caplog.at_level(logging.WARNING, logger="parallelloader.watermark"):
        result = watermark.capture_and_log(None, 100, "dsn", log_file)
    assert result is None
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def test_capture_and_log_none_value_returns_none(tmp_path, caplog):
    log_file = str(tmp_path / "app.log")
    with caplog.at_level(logging.WARNING, logger="parallelloader.watermark"):
        result = watermark.capture_and_log(
            {"label": "x", "value": None}, 100, "dsn", log_file
        )
    assert result is None


def test_choose_watermark_reuses_split_when_queryfile_matches():
    sc = {"label": "Oracle SCN", "value": "123456", "queryfile": "queryfile.txt"}
    now = {"label": "Oracle SCN", "value": "999999"}
    wm, used = watermark.choose_watermark(sc, now, "queryfile.txt")
    assert used is True and wm == {"label": "Oracle SCN", "value": "123456"}


def test_choose_watermark_falls_back_on_queryfile_mismatch():
    sc = {"label": "Oracle SCN", "value": "123456", "queryfile": "other.txt"}
    now = {"label": "Oracle SCN", "value": "999999"}
    wm, used = watermark.choose_watermark(sc, now, "queryfile.txt")
    assert used is False and wm == now


def test_choose_watermark_falls_back_when_no_sidecar():
    now = {"label": "Oracle SCN", "value": "999999"}
    wm, used = watermark.choose_watermark(None, now, "queryfile.txt")
    assert used is False and wm == now


def test_choose_watermark_falls_back_when_sidecar_value_empty():
    sc = {"label": "Oracle SCN", "value": None, "queryfile": "queryfile.txt"}
    wm, used = watermark.choose_watermark(sc, None, "queryfile.txt")
    assert used is False and wm is None
