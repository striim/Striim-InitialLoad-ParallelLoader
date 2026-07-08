import importlib
import os


def test_slice_max_runtime_default_zero(monkeypatch):
    monkeypatch.delenv("SLICE_MAX_RUNTIME_SECONDS", raising=False)
    import config

    importlib.reload(config)
    assert config.SLICE_MAX_RUNTIME_SECONDS == 0


def test_slice_max_runtime_from_env(monkeypatch):
    monkeypatch.setenv("SLICE_MAX_RUNTIME_SECONDS", "900")
    import config

    importlib.reload(config)
    assert config.SLICE_MAX_RUNTIME_SECONDS == 900
    monkeypatch.delenv("SLICE_MAX_RUNTIME_SECONDS", raising=False)
    importlib.reload(config)


def test_probe_parallel_degrees_default(monkeypatch):
    monkeypatch.delenv("PROBE_PARALLEL_DEGREES", raising=False)
    import config

    importlib.reload(config)
    assert config.PROBE_PARALLEL_DEGREES == [1, 2, 4, 8]


def test_probe_parallel_degrees_from_env(monkeypatch):
    monkeypatch.setenv("PROBE_PARALLEL_DEGREES", "1, 4 ,16")
    import config

    importlib.reload(config)
    assert config.PROBE_PARALLEL_DEGREES == [1, 4, 16]
    monkeypatch.delenv("PROBE_PARALLEL_DEGREES", raising=False)
    importlib.reload(config)


def test_probe_parallel_runs_default_and_env(monkeypatch):
    monkeypatch.delenv("PROBE_PARALLEL_RUNS", raising=False)
    import config

    importlib.reload(config)
    assert config.PROBE_PARALLEL_RUNS == 1
    monkeypatch.setenv("PROBE_PARALLEL_RUNS", "3")
    importlib.reload(config)
    assert config.PROBE_PARALLEL_RUNS == 3
    monkeypatch.delenv("PROBE_PARALLEL_RUNS", raising=False)
    importlib.reload(config)
