"""Pure (no-network) tests for striim_monitor.py.

available() is tested by monkeypatching the already-loaded config module object;
because available() does `import config` lazily it picks up the same module
instance from sys.modules, so setattr patches take effect immediately.

app_metrics() is tested for defensive return behaviour only (no Striim/network).
"""

import sys

import pytest

import config  # ensure it's in sys.modules before we monkeypatch it
import striim_monitor

# ---------------------------------------------------------------------------
# available()
# ---------------------------------------------------------------------------


class TestAvailable:
    def test_false_when_node_empty(self, monkeypatch):
        monkeypatch.setattr(config, "STRIIM_NODE", "")
        monkeypatch.setattr(config, "STRIIM_API_TOKEN", "sometoken")
        monkeypatch.setattr(config, "STRIIM_ADMIN_PWD", "")
        assert striim_monitor.available() is False

    def test_true_when_node_and_token(self, monkeypatch):
        monkeypatch.setattr(config, "STRIIM_NODE", "127.0.0.1:9080")
        monkeypatch.setattr(config, "STRIIM_API_TOKEN", "tok123")
        monkeypatch.setattr(config, "STRIIM_ADMIN_PWD", "")
        assert striim_monitor.available() is True

    def test_true_when_node_and_pwd(self, monkeypatch):
        monkeypatch.setattr(config, "STRIIM_NODE", "127.0.0.1:9080")
        monkeypatch.setattr(config, "STRIIM_API_TOKEN", "")
        monkeypatch.setattr(config, "STRIIM_ADMIN_PWD", "s3cr3t")
        assert striim_monitor.available() is True

    def test_false_when_node_set_but_no_auth(self, monkeypatch):
        monkeypatch.setattr(config, "STRIIM_NODE", "127.0.0.1:9080")
        monkeypatch.setattr(config, "STRIIM_API_TOKEN", "")
        monkeypatch.setattr(config, "STRIIM_ADMIN_PWD", "")
        assert striim_monitor.available() is False


# ---------------------------------------------------------------------------
# app_metrics() — defensive behaviour; never touches network
# ---------------------------------------------------------------------------


class TestAppMetrics:
    def _set_no_creds(self, monkeypatch):
        """Force available() → False so the short-circuit path is exercised."""
        monkeypatch.setattr(config, "STRIIM_NODE", "")
        monkeypatch.setattr(config, "STRIIM_API_TOKEN", "")
        monkeypatch.setattr(config, "STRIIM_ADMIN_PWD", "")

    def test_returns_empty_dict_when_creds_absent(self, monkeypatch):
        self._set_no_creds(monkeypatch)
        result = striim_monitor.app_metrics(["ILA_100_1", "ILA_100_2"])
        assert result == {}

    def test_never_returns_none(self, monkeypatch):
        self._set_no_creds(monkeypatch)
        result = striim_monitor.app_metrics([])
        assert result is not None
        assert isinstance(result, dict)

    def test_does_not_raise_when_creds_absent(self, monkeypatch):
        self._set_no_creds(monkeypatch)
        # Must not raise regardless of namespace list
        striim_monitor.app_metrics(["ILA_100_1"])
        striim_monitor.app_metrics([])

    def test_returns_dict_type_always(self, monkeypatch):
        """Return type is always dict, never None, even on error paths."""
        self._set_no_creds(monkeypatch)
        for namespaces in [[], ["ILA_100_1"], ["ILA_100_1", "ILA_100_2"]]:
            result = striim_monitor.app_metrics(namespaces)
            assert isinstance(
                result, dict
            ), f"Expected dict for namespaces={namespaces}"
