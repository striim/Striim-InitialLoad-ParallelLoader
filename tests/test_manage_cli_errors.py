"""main() maps a missing-credentials error to a clean exit 3 (no traceback)."""

import pytest

import manage
from oracle_client import OracleConfigError


class _FakeParser:
    def __init__(self, func):
        self._func = func

    def parse_args(self, argv=None):
        ns = type("NS", (), {})()
        ns.func = self._func
        return ns


def _patch_func(monkeypatch, func):
    monkeypatch.setattr(manage, "build_parser", lambda: _FakeParser(func))


def test_is_missing_credentials_matches_every_engine_convention():
    # All engines raise "Missing <Engine> settings: …" — classify on that.
    for msg in (
        "Missing Oracle settings: ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN",
        "Missing JDBC settings: JDBC_URL",
        "Missing SQL Server settings: SQLSERVER_HOST",
        "Missing PG settings: SOURCE_PG_HOST",
    ):
        assert manage._is_missing_credentials(Exception(msg))
    assert not manage._is_missing_credentials(RuntimeError("connection refused"))
    assert not manage._is_missing_credentials(ValueError("bad OWNER.TABLE"))


def test_main_returns_3_on_missing_credentials(monkeypatch, capsys):
    def boom(args):
        raise OracleConfigError(
            "Missing Oracle settings: ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN"
        )

    _patch_func(monkeypatch, boom)
    rc = manage.main([])
    assert rc == 3
    err = capsys.readouterr().err
    assert "Missing Oracle settings" in err
    assert "setup --interactive" in err


def test_main_propagates_unrelated_errors(monkeypatch):
    def boom(args):
        raise RuntimeError("totally unrelated explosion")

    _patch_func(monkeypatch, boom)
    with pytest.raises(RuntimeError, match="unrelated"):
        manage.main([])


def test_main_passes_through_success_return_code(monkeypatch):
    _patch_func(monkeypatch, lambda args: 0)
    assert manage.main([]) == 0


# --- bad-identifier classification (case-insensitive; table/alias/column/target) ---


def test_is_bad_identifier_matches_all_engine_conventions():
    for msg in (
        "--table must be OWNER.TABLE, got 'CM_CASES'",  # oracle_boundaries
        "Invalid Oracle identifier: '1x'",  # oracle capital-I
        "invalid owner identifier: '1x'",  # generic source_dialect (lowercase)
        "invalid table identifier: 'a b'",
        "invalid alias identifier: 'x WHERE 1=1'",
        "target must be non-empty and must not contain '|'",
    ):
        assert manage._is_bad_identifier(Exception(msg)), msg
    assert not manage._is_bad_identifier(RuntimeError("connection refused"))


def test_main_returns_1_and_hints_owner_table_on_bad_table(monkeypatch, capsys):
    def boom(args):
        raise ValueError("invalid table identifier: 'a b'")

    _patch_func(monkeypatch, boom)
    rc = manage.main([])
    assert rc == 1
    err = capsys.readouterr().err
    assert "invalid table identifier" in err
    assert "OWNER.TABLE" in err


def test_main_returns_1_generic_hint_on_bad_alias(monkeypatch, capsys):
    def boom(args):
        raise ValueError("invalid alias identifier: 'x WHERE 1=1'")

    _patch_func(monkeypatch, boom)
    rc = manage.main([])
    assert rc == 1
    err = capsys.readouterr().err
    assert "re-enter" in err.lower()
    assert "OWNER.TABLE" not in err  # alias error must not show the table hint


# --- positive_int argparse type ---


def test_positive_int_accepts_positive():
    assert manage.positive_int("16") == 16


@pytest.mark.parametrize("bad", ["0", "-5"])
def test_positive_int_rejects_zero_and_negative(bad):
    import argparse

    with pytest.raises(argparse.ArgumentTypeError, match="positive integer"):
        manage.positive_int(bad)


def test_positive_int_rejects_non_numeric():
    import argparse

    with pytest.raises(argparse.ArgumentTypeError, match="not an integer"):
        manage.positive_int("abc")
