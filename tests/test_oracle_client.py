import sys
import pytest
import oracle_client


def test_get_connection_missing_creds_raises(monkeypatch):
    import config

    for name in (
        "ORACLE_DSN",
        "ORACLE_HOST",
        "ORACLE_SERVICE",
        "ORACLE_USER",
        "ORACLE_PASSWORD",
    ):
        monkeypatch.setattr(config, name, "", raising=False)
    with pytest.raises(oracle_client.OracleConfigError) as e:
        oracle_client.get_connection()
    assert "Missing Oracle settings" in str(e.value)


def test_module_import_does_not_load_oracledb():
    # importing oracle_client must NOT eagerly import the optional driver
    assert "oracledb" not in sys.modules or sys.modules.get("oracledb") is not None
