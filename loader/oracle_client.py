"""python-oracledb thin-mode connection helper. `oracledb` is imported lazily."""

import config


class OracleConfigError(Exception):
    pass


def _dsn():
    if getattr(config, "ORACLE_DSN", ""):
        return config.ORACLE_DSN
    host = getattr(config, "ORACLE_HOST", "")
    service = getattr(config, "ORACLE_SERVICE", "")
    port = getattr(config, "ORACLE_PORT", 1521)
    if host and service:
        return f"{host}:{port}/{service}"
    return ""


def get_connection():
    user = getattr(config, "ORACLE_USER", "")
    pwd = getattr(config, "ORACLE_PASSWORD", "")
    dsn = _dsn()
    missing = [
        n
        for n, v in (
            ("ORACLE_USER", user),
            ("ORACLE_PASSWORD", pwd),
            ("ORACLE_DSN (or ORACLE_HOST+ORACLE_SERVICE)", dsn),
        )
        if not v
    ]
    if missing:
        raise OracleConfigError("Missing Oracle settings: " + ", ".join(missing))
    try:
        import oracledb
    except ImportError as e:
        raise OracleConfigError(
            "python-oracledb not installed; run: pip install python-oracledb"
        ) from e
    return oracledb.connect(user=user, password=pwd, dsn=dsn)
