"""Pure-unit tests for the interactive-setup helpers in manage.py.

Only the pure helpers are exercised here — no interactive I/O, no DB, no
subprocess. All tested helpers are import-safe without heavy deps.
"""

import manage


def _env_vars(backend, engine):
    return [f.env_var for f in manage.credential_spec(backend, engine)]


# --- credential_spec -------------------------------------------------------
def test_credential_spec_always_includes_striim():
    vars_ = _env_vars("TinyDB", "oracle")
    for v in (
        "STRIIM_NODE",
        "STRIIM_ADMIN_USER",
        "STRIIM_ADMIN_PWD",
        "STRIIM_API_TOKEN",
    ):
        assert v in vars_


def test_credential_spec_tinydb_has_no_state_vars():
    vars_ = _env_vars("TinyDB", "oracle")
    # No PG/BQ/ORCH_ORACLE state fields for TinyDB.
    assert not any(v.startswith("PG_") for v in vars_)
    assert not any(v.startswith("BQ_") for v in vars_)
    assert not any(v.startswith("ORCH_ORACLE_") for v in vars_)


def test_credential_spec_pg_state_vars():
    vars_ = _env_vars("PG", "oracle")
    for v in (
        "PG_HOST",
        "PG_PORT",
        "PG_DATABASE",
        "PG_USER",
        "PG_PASSWORD",
        "PG_SSLMODE",
    ):
        assert v in vars_


def test_credential_spec_bq_state_vars():
    vars_ = _env_vars("BQ", "oracle")
    for v in ("BQ_KEYFILE_LOCATION", "BQ_PROJECT_ID", "BQ_DATASET_ID", "BQ_TABLE_ID"):
        assert v in vars_


def test_credential_spec_oracle_state_vars():
    vars_ = _env_vars("ORACLE", "oracle")
    for v in (
        "ORCH_ORACLE_HOST",
        "ORCH_ORACLE_PORT",
        "ORCH_ORACLE_SERVICE",
        "ORCH_ORACLE_USER",
        "ORCH_ORACLE_PASSWORD",
    ):
        assert v in vars_
    # No *_DSN discrete-only rule.
    assert "ORCH_ORACLE_DSN" not in vars_


def test_credential_spec_source_oracle():
    vars_ = _env_vars("TinyDB", "oracle")
    for v in (
        "ORACLE_HOST",
        "ORACLE_PORT",
        "ORACLE_SERVICE",
        "ORACLE_USER",
        "ORACLE_PASSWORD",
    ):
        assert v in vars_
    assert "ORACLE_DSN" not in vars_


def test_credential_spec_source_postgres():
    vars_ = _env_vars("TinyDB", "postgres")
    for v in (
        "SOURCE_PG_HOST",
        "SOURCE_PG_PORT",
        "SOURCE_PG_DATABASE",
        "SOURCE_PG_USER",
        "SOURCE_PG_PASSWORD",
    ):
        assert v in vars_


def test_credential_spec_source_sqlserver():
    vars_ = _env_vars("TinyDB", "sqlserver")
    for v in (
        "SQLSERVER_HOST",
        "SQLSERVER_PORT",
        "SQLSERVER_DATABASE",
        "SQLSERVER_USER",
        "SQLSERVER_PASSWORD",
        "SQLSERVER_DRIVER",
    ):
        assert v in vars_


def test_credential_spec_source_jdbc():
    vars_ = _env_vars("TinyDB", "jdbc")
    for v in (
        "JDBC_DRIVER_CLASS",
        "JDBC_URL",
        "JDBC_JAR_PATH",
        "JDBC_USER",
        "JDBC_PASSWORD",
    ):
        assert v in vars_


def test_credential_spec_pg_plus_postgres_combo():
    # State PG + source postgres must keep both discrete sets distinct.
    vars_ = _env_vars("PG", "postgres")
    assert "PG_HOST" in vars_
    assert "SOURCE_PG_HOST" in vars_


def test_credential_spec_secret_and_optional_flags():
    fields = {f.env_var: f for f in manage.credential_spec("PG", "oracle")}
    assert fields["STRIIM_ADMIN_PWD"].secret is True
    assert fields["STRIIM_API_TOKEN"].secret is True
    assert fields["STRIIM_API_TOKEN"].optional is True
    assert fields["PG_PASSWORD"].secret is True
    assert fields["PG_SSLMODE"].optional is True
    assert fields["STRIIM_NODE"].secret is False
    assert fields["STRIIM_NODE"].optional is False


# --- mask_value ------------------------------------------------------------
def test_mask_value_secret_set():
    assert manage.mask_value("hunter2", True) == "******** (set)"


def test_mask_value_secret_empty():
    assert manage.mask_value("", True) == "(unset)"


def test_mask_value_non_secret_shows_value():
    assert manage.mask_value("localhost:9080", False) == "localhost:9080"


def test_mask_value_non_secret_empty():
    assert manage.mask_value("", False) == "(unset)"


# --- merge_env_lines -------------------------------------------------------
def test_merge_env_lines_updates_in_place():
    out = manage.merge_env_lines("A=1\nB=2\n", {"B": "9"})
    assert out == "A=1\nB=9\n"


def test_merge_env_lines_appends_new_keys():
    out = manage.merge_env_lines("A=1\n", {"B": "2", "C": "3"})
    assert out == "A=1\nB=2\nC=3\n"


def test_merge_env_lines_preserves_comments_and_unrelated():
    out = manage.merge_env_lines("A=1\n# c\nB=2\n", {"B": "9", "C": "3"})
    assert out == "A=1\n# c\nB=9\nC=3\n"


def test_merge_env_lines_from_empty():
    out = manage.merge_env_lines("", {"A": "1"})
    assert out == "A=1\n"


def test_merge_env_lines_no_updates_is_noop_text():
    out = manage.merge_env_lines("A=1\n# comment\n", {})
    assert out == "A=1\n# comment\n"


# --- ensure_gitignore_has --------------------------------------------------
def test_ensure_gitignore_appends_when_missing():
    out = manage.ensure_gitignore_has(".venv\n__pycache__\n", ".env")
    assert out == ".venv\n__pycache__\n.env\n"


def test_ensure_gitignore_appends_newline_when_no_trailing():
    out = manage.ensure_gitignore_has(".venv", ".env")
    assert out == ".venv\n.env\n"


def test_ensure_gitignore_idempotent():
    once = manage.ensure_gitignore_has(".venv\n.env\n", ".env")
    assert once == ".venv\n.env\n"
    twice = manage.ensure_gitignore_has(once, ".env")
    assert twice == once


def test_ensure_gitignore_from_empty():
    assert manage.ensure_gitignore_has("", ".env") == ".env\n"
