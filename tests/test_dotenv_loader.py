"""Unit tests for the no-dependency ``.env`` loader in config.py.

Exercises ``config._load_dotenv`` in isolation against temp files. Each test
cleans up any os.environ keys it touches so the loader's direct mutations do
not leak between tests.
"""

import os

import pytest

import config

# Keys used only by these tests — always test-namespaced so cleanup is safe.
_TEST_KEYS = (
    "DOTENV_TEST_LOADED",
    "DOTENV_TEST_EXISTING",
    "DOTENV_TEST_QUOTED_S",
    "DOTENV_TEST_QUOTED_D",
    "DOTENV_TEST_SHOULD_SKIP",
    "DOTENV_TEST_MISSING",
)


@pytest.fixture(autouse=True)
def _clean_env():
    for k in _TEST_KEYS:
        os.environ.pop(k, None)
    yield
    for k in _TEST_KEYS:
        os.environ.pop(k, None)


def test_load_sets_absent_key(tmp_path):
    env = tmp_path / ".env"
    env.write_text("DOTENV_TEST_LOADED=hello\n")
    assert "DOTENV_TEST_LOADED" not in os.environ
    config._load_dotenv(str(env))
    assert os.environ["DOTENV_TEST_LOADED"] == "hello"


def test_existing_env_var_is_not_overwritten(tmp_path):
    os.environ["DOTENV_TEST_EXISTING"] = "real-env-wins"
    env = tmp_path / ".env"
    env.write_text("DOTENV_TEST_EXISTING=from-dotenv\n")
    config._load_dotenv(str(env))
    # Real env var takes precedence over .env.
    assert os.environ["DOTENV_TEST_EXISTING"] == "real-env-wins"


def test_missing_file_is_noop(tmp_path):
    missing = tmp_path / "does-not-exist.env"
    # Must not raise.
    config._load_dotenv(str(missing))
    assert "DOTENV_TEST_MISSING" not in os.environ


def test_quote_stripping_single_and_double(tmp_path):
    env = tmp_path / ".env"
    env.write_text("DOTENV_TEST_QUOTED_S='single'\n" 'DOTENV_TEST_QUOTED_D="double"\n')
    config._load_dotenv(str(env))
    assert os.environ["DOTENV_TEST_QUOTED_S"] == "single"
    assert os.environ["DOTENV_TEST_QUOTED_D"] == "double"


def test_comments_blanks_and_no_equals_are_skipped(tmp_path):
    env = tmp_path / ".env"
    env.write_text(
        "\n"
        "# a comment line\n"
        "this line has no equals sign\n"
        "   \n"
        "DOTENV_TEST_SHOULD_SKIP=kept\n"
    )
    config._load_dotenv(str(env))
    # The only valid KEY=VALUE line was loaded; the junk lines produced no keys.
    assert os.environ["DOTENV_TEST_SHOULD_SKIP"] == "kept"
    assert "# a comment line" not in os.environ
    assert "this line has no equals sign" not in os.environ
