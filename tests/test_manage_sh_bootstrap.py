"""manage.sh venv bootstrap: the non-interactive (scripted) path must never
prompt, must keep stdout clean for `--json` consumers, and must still pass the
subcommand through to manage.py."""

import os
import shutil
import subprocess

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MANAGE_SH = os.path.join(_ROOT, "manage.sh")

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("python") is None,
    reason="needs bash and a `python` on PATH",
)


def _stage(tmp_path):
    shutil.copy(_MANAGE_SH, tmp_path / "manage.sh")
    (tmp_path / "requirements.txt").write_text("")
    # Stub engine: echoes a marker + args to stdout so we can prove pass-through.
    (tmp_path / "manage.py").write_text(
        "import sys\nprint('STUB-RAN ' + ' '.join(sys.argv[1:]))\n"
    )


def test_noninteractive_no_venv_warns_on_stderr_and_passes_through(tmp_path):
    _stage(tmp_path)
    # stdin from DEVNULL => not a TTY => bootstrap must not prompt.
    proc = subprocess.run(
        ["bash", "manage.sh", "status", "--json"],
        cwd=tmp_path,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0
    # Pass-through reached the stub with the args intact.
    assert "STUB-RAN status --json" in proc.stdout
    # The bootstrap notice went to stderr, never stdout (stdout stays JSON-clean).
    assert "no .venv found" in proc.stderr
    assert "no .venv found" not in proc.stdout
    assert "[y/N]" not in proc.stdout and "[Y/n]" not in proc.stdout


def test_noninteractive_uses_existing_venv_without_warning(tmp_path):
    _stage(tmp_path)
    # A directory named .venv with a no-op activate script satisfies the check.
    venv_bin = tmp_path / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    (venv_bin / "activate").write_text(": # no-op activate for the test\n")
    proc = subprocess.run(
        ["bash", "manage.sh", "status"],
        cwd=tmp_path,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0
    assert "STUB-RAN status" in proc.stdout
    assert "no .venv found" not in proc.stderr
