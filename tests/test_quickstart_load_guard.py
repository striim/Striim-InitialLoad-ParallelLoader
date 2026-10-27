"""quickstart.sh load must not empty the target while a previous run can still resume.

main.py RESUMEs whenever the run has loader rows, and completed slices are not
replayed, so truncating the target first would lose their rows for good. The
script runs here with a fake `docker` (records every psql call) and a fake loader
Python, so no database or Striim is needed.
"""

import os
import shutil
import stat
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

FAKE_DOCKER = """#!/usr/bin/env bash
echo "docker $*" >> "$FAKE_LOG"
cat > /dev/null
"""

# Stands in for the loader venv's python: `manage.py status` prints $FAKE_COUNTS,
# main.py exits 0 at once, and inline scripts (`-`) run on the real interpreter.
FAKE_PY = """#!/usr/bin/env bash
case "$1" in
  -) exec "$REAL_PY" "$@" ;;
  */manage.py) [ "$2" = status ] && printf 'Run 100: test\\n  counts: %s\\n' "$FAKE_COUNTS"; exit 0 ;;
  */main.py) echo "Run completed"; exit 0 ;;
esac
"""


def _exe(path, text):
    with open(path, "w") as f:
        f.write(text)
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)


@pytest.fixture
def qs(tmp_path):
    home = tmp_path / "loader"
    shutil.copytree(os.path.join(ROOT, "quickstart"), home / "quickstart")
    run = home / "quickstart" / "postgres" / "run"
    (run / "logging").mkdir(parents=True, exist_ok=True)
    (run / "queryfile.txt").write_text("SELECT 1|demo.orders\n")
    (run / "load_query.sql").write_text("SELECT o.* FROM demo.orders o WHERE ~SPLIT~\n")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    _exe(bindir / "docker", FAKE_DOCKER)
    _exe(bindir / "fakepy", FAKE_PY)
    log = tmp_path / "calls.log"
    log.write_text("")

    def run_load(counts):
        env = dict(
            os.environ,
            PATH=f"{bindir}{os.pathsep}{os.environ['PATH']}",
            FAKE_LOG=str(log),
            FAKE_COUNTS=counts,
            REAL_PY=sys.executable,
            LOADER_PY=str(bindir / "fakepy"),
        )
        script = home / "quickstart" / "postgres" / "quickstart.sh"
        proc = subprocess.run(
            ["bash", str(script), "load"],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        return proc, log.read_text()

    run_load.run_dir = run
    return run_load


def test_fresh_run_empties_the_target(qs):
    proc, calls = qs("(none)")
    assert proc.returncode == 0, proc.stderr
    assert "TRUNCATE demo.orders" in calls


def test_resumable_run_keeps_the_target(qs):
    proc, calls = qs("{'COMPLETED': 3, 'PENDING': 4}")
    assert proc.returncode == 0, proc.stderr
    assert "TRUNCATE" not in calls
    assert "resuming it and keeping the target rows" in proc.stdout


def test_running_load_is_refused_before_any_truncate(qs):
    lock = qs.run_dir / "logging" / "run_100.lock"
    lock.write_text('{"pid": %d, "start_time": 0}' % os.getpid())
    proc, calls = qs("(none)")
    assert proc.returncode != 0
    assert "already running" in proc.stderr
    assert "TRUNCATE" not in calls
