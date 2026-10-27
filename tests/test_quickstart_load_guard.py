"""quickstart.sh: `load` never empties the target; `clean` does, but not under a live loader.

main.py takes the run lock and decides FRESH / RESUME / REFUSE itself, so `load` must not
mutate the target before it (a resumed run's completed slices are not replayed, and a
refused or losing start must leave the target alone). `clean` is the explicit reset; it
checks the run lock with the loader's own logic (pid plus process start time), so a live
loader blocks it and a stale lock whose pid was recycled does not.

The script runs with a fake `docker` (records every psql call) and a fake loader Python
for manage.py / main.py; inline helpers run on the real interpreter with the real
config.py and loader/run_safety.py. No database or Striim is needed.
"""

import json
import os
import shutil
import stat
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "loader"))
import run_safety  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

# psql_val (-tAc) answers "1" so clean sees the target database as present.
FAKE_DOCKER = """#!/usr/bin/env bash
echo "docker $*" >> "$FAKE_LOG"
cat > /dev/null
case " $* " in *" -tAc "*) echo 1 ;; esac
"""

FAKE_PY = """#!/usr/bin/env bash
case "$1" in
  -) exec "$REAL_PY" "$@" ;;
  */manage.py) [ "$2" = status ] && printf 'Run 100: test\\n  counts: (none)\\n'
               if [ "$2" = clear ] && [ -n "$CLEAR_BARRIER" ]; then
                   touch "$CLEAR_BARRIER.in"
                   while [ ! -e "$CLEAR_BARRIER.go" ]; do sleep 0.1; done
               fi
               exit 0 ;;
  */main.py) [ -n "$MAIN_PIDFILE" ] && { echo $$ > "$MAIN_PIDFILE"; exec sleep 30; }
             echo "Run completed"; exit 0 ;;
esac
"""


def _exe(path, text):
    path.write_text(text)
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)


@pytest.fixture
def qs(tmp_path):
    home = tmp_path / "loader"
    shutil.copytree(os.path.join(ROOT, "quickstart"), home / "quickstart")
    shutil.copytree(os.path.join(ROOT, "loader"), home / "loader")
    shutil.copy(os.path.join(ROOT, "config.py"), home / "config.py")
    run = home / "quickstart" / "postgres" / "run"
    (run / "logging").mkdir(parents=True, exist_ok=True)
    (run / "queryfile.txt").write_text("SELECT 1|demo.orders\n")
    (run / "load_query.sql").write_text("SELECT o.* FROM demo.orders o WHERE ~SPLIT~\n")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    _exe(bindir / "docker", FAKE_DOCKER)
    _exe(bindir / "fakepy", FAKE_PY)
    log = tmp_path / "calls.log"

    def run_stage(stage):
        log.write_text("")
        env = dict(
            os.environ,
            PATH=f"{bindir}{os.pathsep}{os.environ['PATH']}",
            FAKE_LOG=str(log),
            REAL_PY=sys.executable,
            LOADER_PY=str(bindir / "fakepy"),
        )
        proc = subprocess.run(
            ["bash", str(home / "quickstart" / "postgres" / "quickstart.sh"), stage],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        return proc, log.read_text()

    def start_stage(stage, extra_env):
        log.write_text("")
        env = dict(
            os.environ,
            PATH=f"{bindir}{os.pathsep}{os.environ['PATH']}",
            FAKE_LOG=str(log),
            REAL_PY=sys.executable,
            LOADER_PY=str(bindir / "fakepy"),
            **{k: v for k, v in extra_env.items() if k in ("MAIN_PIDFILE", "CLEAR_BARRIER")},
        )
        return subprocess.Popen(
            ["bash", str(home / "quickstart" / "postgres" / "quickstart.sh"), stage],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    run_stage.start = start_stage
    run_stage.log_path = str(run / "logging" / "striimautoloader.log")
    run_stage.lock = run / "logging" / "run_100.lock"
    run_stage.script = home / "quickstart" / "postgres" / "quickstart.sh"
    return run_stage


def _inject_before(script, anchor, line):
    """Insert `line` into the copied quickstart.sh just before the line `anchor`."""
    text = script.read_text()
    assert text.count(anchor + "\n") == 1, anchor
    script.write_text(text.replace(anchor + "\n", line + "\n" + anchor + "\n"))


def _wait_for(path, seconds=10):
    import time

    for _ in range(int(seconds * 10)):
        if path.exists():
            return True
        time.sleep(0.1)
    return False


def test_load_never_empties_the_target(qs):
    proc, calls = qs("load")
    assert proc.returncode == 0, proc.stderr
    assert "TRUNCATE" not in calls


def test_clean_empties_the_target_when_no_loader_runs(qs):
    proc, calls = qs("clean")
    assert proc.returncode == 0, proc.stderr
    assert "TRUNCATE demo.orders" in calls
    assert not qs.lock.exists()  # the check releases the lock it took


def test_clean_refuses_while_a_live_loader_holds_the_lock(qs):
    ok, _ = run_safety.acquire_lock(qs.log_path, 100)  # this process, real start time
    assert ok
    try:
        proc, calls = qs("clean")
    finally:
        run_safety.release_lock(qs.log_path, 100)
    assert proc.returncode != 0
    assert "a load is running" in proc.stderr
    assert "TRUNCATE" not in calls


def test_clean_takes_over_a_stale_lock_whose_pid_was_recycled(qs):
    # A crashed loader's pid now belongs to an unrelated live process (this one), but
    # the recorded start time does not match it, so the lock is stale.
    # A different start time of the same kind (/proc tick count or ps text) as this one.
    stale = run_safety.proc_start_time(os.getpid()) + "0"
    qs.lock.write_text(json.dumps({"pid": os.getpid(), "start_time": stale}))
    proc, calls = qs("clean")
    assert proc.returncode == 0, proc.stderr
    assert "TRUNCATE demo.orders" in calls


def test_ctrl_c_during_load_stops_the_loader(qs, tmp_path):
    import signal
    import time

    pidfile = tmp_path / "main.pid"
    env = dict(os.environ, MAIN_PIDFILE=str(pidfile))
    proc = qs.start("load", env)
    for _ in range(100):
        if pidfile.exists() and pidfile.read_text().strip():
            break
        time.sleep(0.1)
    main_pid = int(pidfile.read_text())
    proc.send_signal(signal.SIGINT)
    out, err = proc.communicate(timeout=30)
    assert proc.returncode != 0
    assert "load stopped" in err
    with pytest.raises(ProcessLookupError):
        os.kill(main_pid, 0)


def test_clean_holds_the_lock_through_the_whole_reset(qs, tmp_path):
    # A loader that starts while clean is mid-reset must be refused, and get the lock
    # once clean has finished.
    barrier = tmp_path / "clear"
    proc = qs.start("clean", {"CLEAR_BARRIER": str(barrier)})
    try:
        assert _wait_for(tmp_path / "clear.in"), "clean never reached manage.py clear"
        ok, msg = run_safety.acquire_lock(qs.log_path, 100)
        assert ok is False and "already locked" in msg
    finally:
        (tmp_path / "clear.go").touch()
        out, err = proc.communicate(timeout=30)
    assert proc.returncode == 0, err
    ok, _ = run_safety.acquire_lock(qs.log_path, 100)
    assert ok
    run_safety.release_lock(qs.log_path, 100)


def _stop_load_at(qs, tmp_path, anchor):
    """Signal load at a fixed point in stage_load; return (proc, err, child alive)."""
    import time

    _inject_before(qs.script, anchor, '    kill -TERM "$$"')
    pidfile = tmp_path / "main.pid"
    proc = qs.start("load", {"MAIN_PIDFILE": str(pidfile)})
    out, err = proc.communicate(timeout=30)
    # A surviving loader would write its pid shortly after the shell has gone.
    if not _wait_for(pidfile, seconds=2):
        return proc, err, False
    time.sleep(0.2)
    main_pid = int(pidfile.read_text())
    try:
        os.kill(main_pid, 0)
    except ProcessLookupError:
        return proc, err, False
    os.kill(main_pid, 15)
    return proc, err, True


def test_stop_between_launch_and_pid_stops_the_loader(qs, tmp_path):
    proc, err, alive = _stop_load_at(qs, tmp_path, "    pid=$!")
    assert proc.returncode != 0
    assert "load stopped" in err
    assert not alive


def test_stop_before_launch_stops_the_loader(qs, tmp_path):
    anchor = '    (cd "$RUN_DIR" && exec "$LOADER_PY" "$LOADER_HOME/main.py") > "$log" 2>&1 &'
    proc, err, alive = _stop_load_at(qs, tmp_path, anchor)
    assert proc.returncode != 0
    assert "load stopped" in err
    assert not alive


def test_stop_after_the_loader_exited_still_reports(qs, tmp_path):
    # The loader has finished (fake main.py exits at once) when the signal lands.
    _inject_before(qs.script, '    rc=0; wait "$pid" || rc=$?', '    kill -TERM "$$"')
    proc = qs.start("load", {})
    out, err = proc.communicate(timeout=30)
    assert proc.returncode != 0
    assert "load stopped" in err
