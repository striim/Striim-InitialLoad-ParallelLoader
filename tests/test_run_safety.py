# tests/test_run_safety.py
import json
import os

import run_safety


def _marker(qf_hash="abc", backend="PG", slice_count=3):
    return {
        "run_id": 100,
        "backend": backend,
        "queryfile_sha256": qf_hash,
        "slice_count": slice_count,
    }


def test_queryfile_hash_stable(tmp_path):
    p = tmp_path / "qf.txt"
    p.write_text("SELECT 1|T\n")
    h1 = run_safety.queryfile_hash(str(p))
    h2 = run_safety.queryfile_hash(str(p))
    assert h1 == h2 and len(h1) == 64


def test_marker_round_trip(tmp_path):
    log = str(tmp_path / "logging" / "app.log")
    path = run_safety.write_marker(log, 100, "PG", "deadbeef", 7)
    assert path.endswith("run_100.marker.json")
    m = run_safety.read_marker(log, 100)
    assert (
        m["backend"] == "PG"
        and m["queryfile_sha256"] == "deadbeef"
        and m["slice_count"] == 7
    )
    assert run_safety.delete_marker(log, 100) is True
    assert run_safety.read_marker(log, 100) is None
    assert run_safety.delete_marker(log, 100) is False


def test_decide_rows_present_resume():
    d, _ = run_safety.decide_startup(5, None, "PG", "h", False)
    assert d == "RESUME"


def test_decide_rows_present_warns_on_hash_diff():
    d, reason = run_safety.decide_startup(5, _marker(qf_hash="OLD"), "PG", "NEW", False)
    assert d == "RESUME" and "differ" in reason.lower()


def test_decide_zero_no_marker_fresh():
    d, _ = run_safety.decide_startup(0, None, "PG", "h", False)
    assert d == "FRESH"


def test_decide_zero_marker_hash_match_refuse():
    d, reason = run_safety.decide_startup(
        0, _marker(qf_hash="SAME"), "PG", "SAME", False
    )
    assert d == "REFUSE" and "force-fresh" in reason


def test_decide_zero_marker_hash_match_force_fresh():
    d, _ = run_safety.decide_startup(0, _marker(qf_hash="SAME"), "PG", "SAME", True)
    assert d == "FRESH"


def test_decide_zero_marker_hash_differ_fresh():
    d, _ = run_safety.decide_startup(0, _marker(qf_hash="OLD"), "PG", "NEW", False)
    assert d == "FRESH"


import os


def test_is_pid_alive_self():
    assert run_safety.is_pid_alive(os.getpid()) is True


def test_is_pid_alive_none_false():
    assert run_safety.is_pid_alive(None) is False


def test_acquire_decision_matrix():
    assert run_safety.acquire_decision(None, False, False) == "ACQUIRE"
    assert run_safety.acquire_decision(999, False, False) == "TAKEOVER"
    assert run_safety.acquire_decision(999, True, False) == "TAKEOVER"  # recycled PID
    assert run_safety.acquire_decision(999, True, True) == "REFUSE"


def test_acquire_lock_fresh(tmp_path):
    log = str(tmp_path / "logging" / "app.log")
    ok, msg = run_safety.acquire_lock(log, 100, pid=4242, start_time_fn=lambda p: "111")
    assert ok is True and "4242" in msg
    assert run_safety.release_lock(log, 100) is True


def test_acquire_lock_takeover_dead_pid(tmp_path):
    log = str(tmp_path / "logging" / "app.log")
    run_safety.acquire_lock(log, 100, pid=4242, start_time_fn=lambda p: "111")
    ok, _ = run_safety.acquire_lock(
        log, 100, pid=5555, alive_fn=lambda p: False, start_time_fn=lambda p: "222"
    )
    assert ok is True  # previous holder dead -> take over


def test_acquire_lock_refuse_live_holder(tmp_path):
    log = str(tmp_path / "logging" / "app.log")
    run_safety.acquire_lock(log, 100, pid=4242, start_time_fn=lambda p: "111")
    ok, msg = run_safety.acquire_lock(
        log, 100, pid=5555, alive_fn=lambda p: True, start_time_fn=lambda p: "111"
    )
    assert ok is False and "run_100.lock" in msg and "remove" in msg.lower()


import datetime as _dt


def test_is_stalled_disabled_when_zero():
    s = _dt.datetime(2024, 1, 1, 0, 0, 0)
    now = _dt.datetime(2024, 1, 1, 5, 0, 0)
    assert run_safety.is_stalled(s, now, 0) is False


def test_is_stalled_under_bound():
    s = _dt.datetime(2024, 1, 1, 0, 0, 0)
    now = _dt.datetime(2024, 1, 1, 0, 0, 30)
    assert run_safety.is_stalled(s, now, 60) is False


def test_is_stalled_at_bound_false():
    s = _dt.datetime(2024, 1, 1, 0, 0, 0)
    now = _dt.datetime(2024, 1, 1, 0, 1, 0)
    assert run_safety.is_stalled(s, now, 60) is False


def test_is_stalled_over_bound():
    s = _dt.datetime(2024, 1, 1, 0, 0, 0)
    now = _dt.datetime(2024, 1, 1, 0, 2, 0)
    assert run_safety.is_stalled(s, now, 60) is True


def test_is_stalled_none_started():
    now = _dt.datetime(2024, 1, 1, 0, 2, 0)
    assert run_safety.is_stalled(None, now, 60) is False


def test_is_stalled_tz_aware_now_naive_started():
    s = _dt.datetime(2024, 1, 1, 0, 0, 0)  # naive -> assumed UTC
    now = _dt.datetime(2024, 1, 1, 0, 5, 0, tzinfo=_dt.timezone.utc)
    assert run_safety.is_stalled(s, now, 60) is True


# ── NEW-2 atomic O_EXCL tests ──────────────────────────────────────────────


def test_new2_sequential_first_acquires_second_refuses_live_pid(tmp_path):
    """(a) Sequential: first acquire succeeds; second call with a live holder is REFUSED."""
    log = str(tmp_path / "logging" / "app.log")
    ok1, msg1 = run_safety.acquire_lock(
        log, 200, pid=1001, alive_fn=lambda p: False, start_time_fn=lambda p: "t1"
    )
    assert ok1 is True, f"first acquire should succeed; got: {msg1}"

    # Second caller — holder 1001 is alive + start_time matches
    ok2, msg2 = run_safety.acquire_lock(
        log, 200, pid=1002, alive_fn=lambda p: True, start_time_fn=lambda p: "t1"
    )
    assert ok2 is False, "second acquire must be REFUSED when holder is live"
    assert "run_200.lock" in msg2
    assert "remove" in msg2.lower()


def test_new2_stale_dead_pid_takeover(tmp_path):
    """(b) Dead-PID lockfile is taken over atomically."""
    log = str(tmp_path / "logging" / "app.log")
    # Establish a lock for a 'dead' pid
    run_safety.acquire_lock(
        log, 201, pid=9999, alive_fn=lambda p: False, start_time_fn=lambda p: "t2"
    )
    ok, msg = run_safety.acquire_lock(
        log, 201, pid=8888, alive_fn=lambda p: False, start_time_fn=lambda p: "t3"
    )
    assert ok is True, f"takeover of dead-pid lock should succeed; got: {msg}"
    assert "8888" in msg


def test_new2_empty_lockfile_refuses_not_acquires(tmp_path):
    """(c) An empty lockfile (e.g. crash mid-write) must REFUSE, never silently acquire."""
    log = str(tmp_path / "logging" / "app.log")
    lock_dir = tmp_path / "logging"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_file = lock_dir / "run_202.lock"
    lock_file.write_bytes(
        b""
    )  # empty — simulates crash between O_EXCL create and write

    ok, msg = run_safety.acquire_lock(
        log, 202, pid=7777, alive_fn=lambda p: True, start_time_fn=lambda p: "t4"
    )
    assert ok is False, "empty lockfile must REFUSE (back off), not acquire"
    assert "run_202.lock" in msg or "202" in msg


def test_new2_excl_create_only_one_winner(tmp_path):
    """(d) O_EXCL guarantee: of two back-to-back O_CREAT|O_EXCL attempts on the same
    path, exactly one raises FileExistsError — verifying the kernel primitive we rely on.
    """
    lock_path = str(tmp_path / "race.lock")
    results = []
    for _ in range(2):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.close(fd)
            results.append("WIN")
        except FileExistsError:
            results.append("LOSE")
    assert (
        results.count("WIN") == 1
    ), f"exactly one O_EXCL winner expected; got {results}"
    assert results.count("LOSE") == 1


def test_live_pid_with_unknown_start_time_is_refused(tmp_path):
    # Without a readable start time (no /proc and no ps) a live PID cannot be shown to
    # be recycled, so a second loader must not take over the lock.
    log = str(tmp_path / "logging" / "x.log")
    os.makedirs(os.path.dirname(log))
    with open(os.path.join(os.path.dirname(log), "run_7.lock"), "w") as f:
        json.dump({"pid": 4242, "start_time": None}, f)
    ok, msg = run_safety.acquire_lock(
        log, 7, pid=5555, alive_fn=lambda p: True, start_time_fn=lambda p: None
    )
    assert ok is False and "already locked" in msg


def test_recycled_pid_with_known_differing_start_time_is_taken_over(tmp_path):
    log = str(tmp_path / "logging" / "x.log")
    os.makedirs(os.path.dirname(log))
    with open(os.path.join(os.path.dirname(log), "run_7.lock"), "w") as f:
        json.dump({"pid": 4242, "start_time": "old"}, f)
    ok, _ = run_safety.acquire_lock(
        log, 7, pid=5555, alive_fn=lambda p: True, start_time_fn=lambda p: "new"
    )
    assert ok is True


def test_proc_start_time_is_known_for_this_process():
    # /proc on Linux, ps on macOS: either way the running test process has a start time.
    assert run_safety.proc_start_time(os.getpid())
