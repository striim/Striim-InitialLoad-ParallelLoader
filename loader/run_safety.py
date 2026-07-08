# run_safety.py
"""Fresh-run guard + run-lock decisions and their marker/lock I/O, plus the one
stall decider (is_stalled). No retry_policy module — F is stall-detect only.
Pure decisions are unit-tested; the file/lock I/O is tested on the real FS.
No heavy/optional deps.
"""

import hashlib
import json
import os
from datetime import datetime, timezone


def queryfile_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _marker_path(log_path, run_id):
    d = os.path.dirname(log_path) or "."
    return os.path.join(d, f"run_{run_id}.marker.json")


def write_marker(log_path, run_id, backend, qf_hash, slice_count):
    path = _marker_path(log_path, run_id)
    payload = {
        "run_id": run_id,
        "backend": backend,
        "queryfile_sha256": qf_hash,
        "slice_count": slice_count,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    return path


def read_marker(log_path, run_id):
    try:
        with open(_marker_path(log_path, run_id)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def delete_marker(log_path, run_id):
    try:
        os.remove(_marker_path(log_path, run_id))
        return True
    except OSError:
        return False


def decide_startup(row_count, marker, current_backend, qf_hash, force_fresh):
    """Return (RESUME|FRESH|REFUSE, reason). See spec workstream B1 truth table."""
    if row_count > 0:
        if marker and marker.get("queryfile_sha256") != qf_hash:
            return (
                "RESUME",
                "queryfile hash differs from the run marker — queryfile edited mid-run? proceeding with existing rows.",
            )
        return ("RESUME", "existing rows found; resuming run.")
    if not marker:
        return ("FRESH", "no existing rows and no run marker — starting a fresh load.")
    if marker.get("queryfile_sha256") == qf_hash:
        if force_fresh:
            return (
                "FRESH",
                "marker matches but --force-fresh set — re-deploying the full load.",
            )
        return (
            "REFUSE",
            f"run marker exists (backend {marker.get('backend')!r}, {marker.get('slice_count')} slices) "
            "but the backend shows 0 rows. STAGE_DB_LOCATION was likely switched or the backend wiped — "
            "re-deploying would DUPLICATE the load. Re-run with --force-fresh to override, or point "
            "STAGE_DB_LOCATION back at the original backend.",
        )
    return (
        "FRESH",
        "marker present but queryfile hash differs — a genuinely new load on this run id; rewriting the marker.",
    )


def is_pid_alive(pid):
    if pid is None:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, ValueError):
        return False
    return True


def proc_start_time(pid):
    """Best-effort process start-time (Linux /proc field 22). None if unavailable."""
    try:
        with open(f"/proc/{int(pid)}/stat") as f:
            return f.read().rsplit(")", 1)[-1].split()[19]
    except (OSError, IndexError, ValueError):
        return None


def acquire_decision(existing_pid, alive, start_time_matches):
    if existing_pid is None:
        return "ACQUIRE"
    if not alive:
        return "TAKEOVER"
    if not start_time_matches:
        return "TAKEOVER"  # recycled PID
    return "REFUSE"


def _lock_path(log_path, run_id):
    d = os.path.dirname(log_path) or "."
    return os.path.join(d, f"run_{run_id}.lock")


def _read_lock(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _write_lock_fd(fd, pid, start_time_fn):
    """Write pid+start_time JSON to an already-open file descriptor, then close it."""
    payload = json.dumps({"pid": pid, "start_time": start_time_fn(pid)}).encode()
    os.write(fd, payload)
    os.close(fd)


def acquire_lock(
    log_path, run_id, pid=None, alive_fn=is_pid_alive, start_time_fn=proc_start_time
):
    """Atomic PID+start-time lock using O_CREAT|O_EXCL as the hard exclusion floor.

    Returns (ok, message).  REFUSE always names the lockfile + manual override.

    Branches on FileExistsError:
      - Readable + live pid + matching start_time  → REFUSE (holder is running)
      - Readable + dead/recycled pid               → TAKEOVER (atomic re-create with O_EXCL)
      - Empty / unreadable / JSON-parse-fails      → REFUSE (back off; winner may be
            mid-write; a crash leaving an empty file requires manual rm — fails-closed)
    """
    pid = os.getpid() if pid is None else pid
    path = _lock_path(log_path, run_id)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    # --- Primary path: atomic exclusive create ---
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        _write_lock_fd(fd, pid, start_time_fn)
        return (True, f"acquired run lock {path} (pid {pid})")
    except FileExistsError:
        pass

    # --- File already exists: read and branch ---
    existing = _read_lock(path)

    # Empty / unreadable / JSON-invalid → back off; O_EXCL winner may be mid-write
    if existing is None:
        return (
            False,
            f"run {run_id} lockfile {path} exists but is empty or unreadable — "
            f"another process may be mid-acquire. If the file is stale, "
            f"remove {path} to override.",
        )

    epid = existing.get("pid")
    alive = alive_fn(epid)
    est = existing.get("start_time")
    cur_est = start_time_fn(epid) if alive else None
    start_time_matches = est is not None and est == cur_est
    decision = acquire_decision(epid, alive, start_time_matches)

    if decision == "REFUSE":
        return (
            False,
            f"run {run_id} is already locked by PID {epid} (lockfile {path}). "
            f"If that process is gone, remove {path} to override.",
        )

    # TAKEOVER: stale lock (dead PID or recycled PID with different start_time)
    try:
        os.remove(path)
    except OSError:
        pass  # Concurrent takeover already removed it — proceed to O_EXCL re-create

    # Atomic re-create — if another concurrent takeover beat us, fall back to REFUSE
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        _write_lock_fd(fd, pid, start_time_fn)
        return (
            True,
            f"acquired run lock {path} (pid {pid}) [takeover from stale PID {epid}]",
        )
    except FileExistsError:
        return (
            False,
            f"run {run_id} lockfile {path} was claimed by another process during takeover. "
            f"Remove {path} to override.",
        )


def release_lock(log_path, run_id):
    try:
        os.remove(_lock_path(log_path, run_id))
        return True
    except OSError:
        return False


def is_stalled(started_datetime, now, max_runtime_seconds):
    """A slice RUNNING longer than max_runtime_seconds is stalled. 0 = disabled."""
    if not max_runtime_seconds or max_runtime_seconds <= 0:
        return False
    if started_datetime is None:
        return False
    s = started_datetime
    if s.tzinfo is None:
        s = s.replace(tzinfo=timezone.utc)
    n = now
    if n.tzinfo is None:
        n = n.replace(tzinfo=timezone.utc)
    return (n - s).total_seconds() > max_runtime_seconds
