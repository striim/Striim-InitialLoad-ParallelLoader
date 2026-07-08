"""Operator CLI for the Striim InitialLoad ParallelLoader.

Subcommands: status, clear, reset, logs, split, probe, board, setup, reconcile.
`data` and Oracle modules are imported lazily so the pure helpers and the
state commands work without heavy/optional dependencies loaded up front.
"""

import argparse
import logging
import os
import sys
import time
from collections import namedtuple

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "loader"))

import config
import reconcile

_logger = logging.getLogger(__name__)

DONE = frozenset({"COMPLETED", "FAILED", "COMPLETED-FAILEDDROP"})
RESET_BASE = frozenset({"FAILED"})
FAILEDDROP = "COMPLETED-FAILEDDROP"


def _valid_port_input(entered):
    """True if ``entered`` is an acceptable port answer in the setup wizard: blank
    (keep current) or a positive integer. Non-numeric / 0 / negative are rejected."""
    entered = (entered or "").strip()
    return entered == "" or (entered.isdigit() and int(entered) >= 1)


def positive_int(value):
    """argparse type: accept only integers >= 1.

    Rejects non-numeric, zero, and negative values at parse time with a clean
    argparse error (exit 2) instead of letting e.g. ``--chunks 0`` reach boundary
    math and raise ZeroDivisionError, or a negative tunable silently produce a
    degenerate single-slice result.
    """
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(f"{value!r} is not an integer")
    if n < 1:
        raise argparse.ArgumentTypeError(
            f"must be a positive integer (>= 1), got {n}"
        )
    return n


def int_csv(value):
    """argparse type: comma-separated positive ints (e.g. ``1,2,4,8``) -> [1,2,4,8]."""
    out = []
    for part in str(value).split(","):
        part = part.strip()
        if not part:
            continue
        out.append(positive_int(part))
    if not out:
        raise argparse.ArgumentTypeError(f"{value!r} is not a comma-separated int list")
    return out


# --- pure helpers (unit-tested) -------------------------------------------
def classify_run(rows):
    if not rows:
        return "NOT STARTED"
    if any(r.status not in DONE for r in rows):
        return "IN PROGRESS"
    return "FINISHED"


def status_counts(rows):
    counts = {}
    for r in rows:
        counts[r.status] = counts.get(r.status, 0) + 1
    return counts


def status_headline(rows):
    """Display headline that never shows a bare green FINISHED over failures."""
    state = classify_run(rows)
    if state == "FINISHED" and reconcile.final_verdict(rows) != "ALL_COMPLETE":
        return "FINISHED (with FAILURES — run not cleared)"
    return state


def schema_diff(expected_cols, actual_cols):
    """Pure: case-insensitive column-set diff. Returns (ok, missing, extra)."""
    exp = {c.lower(): c for c in expected_cols}
    act = {c.lower(): c for c in actual_cols}
    missing = sorted(exp[k] for k in exp if k not in act)
    extra = sorted(act[k] for k in act if k not in exp)
    return (not missing and not extra, missing, extra)


def rows_payload(rows):
    """Compact, JSON-serialisable view of the current rows for the bash dashboard."""
    return [
        {
            "roworder": r.roworder,
            "status": r.status,
            "targettbl": r.targettbl,
            "appname": r.appname,
            "notes": r.notes,
        }
        for r in rows
    ]


def build_reset_rows(rows, include_faileddrop=False):
    targets = set(RESET_BASE)
    if include_faileddrop:
        targets.add(FAILEDDROP)
    out = []
    for r in rows:
        if r.status in targets:
            r.status = "NEW"
            r.appname = ""
            r.namespace = ""
            r.notes = ""
            r.started_datetime = None
            r.finished_datetime = None
            out.append(r)
    return out


def ns_base_for_run(run_id):
    """Namespace prefix for a run id, e.g. ns_base_for_run(100) == 'ILA_100_'."""
    return f"ILA_{run_id}_"


def apps_to_clean(app_names, ns_prefix, only=None, protect=None):
    """Pure: ILA app names under ns_prefix that should be dropped.

    only: if given (set of namespaces), restrict to those.
    protect: namespaces to never drop (healthy in-flight slices).
    """
    only = set(only) if only is not None else None
    protect = set(protect) if protect else set()
    out = []
    for name in app_names:
        ns = name.split(".")[0]
        if not ns.startswith(ns_prefix):
            continue
        if only is not None and ns not in only:
            continue
        if ns in protect:
            continue
        out.append(name)
    return out


def failed_namespaces(rows, include_faileddrop=False):
    """Namespaces of FAILED rows — snapshot BEFORE build_reset_rows blanks them."""
    targets = {"FAILED"}
    if include_faileddrop:
        targets.add(FAILEDDROP)
    return {r.namespace for r in rows if r.status in targets and r.namespace}


def _read_current(run_id):
    import data

    return data.read_data(f"iscurrentrow = True AND uniquerunid = {int(run_id)}")


def _confirm(prompt, assume_yes):
    if assume_yes:
        return True
    return input(f"{prompt} [y/N]: ").strip().lower() in ("y", "yes")


# --- setup pure helpers (unit-tested; no heavy deps) ----------------------
def normalize_backend(value):
    """Mirror data.get_database() but standalone (no heavy deps)."""
    v = (value or "").upper()
    if v in ("BQ", "BIGQUERY"):
        return "BQ"
    if v in ("PG", "POSTGRES", "POSTGRESQL"):
        return "PG"
    if v in ("ORACLE", "ORA"):
        return "ORACLE"
    return "TinyDB"


_BACKEND_ENV = {
    "TinyDB": [],
    "PG": [
        "PG_HOST",
        "PG_PORT",
        "PG_DATABASE",
        "PG_USER",
        "PG_PASSWORD",
        "PG_TABLE_ID",
        "PG_SSLMODE",
    ],
    "BQ": ["BQ_KEYFILE_LOCATION", "BQ_PROJECT_ID", "BQ_DATASET_ID", "BQ_TABLE_ID"],
    "ORACLE": [
        "ORCH_ORACLE_DSN",
        "ORCH_ORACLE_HOST",
        "ORCH_ORACLE_PORT",
        "ORCH_ORACLE_SERVICE",
        "ORCH_ORACLE_USER",
        "ORCH_ORACLE_PASSWORD",
        "ORCH_ORACLE_TABLE_ID",
    ],
}


def required_env_vars(backend):
    return _BACKEND_ENV.get(normalize_backend(backend), [])


def env_block(backend):
    return "\n".join(f"export {v}=" for v in required_env_vars(backend))


# --- interactive setup pure helpers (unit-tested; no heavy deps, no I/O) ----
CredField = namedtuple("CredField", ["env_var", "label", "secret", "optional"])


def credential_spec(backend, source_engine):
    """Ordered credential fields to collect for (state backend, source engine).

    Returns a list of CredField(env_var, label, secret, optional). Pure — no
    I/O. Uses the exact env var names from config.py; discrete host/port/service
    fields only (never the ``*_DSN`` forms, which stay a manual override).
    """
    b = normalize_backend(backend)
    eng = (source_engine or "oracle").strip().lower()

    fields = [
        CredField("STRIIM_NODE", "Striim node (host:port)", False, False),
        CredField("STRIIM_ADMIN_USER", "Striim admin user", False, False),
        CredField("STRIIM_ADMIN_PWD", "Striim admin password", True, False),
        CredField("STRIIM_API_TOKEN", "Striim API token (optional)", True, True),
    ]

    # State backend fields (TinyDB needs none).
    if b == "PG":
        fields += [
            CredField("PG_HOST", "State PG host", False, False),
            CredField("PG_PORT", "State PG port", False, False),
            CredField("PG_DATABASE", "State PG database", False, False),
            CredField("PG_USER", "State PG user", False, False),
            CredField("PG_PASSWORD", "State PG password", True, False),
            CredField("PG_SSLMODE", "State PG sslmode (optional)", False, True),
        ]
    elif b == "BQ":
        fields += [
            CredField(
                "BQ_KEYFILE_LOCATION",
                "BigQuery service-account keyfile path",
                False,
                False,
            ),
            CredField("BQ_PROJECT_ID", "BigQuery project id", False, False),
            CredField("BQ_DATASET_ID", "BigQuery dataset id", False, False),
            CredField("BQ_TABLE_ID", "BigQuery table id (optional)", False, True),
        ]
    elif b == "ORACLE":
        fields += [
            CredField("ORCH_ORACLE_HOST", "State Oracle host", False, False),
            CredField("ORCH_ORACLE_PORT", "State Oracle port", False, False),
            CredField("ORCH_ORACLE_SERVICE", "State Oracle service name", False, False),
            CredField("ORCH_ORACLE_USER", "State Oracle user", False, False),
            CredField("ORCH_ORACLE_PASSWORD", "State Oracle password", True, False),
        ]

    # Source engine fields.
    if eng == "oracle":
        fields += [
            CredField("ORACLE_HOST", "Source Oracle host", False, False),
            CredField("ORACLE_PORT", "Source Oracle port", False, False),
            CredField("ORACLE_SERVICE", "Source Oracle service name", False, False),
            CredField("ORACLE_USER", "Source Oracle user", False, False),
            CredField("ORACLE_PASSWORD", "Source Oracle password", True, False),
        ]
    elif eng == "postgres":
        fields += [
            CredField("SOURCE_PG_HOST", "Source PG host", False, False),
            CredField("SOURCE_PG_PORT", "Source PG port", False, False),
            CredField("SOURCE_PG_DATABASE", "Source PG database", False, False),
            CredField("SOURCE_PG_USER", "Source PG user", False, False),
            CredField("SOURCE_PG_PASSWORD", "Source PG password", True, False),
        ]
    elif eng == "sqlserver":
        fields += [
            CredField("SQLSERVER_HOST", "Source SQL Server host", False, False),
            CredField("SQLSERVER_PORT", "Source SQL Server port", False, False),
            CredField("SQLSERVER_DATABASE", "Source SQL Server database", False, False),
            CredField("SQLSERVER_USER", "Source SQL Server user", False, False),
            CredField("SQLSERVER_PASSWORD", "Source SQL Server password", True, False),
            CredField(
                "SQLSERVER_DRIVER",
                "Source SQL Server ODBC driver (optional)",
                False,
                True,
            ),
        ]
    elif eng == "jdbc":
        fields += [
            CredField("JDBC_DRIVER_CLASS", "JDBC driver class", False, False),
            CredField("JDBC_URL", "JDBC URL", False, False),
            CredField("JDBC_JAR_PATH", "JDBC driver jar path", False, False),
            CredField("JDBC_USER", "JDBC user", False, False),
            CredField("JDBC_PASSWORD", "JDBC password", True, False),
        ]

    return fields


def mask_value(value, secret):
    """Display form of a credential value for a masked summary. Pure.

    secret + set   -> "******** (set)"
    secret + empty -> "(unset)"
    non-secret     -> the value, or "(unset)" when empty
    """
    if secret:
        return "******** (set)" if value else "(unset)"
    return value if value else "(unset)"


def merge_env_lines(existing_text, updates):
    """Merge ``{KEY: value}`` into ``.env`` text and return the new text. Pure.

    Existing ``KEY=`` lines are updated in place; new keys are appended at the
    end (in ``updates`` order); all other/unrelated lines and comments are
    preserved.
    """
    remaining = dict(updates)
    out = []
    for line in existing_text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in remaining:
                out.append(f"{key}={remaining.pop(key)}")
                continue
        out.append(line)
    for key, value in updates.items():
        if key in remaining:
            out.append(f"{key}={value}")
    text = "\n".join(out)
    if text and not text.endswith("\n"):
        text += "\n"
    return text


def ensure_gitignore_has(text, entry):
    """Return ``.gitignore`` text guaranteed to contain ``entry`` on its own
    line. Appends with a trailing newline if missing; idempotent. Pure."""
    if entry in [ln.strip() for ln in text.splitlines()]:
        return text
    if text and not text.endswith("\n"):
        text += "\n"
    return text + entry + "\n"


def _bq_ddl_from_template(template, project, dataset, table):
    """Replace placeholder identifiers in BQ_TableCreate.sql for idempotent DDL."""
    fq = f"{project}.{dataset}.{table}"
    result = template.replace(
        "YOUR_PROJECT_ID.YOUR_DATASET_ID.striim_orchestration", fq
    )
    result = result.replace("CREATE TABLE `", "CREATE TABLE IF NOT EXISTS `", 1)
    return result


# --- commands --------------------------------------------------------------
def cmd_status(args):
    if args.all_runs:
        import data

        rows = data.read_data("iscurrentrow = True")
        by_run = {}
        for r in rows:
            by_run.setdefault(r.uniquerunid, []).append(r)
        for run in sorted(by_run):
            grp = by_run[run]
            print(f"Run {run}: {classify_run(grp)}  {status_counts(grp)}")
        return 0

    rows = _read_current(args.run_id)
    state = classify_run(rows)
    headline = status_headline(rows)
    if args.json:
        import json

        payload = {"run_id": args.run_id, "state": state, "counts": status_counts(rows)}
        if args.rows:
            payload["rows"] = rows_payload(rows)
        print(json.dumps(payload))
        return 0

    print(f"Run {args.run_id}: {headline}")
    print(f"  counts: {status_counts(rows) or '(none)'}")
    if not args.failed:
        inflight = [r for r in rows if r.status == "RUNNING"]
        if inflight:
            print("  in-flight:")
            for r in inflight:
                print(
                    f"    {r.appname}  ->  {r.targettbl}  (started {r.started_datetime})"
                )
    failed_statuses = ("FAILED", FAILEDDROP) if args.failed else ("FAILED",)
    failed = [r for r in rows if r.status in failed_statuses]
    if failed:
        print("  failed:")
        for r in failed:
            print(f"    #{r.roworder} {r.targettbl}: {r.notes}")
    return 0


def cmd_clear(args):
    import data

    rows = _read_current(args.run_id)
    if not rows:
        print(f"Run {args.run_id}: nothing to clear.")
        return 0
    verb = "DELETE" if args.hard else "retire (iscurrentrow=FALSE)"
    if not _confirm(f"{verb} {len(rows)} rows for run {args.run_id}?", args.yes):
        print("Aborted.")
        return 1
    if args.with_striim_cleanup:
        _striim_cleanup(args.run_id)
    if args.hard:
        data.delete_runid(args.run_id)
    else:
        data.clear_runid(args.run_id)
    import run_safety

    run_safety.delete_marker(config.LOG_OUTPUT_PATH, args.run_id)
    print("Done.")
    return 0


def cmd_reset(args):
    import data

    rows = _read_current(args.run_id)
    failed_ns = failed_namespaces(
        rows, include_faileddrop=args.include_faileddrop
    )  # snapshot BEFORE build_reset_rows blanks them
    to_reset = build_reset_rows(rows, include_faileddrop=args.include_faileddrop)
    if not to_reset:
        print(f"Run {args.run_id}: no failed rows to reset.")
        return 0
    if not _confirm(
        f"Reset {len(to_reset)} failed rows to NEW for run {args.run_id}?", args.yes
    ):
        print("Aborted.")
        return 1
    if args.with_striim_cleanup:
        _striim_cleanup(args.run_id, only_namespaces=failed_ns)
    data.write_data(to_reset)
    print(f"Reset {len(to_reset)} rows. Re-run `python main.py` to redo them.")
    return 0


def cmd_logs(args):
    path = config.LOG_OUTPUT_PATH
    if not os.path.exists(path):
        print(f"No log file at {path}")
        return 1
    if args.follow:
        with open(path) as f:
            f.seek(0, os.SEEK_END)
            try:
                while True:
                    line = f.readline()
                    if line:
                        if not args.errors or _is_err(line):
                            sys.stdout.write(line)
                    else:
                        time.sleep(0.5)
            except KeyboardInterrupt:
                return 0
    with open(path) as f:
        lines = f.readlines()
    if args.errors:
        lines = [ln for ln in lines if _is_err(ln)]
    sys.stdout.writelines(lines[-args.lines :])
    return 0


def _is_err(line):
    low = line.lower()
    return "fail" in low or "error" in low or "exception" in low


def _striim_cleanup(run_id, only_namespaces=None):
    """Opt-in: stop/undeploy/drop leftover ILA_<run_id>_* apps + namespaces for THIS run.
    only_namespaces (set) restricts the drop to those namespaces (healthy slices kept).
    Prints a dry-run preview of exactly which apps will be dropped before acting."""
    import main  # reuses authenticate() + mon

    main.headers = main.authenticate()
    ns_prefix = ns_base_for_run(run_id)
    app_names = [a.full_name for a in main.doGetMonOutputAndReview()]
    to_drop = apps_to_clean(app_names, ns_prefix, only=only_namespaces)
    print(f"[cleanup] run {run_id}: dropping {len(to_drop)} app(s): {to_drop}")
    for name in to_drop:
        ns = name.split(".")[0]
        print(main.runCommand("STOP APPLICATION " + name + ";"))
        print(main.runCommand("UNDEPLOY APPLICATION " + name + ";"))
        print(main.runCommand("DROP APPLICATION " + name + " CASCADE;"))
        try:
            main.resetNamespace(ns)
        except Exception as e:
            print("Error at resetNamespace:", e)


def cmd_split(args):
    from split_runner import run_split  # added in Task 6

    return run_split(args, source_engine=args.source_engine)


def cmd_split_batch(args):
    """Fan a pasted batch of queries into one queryfile.

    Reads a JSON manifest (array of entries) built by the manage.sh build wizard:
    splittable entries (~SPLIT~) are probed+split, plain entries are appended
    verbatim. Kept thin — all logic lives in loader/batch_split.run_batch.
    """
    import json

    from batch_split import BatchError, run_batch

    try:
        with open(args.manifest) as f:
            entries = json.load(f)
    except (OSError, ValueError) as e:
        print(f"[batch] could not read manifest {args.manifest}: {e}")
        return 2
    if not isinstance(entries, list):
        print("[batch] manifest must be a JSON array of query entries.")
        return 2

    try:
        return run_batch(
            entries,
            args.output,
            source_engine=args.source_engine,
            depth=args.depth,
            probe=args.probe,
            default_strategy=args.strategy,
            default_chunks=args.chunks,
            explain=args.explain,
            assort=args.assort,
        )
    except BatchError as e:
        print(f"[batch] {e}")
        return 1


def cmd_reconcile(args):
    """SCN-anchored source row-count reconcile + completeness gate.

    Live source-Oracle exec is operator-verified. Degrades cleanly on (a) missing
    SCN -> state-only gate; (b) ORA-01555 -> labeled live (non-SCN-anchored) COUNT. Never raises.
    """
    import json

    import watermark

    rows = _read_current(args.run_id)
    if not rows:
        print(f"Run {args.run_id}: nothing to reconcile (no current rows).")
        return 1
    verdict = reconcile.final_verdict(rows)
    print(f"Run {args.run_id}: gate verdict = {verdict}")
    if verdict != "ALL_COMPLETE":
        print(f"  offending: {reconcile.offending_counts(rows)}")

    scn = None
    sc_path = watermark.sidecar_path(config.LOG_OUTPUT_PATH, args.run_id)
    try:
        with open(sc_path) as f:
            scn = json.load(f).get("value")
    except (OSError, ValueError):
        scn = None
    if not scn:
        print("  No watermark SCN sidecar — state-only reconcile (gate only).")
        print(
            "  Manually run SELECT COUNT(*) on each target table and compare to source."
        )
        return 0 if verdict == "ALL_COMPLETE" else 1

    per_slice = []
    targets = []
    flashback_lost = False
    flashback_lost_slice = None
    no_snapshot = False
    try:
        from source_dialect import get_dialect

        dialect = get_dialect()
        conn = dialect.get_connection()
        try:
            enable_sql = dialect.snapshot_enable_sql(scn)
            no_snapshot = enable_sql is None
            if no_snapshot:
                print(
                    "  WARNING: this source engine has no point-in-time"
                    " reconcile — counts are LIVE (not snapshot-anchored)."
                )
            else:
                with conn.cursor() as cur:
                    cur.execute(enable_sql)
            try:
                for r in rows:
                    try:
                        if flashback_lost or no_snapshot:
                            # No snapshot for this engine, or the flashback undo
                            # for this SCN is gone (ORA-01555) — live-count
                            # directly. Re-enabling would just re-hit ORA-01555.
                            inner = r.query.rstrip().rstrip(";").rstrip()
                            with conn.cursor() as cur:
                                cur.execute(f"SELECT COUNT(*) FROM ({inner})")
                                per_slice.append(int(cur.fetchone()[0]))
                        else:
                            with conn.cursor() as cur:
                                cur.execute(reconcile.reconcile_count_sql(r.query, scn))
                                per_slice.append(int(cur.fetchone()[0]))
                    except Exception as exc:  # noqa: BLE001
                        if dialect.is_snapshot_lost(exc):
                            flashback_lost = True
                            flashback_lost_slice = r.roworder
                            print(
                                f"  WARNING: ORA-01555 at SCN {scn} for slice"
                                f" #{r.roworder}; falling back to a LIVE"
                                " (non-SCN-anchored) COUNT for this and all"
                                " remaining slices."
                            )
                            disable_sql = dialect.snapshot_disable_sql()
                            if disable_sql is not None:
                                with conn.cursor() as cur:
                                    cur.execute(disable_sql)
                            inner = r.query.rstrip().rstrip(";").rstrip()
                            with conn.cursor() as cur:
                                cur.execute(f"SELECT COUNT(*) FROM ({inner})")
                                per_slice.append(int(cur.fetchone()[0]))
                        else:
                            raise
                    targets.append(r.targettbl)
            finally:
                try:
                    disable_sql = dialect.snapshot_disable_sql()
                    if disable_sql is not None:
                        with conn.cursor() as cur:
                            cur.execute(disable_sql)
                except (
                    Exception
                ):  # noqa: BLE001 - best-effort; already disabled on ORA-01555 path
                    pass
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - reconcile must never hard-fail
        print(f"  Source reconcile could not complete: {exc}")
        return 1

    if flashback_lost:
        _warn = (
            f"  WARNING: reconcile result is NOT SCN-anchored — "
            f"flashback SCN {scn} expired from slice #{flashback_lost_slice}"
            " onward; counts reflect CURRENT source data, not the original"
            " snapshot."
        )
        print(_warn)
        _logger.warning(_warn.strip())

    summary = reconcile.summarize(
        per_slice,
        rows,
        flashback_lost=flashback_lost,
        flashback_lost_slice=flashback_lost_slice,
    )
    if flashback_lost:
        print(
            f"  live counts — flashback SCN {scn} too old from slice"
            f" #{flashback_lost_slice} onward; NOT SCN-anchored"
            f" (counts reflect CURRENT source data):"
            f" {summary['expected_source_rows']}"
            f" across {summary['slice_count']} slices"
        )
    elif no_snapshot:
        print(
            f"  live counts — this engine has no point-in-time reconcile;"
            f" NOT snapshot-anchored (counts reflect CURRENT source data):"
            f" {summary['expected_source_rows']}"
            f" across {summary['slice_count']} slices"
        )
    else:
        print(
            f"  expected source rows (AS OF SCN {scn}):"
            f" {summary['expected_source_rows']}"
            f" across {summary['slice_count']} slices"
        )
    print("  Target-side check — run these against YOUR target DB and sum the counts:")
    for t in sorted(set(targets)):
        print(f"    SELECT COUNT(*) FROM {t};")
    return 0 if verdict == "ALL_COMPLETE" else 1


# --- board helpers (pure + best-effort) ------------------------------------
def _board_payload(run_id, state, counts, rows, metrics, backend, source_engine):
    total = sum(counts.values()) if counts else 0
    done = sum(
        counts.get(s, 0) for s in ("COMPLETED", "FAILED", "COMPLETED-FAILEDDROP")
    )

    def m(ns):
        return metrics.get(ns) or {}

    inflight = [
        {
            "roworder": r.roworder,
            "targettbl": r.targettbl,
            "appname": r.appname,
            "namespace": r.namespace,
            "started": str(r.started_datetime) if r.started_datetime else None,
            "rate": m(r.namespace).get("rate"),
            "rows": m(r.namespace).get("rows"),
        }
        for r in rows
        if r.status == "RUNNING"
    ]
    recent = [
        {
            "roworder": r.roworder,
            "targettbl": r.targettbl,
            "status": r.status,
            "finished": str(r.finished_datetime) if r.finished_datetime else None,
        }
        for r in rows
        if r.status in ("COMPLETED", "FAILED", "COMPLETED-FAILEDDROP")
    ][-10:]
    return {
        "run_id": run_id,
        "state": state,
        "backend": backend,
        "source_engine": source_engine,
        "counts": counts,
        "total": total,
        "done": done,
        "pct_complete": round(done / total, 4) if total else 0.0,
        "inflight": inflight,
        "recent": recent,
        "striim": bool(metrics),
    }


def _safe_striim_metrics(namespaces, timeout=4):
    """Best-effort Striim rates, wrapped in a hard timeout (app_metrics' HTTP call can block)."""
    if not namespaces:
        return {}
    try:
        import striim_monitor

        if not striim_monitor.available():
            return {}
        from concurrent.futures import ThreadPoolExecutor

        ex = ThreadPoolExecutor(max_workers=1)
        try:
            return ex.submit(striim_monitor.app_metrics, list(namespaces)).result(
                timeout=timeout
            )
        finally:
            ex.shutdown(wait=False)
    except Exception:  # noqa: BLE001 - board must never hang/raise on Striim
        return {}


def cmd_probe(args):
    from split_runner import load_query
    import probe
    from source_dialect import get_dialect

    query = load_query(args)
    dialect = get_dialect(args.source_engine)
    owner, table = dialect.parse_owner_table(args.table)
    rec = probe.run_probe(
        query,
        owner,
        table,
        alias=args.alias,
        depth=args.depth,
        dialect=dialect,
        target_slice_seconds=args.target_slice_seconds,
        sample_rows=args.sample_rows,
        time_budget_seconds=args.time_budget_seconds,
        max_concurrency=args.max_concurrency,
        parallel_sweep=args.parallel_sweep,
        parallel_degrees=args.parallel_degrees,
        parallel_runs=args.parallel_runs,
    )
    print(probe.format_recommendation(rec))
    import json as _json
    import os as _os

    rec_path = _os.path.join(
        _os.path.dirname(config.LOG_OUTPUT_PATH) or ".", "probe_recommendation.json"
    )
    try:
        with open(rec_path, "w") as _f:
            _json.dump(
                {
                    "strategy": rec.strategy,
                    "key": rec.key,
                    "chunk_count": rec.chunk_count,
                    "concurrency": rec.concurrency,
                    "parallel_degree": rec.parallel_degree,
                    "table": args.table,
                    "query_file": getattr(args, "query_file", None),
                    "depth": args.depth,
                },
                _f,
                indent=2,
            )
        print(f"\n[probe] recommendation written to {rec_path}")
    except OSError:
        pass
    return 0


def cmd_board(args):
    import json

    rows = _read_current(args.run_id)
    state = classify_run(rows)
    counts = status_counts(rows)
    namespaces = [r.namespace for r in rows if r.status == "RUNNING" and r.namespace]
    metrics = _safe_striim_metrics(namespaces)
    payload = _board_payload(
        args.run_id,
        state,
        counts,
        rows,
        metrics,
        getattr(config, "STAGE_DB_LOCATION", "?"),
        getattr(config, "SOURCE_DB_TYPE", "oracle"),
    )
    print(json.dumps(payload))
    return 0


# --- setup operator helpers (lazy imports; NOT run in tests) ---------------
def _check_connectivity(backend):
    """Return (ok, detail) for the configured backend. Never raises."""
    try:
        b = normalize_backend(backend)
        if b == "TinyDB":
            parent = os.path.dirname(config.TINYDB_PATH) or "."
            if not os.path.isdir(parent):
                return False, f"TinyDB parent dir not found: {parent}"
            if not os.access(parent, os.W_OK):
                return False, f"TinyDB parent dir not writable: {parent}"
            return True, f"TinyDB path OK ({config.TINYDB_PATH})"
        if b == "PG":
            import data_pg

            conn = data_pg.get_pg_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True, "PostgreSQL connection OK"
        if b == "BQ":
            import data_bq

            client = data_bq.get_bq_client()
            client.get_dataset(f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}")
            return True, "BigQuery dataset reachable"
        if b == "ORACLE":
            import data_oracle

            conn = data_oracle.get_oracle_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM dual")
            return True, "Oracle connection OK"
        return False, f"Unknown backend: {backend!r}"
    except Exception as e:
        return False, f"{backend} connectivity check failed: {e}"


def _run_ddl(backend):
    """Create/verify the orchestration table. Return (ok, msg). Never raises."""
    try:
        b = normalize_backend(backend)
        if b == "TinyDB":
            return True, "TinyDB needs no table — the file is created on first write."
        if b == "PG":
            import data_pg

            data_pg.get_pg_connection()  # auto-runs CREATE TABLE IF NOT EXISTS
            return True, "PostgreSQL orchestration table verified/created."
        if b == "ORACLE":
            import data_oracle

            conn = data_oracle.get_oracle_connection()
            data_oracle._ensure_table(conn)
            return True, "Oracle orchestration table verified/created."
        if b == "BQ":
            import data_bq

            sql_path = os.path.join(
                os.path.dirname(__file__) or ".", "BQ_TableCreate.sql"
            )
            with open(sql_path) as f:
                text = f.read()
            ddl = _bq_ddl_from_template(
                text, config.BQ_PROJECT_ID, config.BQ_DATASET_ID, config.BQ_TABLE_ID
            )
            fq = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"
            data_bq.get_bq_client().query(ddl).result()
            return True, f"BigQuery table verified/created: {fq}"
        return False, f"Unknown backend: {backend!r}"
    except Exception as e:
        return False, f"DDL failed: {e}"


def _verify_schema(backend):
    """Operator-verified: SELECT the live column set and diff vs _COLUMNS. Never raises."""
    try:
        if normalize_backend(backend) == "ORACLE":
            import data_oracle

            conn = data_oracle.get_oracle_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM user_tab_columns WHERE table_name = :t",
                    t=data_oracle._table_name().upper().split(".")[-1],
                )
                actual = [r[0] for r in cur.fetchall()]
            return schema_diff(data_oracle._COLUMNS, actual)
        return True, [], []
    except Exception as e:  # noqa: BLE001
        print(f"  (schema verify skipped: {e})")
        return True, [], []


def _interactive_setup(args):
    """Interactive credential collection + connect. I/O-heavy; not unit-tested.

    Resolves the state backend and source engine, shows a masked summary of the
    current values, offers use/reset/cancel, persists chosen values to a
    gitignored ``chmod 600`` ``.env``, then validates in a fresh child process
    (so the new ``.env`` loader picks the values up) and returns its exit code.
    """
    import getpass
    import subprocess
    import sys

    raw = getattr(config, "STAGE_DB_LOCATION", "TinyDB")
    backend = normalize_backend(args.backend or raw)
    print(f"State backend: {backend}  (STAGE_DB_LOCATION={raw!r})")

    valid_engines = ("oracle", "postgres", "sqlserver", "jdbc")
    default_engine = (
        os.environ.get("SOURCE_DB_TYPE", getattr(config, "SOURCE_DB_TYPE", "oracle"))
        .strip()
        .lower()
    )
    # A garbage SOURCE_DB_TYPE must not become the blank-Enter default, or the
    # re-prompt loop below would never exit on empty input. Fall back to oracle.
    if default_engine not in valid_engines:
        default_engine = "oracle"
    engine = (
        input(f"Source DB engine {'/'.join(valid_engines)} [{default_engine}]: ")
        .strip()
        .lower()
        or default_engine
    )
    while engine not in valid_engines:
        engine = (
            input(
                f"  Please choose one of {', '.join(valid_engines)} [{default_engine}]: "
            )
            .strip()
            .lower()
            or default_engine
        )

    fields = credential_spec(backend, engine)
    current = {f.env_var: os.environ.get(f.env_var, "") for f in fields}

    print("\nCurrent connection settings:")
    print(f"  - SOURCE_DB_TYPE  {engine}")
    for f in fields:
        print(f"  - {f.env_var}  {mask_value(current[f.env_var], f.secret)}")

    # Re-prompt until the answer is recognized rather than silently treating a typo
    # (e.g. "rest") as "use".
    while True:
        choice = (
            input("\nUse these / reset (re-enter) / cancel? [use]: ").strip().lower()
        )
        if choice in ("", "u", "use", "r", "reset", "c", "cancel"):
            break
        print("  Please enter 'use', 'reset', or 'cancel'.")
    if choice in ("c", "cancel"):
        print("Cancelled — no changes written.")
        return 1

    updates = {}
    if choice in ("r", "reset"):
        updates["SOURCE_DB_TYPE"] = engine
        print("\nEnter values (blank keeps the current/existing value):")
        for f in fields:
            cur = current[f.env_var]
            if f.secret:
                entered = getpass.getpass(f"  {f.label} [keep existing]: ")
            elif f.env_var.endswith("_PORT"):
                # Re-prompt until the port is a positive integer (blank keeps current).
                while True:
                    entered = input(f"  {f.label} [{cur}]: ").strip()
                    if _valid_port_input(entered):
                        break
                    print(
                        "  Port must be a positive whole number "
                        "(or blank to keep the current value)."
                    )
            else:
                entered = input(f"  {f.label} [{cur}]: ").strip()
            val = entered if entered else cur
            if val:
                updates[f.env_var] = val
    else:
        # 'use' (default): keep existing values, only record a changed engine.
        if engine != default_engine:
            updates["SOURCE_DB_TYPE"] = engine

    if updates:
        path = os.path.join(os.getcwd(), ".env")
        try:
            with open(path) as f:
                existing = f.read()
        except OSError:
            existing = ""
        with open(path, "w") as f:
            f.write(merge_env_lines(existing, updates))
        os.chmod(path, 0o600)

        gi_path = os.path.join(os.getcwd(), ".gitignore")
        try:
            with open(gi_path) as f:
                gi = f.read()
        except OSError:
            gi = ""
        new_gi = ensure_gitignore_has(gi, ".env")
        if new_gi != gi:
            with open(gi_path, "w") as f:
                f.write(new_gi)

        print(f"\nSaved to {path} (chmod 600):")
        print(f"  - SOURCE_DB_TYPE  {engine}")
        for f in fields:
            saved = updates.get(f.env_var, current[f.env_var])
            print(f"  - {f.env_var}  {mask_value(saved, f.secret)}")
    else:
        print("\nNo changes to save — using the existing environment values.")

    print("\nConnecting (fresh process, loads .env)…")
    result = subprocess.run(
        [
            sys.executable,
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "manage.py"),
            "setup",
        ]
        + (["--backend", args.backend] if args.backend else [])
    )
    return result.returncode


def cmd_setup(args):
    if getattr(args, "interactive", False):
        return _interactive_setup(args)
    raw = getattr(config, "STAGE_DB_LOCATION", "TinyDB")
    backend = normalize_backend(args.backend or raw)
    print(f"State backend: {backend}  (STAGE_DB_LOCATION={raw!r})")

    env_vars = required_env_vars(backend)
    if env_vars:
        for v in env_vars:
            state = "(set)" if os.environ.get(v) else "(unset)"
            print(f"  - {v}  {state}")
    else:
        print("  TinyDB needs no env vars.")

    print("Checking connectivity…")
    ok, detail = _check_connectivity(backend)
    if ok:
        print(f"  ✓ {detail}")
    else:
        print(f"  ✗ {detail}")

    if normalize_backend(backend) == "TinyDB":
        print("  No table needed — TinyDB creates its file on first write.")
    else:
        if normalize_backend(backend) in ("PG", "ORACLE"):
            print(
                f"  Note: {backend} auto-creates the orchestration table on first run."
            )
        if _confirm(f"Create/verify the {backend} orchestration table now?", args.yes):
            try:
                ok_ddl, msg = _run_ddl(backend)
                if ok_ddl:
                    print(f"  ✓ {msg}")
                else:
                    print(f"  ✗ {msg}")
            except Exception as e:
                print(f"  ✗ DDL failed: {e}")
            ok_schema, missing, extra = _verify_schema(backend)
            if not ok_schema:
                print(f"  ✗ schema mismatch — missing: {missing}  extra: {extra}")
            elif normalize_backend(backend) == "ORACLE":
                print("  ✓ schema columns verified against _COLUMNS.")

    if normalize_backend(backend) in ("PG", "ORACLE", "BQ"):
        print(
            "  Reminder: a HEAP target with no PK/unique key silently DUPLICATES rows on any "
            "CDC overlap / reset / re-run. Ensure the Striim target has a unique/PK so "
            "DUPLICATE_ROW_EXISTS is ignorable (see admin.SW.tql + README 'Exactly-once')."
        )

    if env_vars:
        print(
            "\nCopy-paste env template (these exports are SESSION-ONLY — persist them in your shell profile / .env):"
        )
        print(env_block(backend))

    return 0 if ok else 1


# --- arg parsing -----------------------------------------------------------
def build_parser():
    p = argparse.ArgumentParser(
        prog="manage.py", description="ParallelLoader operator CLI"
    )
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("status", help="Show run progress")
    s.add_argument("--run-id", type=int, default=config.UNIQUE_RUN_ID)
    s.add_argument("--all-runs", action="store_true")
    s.add_argument("--failed", action="store_true")
    s.add_argument("--json", action="store_true")
    s.add_argument(
        "--rows",
        action="store_true",
        help="With --json, include a rows[] array for the dashboard renderer",
    )
    s.set_defaults(func=cmd_status)

    c = sub.add_parser("clear", help="Retire (default) or delete (--hard) a run")
    c.add_argument("--run-id", type=int, default=config.UNIQUE_RUN_ID)
    c.add_argument("--hard", action="store_true")
    c.add_argument("--with-striim-cleanup", action="store_true")
    c.add_argument("--yes", action="store_true")
    c.set_defaults(func=cmd_clear)

    r = sub.add_parser("reset", help="Re-queue FAILED rows to NEW")
    r.add_argument("--run-id", type=int, default=config.UNIQUE_RUN_ID)
    r.add_argument("--include-faileddrop", action="store_true")
    r.add_argument("--with-striim-cleanup", action="store_true")
    r.add_argument("--yes", action="store_true")
    r.set_defaults(func=cmd_reset)

    lg = sub.add_parser("logs", help="Show loader log")
    lg.add_argument("--lines", type=positive_int, default=50)
    lg.add_argument("--follow", action="store_true")
    lg.add_argument("--errors", action="store_true")
    lg.set_defaults(func=cmd_logs)

    sp = sub.add_parser("split", help="Generate queryfile.txt by ROWID or partition")
    sp.add_argument("--query-file")
    sp.add_argument("--query")
    sp.add_argument("--table", required=True, help="OWNER.TABLE driving table")
    sp.add_argument("--target", required=True, help="OWNER.TARGET table")
    sp.add_argument("--chunks", type=positive_int, default=16)
    sp.add_argument(
        "--strategy", choices=("auto", "rowid", "partition", "column"), default="auto"
    )
    sp.add_argument("--alias")
    sp.add_argument(
        "--column",
        help="Split column for --strategy column (from the probe recommendation)",
    )
    sp.add_argument("--subpartitions", action="store_true")
    sp.add_argument(
        "--parallel",
        type=positive_int,
        default=1,
        help="Inject an Oracle PARALLEL(n) hint into every generated SELECT "
        "(1 = off; from the probe's parallel_degree recommendation). Oracle only.",
    )
    sp.add_argument("--output", default=config.QUERY_FILE)
    sp.add_argument("--assort", action="store_true")
    sp.add_argument("--explain", action="store_true")
    sp.add_argument(
        "--source-engine",
        choices=("oracle", "postgres", "sqlserver", "jdbc"),
        default=None,
        help="Source DB engine for probe/split (default: config.SOURCE_DB_TYPE)",
    )
    sp.set_defaults(func=cmd_split)

    sb = sub.add_parser(
        "split-batch",
        help="Fan a pasted batch of queries into one queryfile "
        "(splittable ones probed+split, plain ones appended)",
    )
    sb.add_argument(
        "--manifest",
        required=True,
        help="JSON array of entries {line, needs_split, table, alias, ...}",
    )
    sb.add_argument("--output", default=config.QUERY_FILE)
    sb.add_argument(
        "--chunks", type=positive_int, default=16, help="Default chunks (no-probe)"
    )
    sb.add_argument(
        "--strategy",
        choices=("auto", "rowid", "partition", "column"),
        default="auto",
        help="Default strategy for splittable entries when --no-probe",
    )
    sb.add_argument(
        "--depth",
        choices=("lightweight", "bakeoff", "adaptive"),
        default=config.PROBE_DEPTH_DEFAULT,
        help="Probe depth applied to each splittable entry",
    )
    probe_grp = sb.add_mutually_exclusive_group()
    probe_grp.add_argument(
        "--probe",
        dest="probe",
        action="store_true",
        default=True,
        help="Probe each splittable entry to auto-pick strategy/chunks (default)",
    )
    probe_grp.add_argument(
        "--no-probe",
        dest="probe",
        action="store_false",
        help="Skip probing; use --strategy/--chunks for all splittable entries",
    )
    sb.add_argument("--assort", action="store_true")
    sb.add_argument("--explain", action="store_true")
    sb.add_argument(
        "--source-engine",
        choices=("oracle", "postgres", "sqlserver", "jdbc"),
        default=None,
        help="Source DB engine for probe/split (default: config.SOURCE_DB_TYPE)",
    )
    sb.set_defaults(func=cmd_split_batch)

    pr = sub.add_parser("probe", help="Run bake-off and print split recommendation")
    pr.add_argument("--query-file")
    pr.add_argument("--query")
    pr.add_argument("--table", required=True, help="OWNER.TABLE to probe")
    pr.add_argument(
        "--depth",
        choices=("lightweight", "bakeoff", "adaptive"),
        default=config.PROBE_DEPTH_DEFAULT,
    )
    pr.add_argument("--alias")
    pr.add_argument(
        "--target-slice-seconds",
        type=positive_int,
        default=config.PROBE_TARGET_SLICE_SECONDS,
    )
    pr.add_argument("--sample-rows", type=positive_int, default=config.PROBE_SAMPLE_ROWS)
    pr.add_argument(
        "--time-budget-seconds",
        type=positive_int,
        default=config.PROBE_TIME_BUDGET_SECONDS,
    )
    pr.add_argument(
        "--max-concurrency", type=positive_int, default=config.PROBE_MAX_CONCURRENCY
    )
    pr.add_argument(
        "--parallel-sweep",
        action="store_true",
        help="Also sweep Oracle PARALLEL degrees on the winner and recommend a degree "
        "(Oracle only; ignored for other engines)",
    )
    pr.add_argument(
        "--parallel-degrees",
        type=int_csv,
        default=config.PROBE_PARALLEL_DEGREES,
        help="Comma-separated PARALLEL degrees to sweep (default from config)",
    )
    pr.add_argument(
        "--parallel-runs",
        type=positive_int,
        default=config.PROBE_PARALLEL_RUNS,
        help="Timings averaged per degree (>=2 discards a warm-up run)",
    )
    pr.add_argument(
        "--source-engine",
        choices=("oracle", "postgres", "sqlserver", "jdbc"),
        default=None,
        help="Source DB engine for probe/split (default: config.SOURCE_DB_TYPE)",
    )
    pr.set_defaults(func=cmd_probe)

    bd = sub.add_parser("board", help="Emit live-board JSON data feed")
    bd.add_argument("--run-id", type=int, default=config.UNIQUE_RUN_ID)
    bd.add_argument("--json", action="store_true", help="Accepted for consistency")
    bd.set_defaults(func=cmd_board)

    st = sub.add_parser("setup", help="Validate the state backend and create its table")
    st.add_argument(
        "--backend", help="Override STAGE_DB_LOCATION (TinyDB/PG/BQ/ORACLE)"
    )
    st.add_argument("--yes", action="store_true", help="Run the DDL without confirming")
    st.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Interactively collect credentials, save to .env, then test",
    )
    st.set_defaults(func=cmd_setup)

    rc = sub.add_parser(
        "reconcile", help="SCN-anchored source row-count reconcile (completeness gate)"
    )
    rc.add_argument("--run-id", type=int, default=config.UNIQUE_RUN_ID)
    rc.set_defaults(func=cmd_reconcile)

    return p


def _is_missing_credentials(exc):
    """True when ``exc`` is an engine 'Missing <Engine> settings: …' error.

    Every source/state engine raises its missing-credentials error with this
    uniform message (oracle_client, jdbc_dialect, sqlserver_dialect, …), so we
    classify on the message rather than importing every engine's exception
    class (which would defeat the lazy imports and couple to reused types).
    """
    msg = str(exc)
    return "Missing" in msg and "settings" in msg


def _is_bad_identifier(exc):
    """True when ``exc`` reports a malformed identifier from user input — a bad
    ``--table`` / ``--alias`` / ``--column`` or empty/piped ``--target``.

    Classified on message (like ``_is_missing_credentials``) so we don't import each
    engine's exception class and defeat the lazy imports. Matched case-insensitively
    because engines vary: oracle_boundaries raises 'Invalid Oracle identifier' /
    '--table must be OWNER.TABLE' while the generic source_dialect path raises lowercase
    'invalid owner identifier' — both must be caught, or a bad value tracebacks.
    """
    msg = str(exc).lower()
    return (
        "--table must be" in msg
        or ("invalid" in msg and "identifier" in msg)
        or "target must" in msg
    )


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except Exception as e:  # noqa: BLE001 - only known user-input errors are handled here
        if _is_missing_credentials(e):
            print(f"\n✗ {e}", file=sys.stderr)
            print(
                "  The command needs database credentials that are not set.",
                file=sys.stderr,
            )
            print(
                "  Enter them with:  python manage.py setup --interactive"
                "   (or option 12 in ./manage.sh)",
                file=sys.stderr,
            )
            return 3
        if _is_bad_identifier(e):
            print(f"\n✗ {e}", file=sys.stderr)
            if "table" in str(e).lower():
                print(
                    "  Pass the driving table as OWNER.TABLE "
                    "(e.g. PAY.CM_CASES, not CM_CASES).",
                    file=sys.stderr,
                )
            else:
                print(
                    "  Check the value and re-enter it — identifiers may contain only "
                    "letters, digits, and _ $ #.",
                    file=sys.stderr,
                )
            return 1
        raise


if __name__ == "__main__":
    sys.exit(main())
