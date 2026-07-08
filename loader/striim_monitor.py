"""Best-effort live Striim app metrics for the status board (§10.1).

Reuses main.py's authenticate() + monitoring calls, imported LAZILY so this module stays
import-safe without the loader's heavy deps (tinydb, google-cloud-bigquery, etc.).
Every public call is defensive: on missing creds, an unreachable node, or any error it
returns empty so the board falls back to state-only ('—'). It never raises.
"""

import logging

logger = logging.getLogger("parallelloader.striim_monitor")


def available():
    """Cheap, no-network check: are Striim creds configured?

    Returns True only when STRIIM_NODE is non-empty AND at least one of
    STRIIM_API_TOKEN or STRIIM_ADMIN_PWD is set.
    """
    import config  # lazy — config has no heavy deps, but keeps the pattern consistent

    node = getattr(config, "STRIIM_NODE", "")
    has_auth = bool(
        getattr(config, "STRIIM_API_TOKEN", "")
        or getattr(config, "STRIIM_ADMIN_PWD", "")
    )
    return bool(node) and has_auth


def app_metrics(namespaces, app_name=None, timeout=5):
    """Best-effort {namespace: {"rows": int|None, "rate": float|None, ...}} for running apps.

    namespaces  – iterable of ILA_<run>_<n> namespace strings (the in-flight slices).
    app_name    – optional; if given, restrict to apps whose full_name ends with
                  '.<app_name>' (e.g. 'OracleInitialLoadApp').
    timeout     – advisory seconds for the caller's wrapping timeout; note that
                  main.runCommand() uses a hardcoded 180 s HTTP timeout internally —
                  the board should wrap this call in a threading.Timer or
                  concurrent.futures timeout to bound wall-clock impact.

    Returns {} on ANY problem (no creds, ImportError, Striim down, parse error).
    Never raises.
    """
    result: dict = {}

    if not available():
        return result

    try:
        import main as _main  # lazy — avoids tinydb/BQ imports at module load time

        # authenticate() raises SystemExit (not Exception) on auth failure, so we
        # must catch BaseException or the specific type here.
        try:
            _headers = _main.authenticate()
        except (Exception, SystemExit) as exc:
            logger.debug("striim_monitor: authenticate() failed: %s", exc)
            return {}

        # Inject the fresh token into main's module-level 'headers' global so that
        # runCommand() (called by runMon()) picks it up.
        _main.headers = _headers

        # runMon() → runCommand('mon;', returnResultOnly=True) → parsed JSON list.
        # runCommand() has a hardcoded 180 s timeout; we cannot pass 'timeout' into it.
        # operator: verify metric keys against your Striim version
        json_response = _main.runMon()
        if not json_response:
            return {}

        striim_apps, _nodes, _es, response_valid = _main.map_mon_json_response(
            json_response
        )
        if not response_valid:
            return {}

        ns_set = set(namespaces)

        for app in striim_apps:
            ns = app.namespace  # e.g. 'ILA_100_1' — set in StriimApplication.__init__
            if ns not in ns_set:
                continue
            # Optional app-name filter (e.g. only 'OracleInitialLoadApp' apps)
            if app_name and not app.full_name.endswith("." + app_name):
                continue

            # operator: verify that app.rate / app.source_rate map to the metric you
            # expect — they come from mon JSON fields "rate" and "sourceRate".
            # The Striim mon output does NOT expose a cumulative row count (only rates),
            # so "rows" is left None here. If your Striim version exposes a cumulative
            # field (e.g. "rowsWritten"), extract it from the raw JSON via
            # main.map_mon_json_response and add it to StriimApplication.
            result[ns] = {
                "rows": None,
                "rate": float(app.rate) if app.rate is not None else None,
                "source_rate": (
                    float(app.source_rate) if app.source_rate is not None else None
                ),
                "status": app.status_change,
            }

    except (
        Exception,
        SystemExit,
    ) as exc:  # noqa: BLE001 — board must survive dead Striim
        logger.debug("striim app_metrics unavailable: %s", exc)
        return {}

    return result
