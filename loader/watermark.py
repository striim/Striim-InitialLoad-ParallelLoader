"""Capture-and-log helper for the source CDC watermark (Oracle SCN / Postgres LSN / SQL
Server LSN). Engine-agnostic: a SourceDialect supplies {"label", "value"}; this formats a
prominent banner, logs it, and writes a sidecar JSON so the operator can set the downstream
CDC reader's start point. No DB drivers are imported here.
"""

import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger("parallelloader.watermark")

_SEP = "=" * 64


def format_banner(label, value, run_id, captured_at):
    return "\n".join(
        [
            _SEP,
            f" INITIAL LOAD START WATERMARK [{label}]: {value}",
            f" captured {captured_at}  (run {run_id})",
            " -> Set the downstream CDC reader's start point to this value.",
            _SEP,
        ]
    )


def sidecar_path(log_path, run_id):
    d = os.path.dirname(log_path) or "."
    return os.path.join(d, f"run_{run_id}_watermark.json")


def capture_and_log(watermark, run_id, source_dsn, log_path, captured_at=None):
    """Log the watermark banner + write a sidecar JSON. Returns the sidecar path, or None.

    `watermark` is {"label":..., "value":...} from dialect.capture_watermark(), or None /
    empty-value when capture failed (e.g. missing privilege) — in which case we log a clear
    warning and return None without raising, so the load still proceeds. `captured_at` is
    injectable for testing; defaults to now(UTC) ISO seconds.
    """
    if not watermark or watermark.get("value") in (None, ""):
        logger.warning(
            "Source CDC watermark could not be captured for run %s — set the CDC start "
            "point manually.",
            run_id,
        )
        return None
    captured_at = captured_at or datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )
    label = watermark.get("label", "watermark")
    value = watermark.get("value")
    for line in format_banner(label, value, run_id, captured_at).splitlines():
        logger.info(line)
    path = sidecar_path(log_path, run_id)
    payload = {
        "run_id": run_id,
        "label": label,
        "value": str(value),
        "captured_at": captured_at,
        "source_dsn": source_dsn,
    }
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
    except OSError as e:
        logger.warning(
            "Watermark captured (%s=%s) but sidecar write failed: %s", label, value, e
        )
        return None
    logger.info("Watermark sidecar written: %s", path)
    return path


def choose_watermark(split_sidecar, captured_now, queryfile_name):
    """Reuse the split-time SCN as the run watermark when the split sidecar's
    queryfile matches the current run's queryfile; else fall back to captured_now.
    Returns (watermark_dict_or_None, used_split_scn)."""
    if (
        split_sidecar
        and os.path.basename(split_sidecar.get("queryfile") or "") == os.path.basename(queryfile_name or "")
        and split_sidecar.get("value") not in (None, "")
    ):
        return (
            {
                "label": split_sidecar.get("label", "Oracle SCN"),
                "value": split_sidecar.get("value"),
            },
            True,
        )
    return (captured_now, False)
