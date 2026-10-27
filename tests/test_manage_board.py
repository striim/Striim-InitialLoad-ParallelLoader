"""Pure tests for manage.py board helpers and new subcommand parsers."""

import types

import config
import manage

# ---------------------------------------------------------------------------
# Parser smoke tests
# ---------------------------------------------------------------------------


def test_probe_parser_defaults():
    args = manage.build_parser().parse_args(
        ["probe", "--table", "A.B", "--query", "SELECT 1"]
    )
    assert args.func is manage.cmd_probe
    assert args.depth == config.PROBE_DEPTH_DEFAULT
    assert args.sample_rows == config.PROBE_SAMPLE_ROWS


def test_board_parser_run_id():
    args = manage.build_parser().parse_args(["board", "--run-id", "5"])
    assert args.func is manage.cmd_board
    assert args.run_id == 5


# ---------------------------------------------------------------------------
# _board_payload — pure, no DB
# ---------------------------------------------------------------------------


def _row(roworder, status, namespace=None, started=None, finished=None):
    return types.SimpleNamespace(
        roworder=roworder,
        status=status,
        targettbl=f"TGT_{roworder}",
        appname=f"APP_{roworder}",
        namespace=namespace,
        started_datetime=started,
        finished_datetime=finished,
    )


def test_board_payload_shape_and_counts():
    rows = [
        _row(1, "RUNNING", namespace="ILA_100_1", started="2026-01-01 00:00:00"),
        _row(2, "RUNNING", namespace="ILA_100_2", started="2026-01-01 00:01:00"),
        _row(3, "COMPLETED", finished="2026-01-01 01:00:00"),
    ]
    counts = {"RUNNING": 2, "COMPLETED": 1}
    metrics = {"ILA_100_1": {"rate": 1234.0, "rows": None}}

    payload = manage._board_payload(
        run_id=100,
        state="IN PROGRESS",
        counts=counts,
        rows=rows,
        metrics=metrics,
        backend="TinyDB",
        source_engine="oracle",
    )

    assert payload["total"] == 3
    assert payload["done"] == 1
    assert payload["pct_complete"] == round(1 / 3, 4)
    assert len(payload["inflight"]) == 2
    assert payload["striim"] is True

    # The namespace that matches gets the rate
    matched = next(i for i in payload["inflight"] if i["namespace"] == "ILA_100_1")
    assert matched["rate"] == 1234.0

    # The namespace without metrics gets None
    unmatched = next(i for i in payload["inflight"] if i["namespace"] == "ILA_100_2")
    assert unmatched["rate"] is None


# ---------------------------------------------------------------------------
# _safe_striim_metrics — empty namespaces → {}
# ---------------------------------------------------------------------------


def test_safe_striim_metrics_empty_namespaces():
    assert manage._safe_striim_metrics([]) == {}
