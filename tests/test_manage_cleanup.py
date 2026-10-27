from models import QueryResult
import manage
import main


def _qr(status, namespace):
    return QueryResult(
        roworder=1, query="q", targettbl="t", status=status, namespace=namespace
    )


def test_ns_base_for_run():
    assert manage.ns_base_for_run(100) == "ILA_100_"
    assert manage.ns_base_for_run(7) == "ILA_7_"


def test_apps_to_clean_prefix_filter():
    apps = ["ILA_100_1.App", "ILA_100_2.App", "ILA_200_1.App", "OTHER.App"]
    assert manage.apps_to_clean(apps, "ILA_100_") == ["ILA_100_1.App", "ILA_100_2.App"]


def test_apps_to_clean_only_filter():
    apps = ["ILA_100_1.App", "ILA_100_2.App", "ILA_100_3.App"]
    assert manage.apps_to_clean(apps, "ILA_100_", only={"ILA_100_2"}) == [
        "ILA_100_2.App"
    ]


def test_apps_to_clean_protect_filter():
    apps = ["ILA_100_1.App", "ILA_100_2.App"]
    assert manage.apps_to_clean(apps, "ILA_100_", protect={"ILA_100_1"}) == [
        "ILA_100_2.App"
    ]


def test_failed_namespaces_only_failed_with_ns():
    rows = [
        _qr("FAILED", "ILA_100_1"),
        _qr("COMPLETED", "ILA_100_2"),
        _qr("FAILED", ""),
    ]
    assert manage.failed_namespaces(rows) == {"ILA_100_1"}


def test_isilapp_default_base():
    assert main.isILApp("ILA_100_1.OracleInitialLoadApp") is True
    assert main.isILApp("OTHER.App") is False


def test_isilapp_explicit_base():
    assert main.isILApp("ILA_200_3.App", ns_base="ILA_200_") is True
    assert main.isILApp("ILA_100_1.App", ns_base="ILA_200_") is False


def test_failed_namespaces_snapshot_survives_reset_mutation():
    rows = [_qr("FAILED", "ILA_100_1"), _qr("COMPLETED", "ILA_100_2")]
    snap = manage.failed_namespaces(rows)
    assert snap == {"ILA_100_1"}
    manage.build_reset_rows(rows)  # mutates: blanks the FAILED row's namespace
    assert rows[0].namespace == ""  # mutation happened
    assert snap == {"ILA_100_1"}  # snapshot taken first is unaffected
