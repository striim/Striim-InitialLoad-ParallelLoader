from models import QueryResult
import manage


def _qr(_id, status):
    return QueryResult(
        roworder=_id,
        query="q",
        targettbl="t",
        _id=_id,
        uniquerunid=100,
        status=status,
        iscurrentrow=True,
        appname="ILA_100_1.App",
        namespace="ILA_100_1",
        notes="boom",
    )


def test_classify_not_started():
    assert manage.classify_run([]) == "NOT STARTED"


def test_classify_in_progress():
    assert (
        manage.classify_run([_qr(1, "RUNNING"), _qr(2, "COMPLETED")]) == "IN PROGRESS"
    )


def test_classify_finished():
    assert manage.classify_run([_qr(1, "COMPLETED"), _qr(2, "FAILED")]) == "FINISHED"


def test_status_counts():
    rows = [_qr(1, "FAILED"), _qr(2, "FAILED"), _qr(3, "COMPLETED")]
    assert manage.status_counts(rows) == {"FAILED": 2, "COMPLETED": 1}


def test_build_reset_rows_resets_failed_only():
    rows = [_qr(1, "FAILED"), _qr(2, "COMPLETED"), _qr(3, "COMPLETED-FAILEDDROP")]
    out = manage.build_reset_rows(rows)
    assert [r.id for r in out] == [1]
    r = out[0]
    assert r.status == "NEW" and r.appname == "" and r.namespace == "" and r.notes == ""
    assert r.started_datetime is None and r.finished_datetime is None


def test_build_reset_rows_include_faileddrop():
    rows = [_qr(1, "FAILED"), _qr(3, "COMPLETED-FAILEDDROP")]
    out = manage.build_reset_rows(rows, include_faileddrop=True)
    assert sorted(r.id for r in out) == [1, 3]


def test_rows_payload_shape():
    out = manage.rows_payload([_qr(1, "FAILED"), _qr(2, "RUNNING")])
    assert out == [
        {
            "roworder": 1,
            "status": "FAILED",
            "targettbl": "t",
            "appname": "ILA_100_1.App",
            "notes": "boom",
        },
        {
            "roworder": 2,
            "status": "RUNNING",
            "targettbl": "t",
            "appname": "ILA_100_1.App",
            "notes": "boom",
        },
    ]
