"""Oracle orchestration/state backend — mirrors data_pg.py.

oracledb is imported lazily (inside functions that need it) so this module
is importable and all pure helpers are testable without the driver installed.
"""

import re
from typing import List

import config
from models import QueryResult

_ora_conn = None

_COLUMNS = [
    "id",
    "roworder",
    "uniquerunid",
    "query",
    "appname",
    "targettbl",
    "status",
    "namespace",
    "started_datetime",
    "finished_datetime",
    "notes",
    "iscurrentrow",
]
_NON_ID_COLUMNS = [c for c in _COLUMNS if c != "id"]


class OracleOrchConfigError(Exception):
    pass


def _dsn():
    if getattr(config, "ORCH_ORACLE_DSN", ""):
        return config.ORCH_ORACLE_DSN
    host = getattr(config, "ORCH_ORACLE_HOST", "")
    service = getattr(config, "ORCH_ORACLE_SERVICE", "")
    port = getattr(config, "ORCH_ORACLE_PORT", 1521)
    if host and service:
        return f"{host}:{port}/{service}"
    return ""


_TABLE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*(\.[A-Za-z][A-Za-z0-9_$#]*)?$")


def _table_name():
    name = getattr(config, "ORCH_ORACLE_TABLE_ID", "striim_orchestration")
    if not _TABLE_RE.match(name):
        raise OracleOrchConfigError(f"Invalid ORCH_ORACLE_TABLE_ID value: {name!r}")
    return name


def _create_table_sql(table):
    return f"""CREATE TABLE {table} (
    id                NUMBER PRIMARY KEY,
    roworder          NUMBER,
    uniquerunid       NUMBER,
    query             CLOB,
    appname           VARCHAR2(4000),
    targettbl         VARCHAR2(4000),
    status            VARCHAR2(64),
    namespace         VARCHAR2(256),
    started_datetime  TIMESTAMP,
    finished_datetime TIMESTAMP,
    notes             VARCHAR2(4000),
    iscurrentrow      NUMBER(1)
)"""


def _merge_sql(table):
    select_parts = ", ".join(f":{c} AS {c}" for c in _COLUMNS)
    update_parts = ", ".join(f"T.{c} = S.{c}" for c in _NON_ID_COLUMNS)
    insert_cols = ", ".join(_COLUMNS)
    insert_vals = ", ".join(f"S.{c}" for c in _COLUMNS)
    return (
        f"MERGE INTO {table} T "
        f"USING (SELECT {select_parts} FROM dual) S "
        f"ON (T.id = S.id) "
        f"WHEN MATCHED THEN UPDATE SET {update_parts} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def _is_already_exists(exc):
    return "ORA-00955" in str(exc)


def _to_bool(v):
    if v is None:
        return False
    return bool(v)


def _lob_to_str(v):
    if v is None:
        return None
    if callable(getattr(v, "read", None)):
        return v.read()
    return v


def _build_where(where_clause):
    conditions = []
    binds = {}

    is_current_match = re.search(
        r"iscurrentrow\s*=\s*(True|False)", where_clause, re.IGNORECASE
    )
    if is_current_match:
        val = is_current_match.group(1).lower() == "true"
        conditions.append("iscurrentrow = :ic")
        binds["ic"] = 1 if val else 0

    unique_run_id_match = re.search(r"uniquerunid\s*=\s*(\d+)", where_clause)
    if unique_run_id_match:
        val = int(unique_run_id_match.group(1))
        conditions.append("uniquerunid = :run")
        binds["run"] = val

    if conditions:
        return (" AND ".join(conditions), binds)
    elif where_clause.strip() == "":
        return ("", {})
    else:
        return (None, {})


def _bind_row(r):
    return {
        "id": r.id,
        "roworder": r.roworder,
        "uniquerunid": r.uniquerunid,
        "query": r.query,
        "appname": r.appname or "",
        "targettbl": r.targettbl,
        "status": r.status or "",
        "namespace": r.namespace or "",
        "started_datetime": r.started_datetime,
        "finished_datetime": r.finished_datetime,
        "notes": r.notes or "",
        "iscurrentrow": 1 if r.iscurrentrow else 0,
    }


def _row_to_query_result(row_dict):
    return QueryResult(
        roworder=row_dict.get("roworder"),
        _id=row_dict.get("id"),
        uniquerunid=row_dict.get("uniquerunid"),
        query=_lob_to_str(row_dict.get("query")),
        appname=row_dict.get("appname"),
        targettbl=row_dict.get("targettbl"),
        status=row_dict.get("status"),
        namespace=row_dict.get("namespace"),
        started_datetime=row_dict.get("started_datetime"),
        finished_datetime=row_dict.get("finished_datetime"),
        notes=row_dict.get("notes"),
        iscurrentrow=_to_bool(row_dict.get("iscurrentrow", False)),
    )


_DISCONNECT_ORA_CODES = frozenset({28, 2396, 3113, 3114})


def is_disconnect_error(exc):
    """Pure: is this a lost/closed-connection error worth one reconnect?

    Matches oracledb Interface/Operational classes when available, the known
    disconnect ORA codes (00028, 02396, 03113, 03114), and DPY- driver codes,
    by both exc.args[0].code and the string form. No oracledb import required.
    """
    import sys

    oracledb = sys.modules.get("oracledb")  # never triggers a fresh import
    if oracledb is not None:
        if isinstance(exc, (oracledb.InterfaceError, oracledb.OperationalError)):
            return True
    s = str(exc)
    if any(f"ORA-{c:05d}" in s for c in _DISCONNECT_ORA_CODES):
        return True
    if "DPY-" in s:
        return True
    args = getattr(exc, "args", None)
    if args:
        return getattr(args[0], "code", None) in _DISCONNECT_ORA_CODES
    return False


def _with_reconnect(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            if is_disconnect_error(exc):
                global _ora_conn
                _ora_conn = None
                return func(*args, **kwargs)
            raise

    return wrapper


def get_oracle_connection():
    global _ora_conn
    if _ora_conn is not None:
        try:
            _ora_conn.ping()  # operator-verified: dead cached conn -> reconnect
        except Exception:
            _ora_conn = None
    if _ora_conn is None:
        user = getattr(config, "ORCH_ORACLE_USER", "")
        pwd = getattr(config, "ORCH_ORACLE_PASSWORD", "")
        dsn = _dsn()
        missing = [
            n
            for n, v in (
                ("ORCH_ORACLE_USER", user),
                ("ORCH_ORACLE_PASSWORD", pwd),
                ("ORCH_ORACLE_DSN (or ORCH_ORACLE_HOST+ORCH_ORACLE_SERVICE)", dsn),
            )
            if not v
        ]
        if missing:
            raise OracleOrchConfigError(
                "Missing Oracle orchestration settings: " + ", ".join(missing)
            )
        try:
            import oracledb
        except ImportError as e:
            raise OracleOrchConfigError(
                "python-oracledb not installed; run: pip install python-oracledb"
            ) from e
        _ora_conn = oracledb.connect(user=user, password=pwd, dsn=dsn)
        _ora_conn.autocommit = True
        _ensure_table(_ora_conn)
    return _ora_conn


def _ensure_table(conn):
    try:
        with conn.cursor() as cur:
            cur.execute(_create_table_sql(_table_name()))
    except Exception as exc:
        if not _is_already_exists(exc):
            raise


@_with_reconnect
def write_to_oracle(query_results):
    if not query_results:
        return
    conn = get_oracle_connection()  # validates creds + driver (friendly error)
    import oracledb

    binds = [_bind_row(r) for r in query_results]
    with conn.cursor() as cur:
        # Long slice queries bind as CLOB. Timestamps deliberately get no
        # setinputsizes: the only write_data callers (fresh run, reset) pass
        # homogeneous all-None timestamp batches, so executemany infers a
        # single column type cleanly. A mixed None/datetime batch would need
        # setinputsizes(... DB_TYPE_TIMESTAMP) added here.
        cur.setinputsizes(query=oracledb.DB_TYPE_CLOB)
        cur.executemany(_merge_sql(_table_name()), binds)
    print("Rows have been merged successfully.")


@_with_reconnect
def read_from_oracle(where_clause: str) -> List[QueryResult]:
    where_sql, binds = _build_where(where_clause)
    if where_sql is None:
        return []
    conn = get_oracle_connection()
    sql = "SELECT " + ", ".join(_COLUMNS) + " FROM " + _table_name()
    if where_sql:
        sql += " WHERE " + where_sql
    with conn.cursor() as cur:
        cur.execute(sql, binds)
        cols = [d[0].lower() for d in cur.description]
        return [_row_to_query_result(dict(zip(cols, row))) for row in cur.fetchall()]


@_with_reconnect
def fetch_record_from_oracle(record_id):
    conn = get_oracle_connection()
    sql = "SELECT " + ", ".join(_COLUMNS) + " FROM " + _table_name() + " WHERE id = :id"
    with conn.cursor() as cur:
        cur.execute(sql, {"id": record_id})
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0].lower() for d in cur.description]
        return _row_to_query_result(dict(zip(cols, row)))


@_with_reconnect
def get_next_id_oracle():
    conn = get_oracle_connection()
    sql = "SELECT COALESCE(MAX(id), 0) + 1 FROM " + _table_name()
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()[0])


@_with_reconnect
def update_record_in_oracle(query_result, return_output=False):
    if query_result.id is None:
        print("Problem, should not have empty id")
        raise NotImplementedError
    conn = get_oracle_connection()
    fields = []
    values = {}
    for attr, value in query_result.__dict__.items():
        if attr != "id" and value is not None:
            if attr == "iscurrentrow":
                value = 1 if value else 0
            fields.append(f"{attr} = :{attr}")
            values[attr] = value
    values["id"] = query_result.id
    sql = "UPDATE " + _table_name() + " SET " + ", ".join(fields) + " WHERE id = :id"
    import oracledb

    with conn.cursor() as cur:
        if "query" in values:
            cur.setinputsizes(query=oracledb.DB_TYPE_CLOB)
        cur.execute(sql, values)
    print(f"Record with ID {query_result.id} has been updated.")
    if return_output:
        return fetch_record_from_oracle(query_result.id)


@_with_reconnect
def clear_runid_oracle(uniquerunid):
    if uniquerunid is None:
        print("Problem, should not have empty uniquerunid")
        raise NotImplementedError
    conn = get_oracle_connection()
    sql = (
        "UPDATE "
        + _table_name()
        + " SET iscurrentrow = 0 WHERE iscurrentrow = 1 AND uniquerunid = :run"
    )
    with conn.cursor() as cur:
        cur.execute(sql, {"run": uniquerunid})
    print(
        f"Records with uniquerunid {uniquerunid} have been updated as iscurrentrow = FALSE"
    )


@_with_reconnect
def delete_runid_oracle(uniquerunid):
    if uniquerunid is None:
        print("Problem, should not have empty uniquerunid")
        raise NotImplementedError
    conn = get_oracle_connection()
    sql = (
        "DELETE FROM "
        + _table_name()
        + " WHERE iscurrentrow = 1 AND uniquerunid = :run"
    )
    with conn.cursor() as cur:
        cur.execute(sql, {"run": uniquerunid})
    print(f"Deleted current rows for uniquerunid {uniquerunid}")
