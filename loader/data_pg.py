from typing import List

import psycopg2
import psycopg2.extras
from psycopg2 import sql

import config
from models import QueryResult

_pg_conn = None

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    id                INTEGER PRIMARY KEY,
    roworder          INTEGER,
    uniquerunid       INTEGER,
    query             TEXT,
    appname           TEXT,
    targettbl         TEXT,
    status            TEXT,
    namespace         TEXT,
    started_datetime  TIMESTAMP,
    finished_datetime TIMESTAMP,
    notes             TEXT,
    iscurrentrow      BOOLEAN
)
"""


def get_pg_connection():
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed != 0:
        _pg_conn = psycopg2.connect(
            host=config.PG_HOST,
            port=config.PG_PORT,
            dbname=config.PG_DATABASE,
            user=config.PG_USER,
            password=config.PG_PASSWORD,
            sslmode=config.PG_SSLMODE,
        )
        _pg_conn.autocommit = True
        with _pg_conn.cursor() as cur:
            cur.execute(
                sql.SQL(_CREATE_TABLE).format(table=sql.Identifier(config.PG_TABLE_ID))
            )
    return _pg_conn


def _with_reconnect(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            global _pg_conn
            _pg_conn = None
            return func(*args, **kwargs)

    return wrapper


def _row_to_query_result(row_dict):
    return QueryResult(
        roworder=row_dict.get("roworder"),
        _id=row_dict.get("id"),
        uniquerunid=row_dict.get("uniquerunid"),
        query=row_dict.get("query"),
        appname=row_dict.get("appname"),
        targettbl=row_dict.get("targettbl"),
        status=row_dict.get("status"),
        namespace=row_dict.get("namespace"),
        started_datetime=row_dict.get("started_datetime"),
        finished_datetime=row_dict.get("finished_datetime"),
        notes=row_dict.get("notes"),
        iscurrentrow=row_dict.get("iscurrentrow", False),
    )


@_with_reconnect
def write_to_postgresql(query_results):
    if not query_results:
        return
    conn = get_pg_connection()
    rows = [
        (
            r.id,
            r.roworder,
            r.uniquerunid,
            r.query,
            r.appname or "",
            r.targettbl,
            r.status or "",
            r.namespace or "",
            r.started_datetime,
            r.finished_datetime,
            r.notes or "",
            r.iscurrentrow,
        )
        for r in query_results
    ]
    insert_sql = sql.SQL("""
        INSERT INTO {table}
            (id, roworder, uniquerunid, query, appname, targettbl, status,
             namespace, started_datetime, finished_datetime, notes, iscurrentrow)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            roworder          = EXCLUDED.roworder,
            uniquerunid       = EXCLUDED.uniquerunid,
            query             = EXCLUDED.query,
            appname           = EXCLUDED.appname,
            targettbl         = EXCLUDED.targettbl,
            status            = EXCLUDED.status,
            namespace         = EXCLUDED.namespace,
            started_datetime  = EXCLUDED.started_datetime,
            finished_datetime = EXCLUDED.finished_datetime,
            notes             = EXCLUDED.notes,
            iscurrentrow      = EXCLUDED.iscurrentrow
    """).format(table=sql.Identifier(config.PG_TABLE_ID))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_sql.as_string(conn), rows)
    print("Rows have been merged successfully.")


@_with_reconnect
def fetch_record_from_postgresql(record_id):
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT * FROM {table} WHERE id = %s").format(
                table=sql.Identifier(config.PG_TABLE_ID)
            ),
            (record_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [desc[0] for desc in cur.description]
    return _row_to_query_result(dict(zip(cols, row)))


@_with_reconnect
def get_next_id_postgresql():
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COALESCE(MAX(id), 0) + 1 FROM {table}").format(
                table=sql.Identifier(config.PG_TABLE_ID)
            )
        )
        return cur.fetchone()[0]


@_with_reconnect
def update_record_in_postgresql(query_result, return_output=False):
    if query_result.id is None:
        print("Problem, should not have empty id")
        raise NotImplementedError
    conn = get_pg_connection()
    fields = []
    values = []
    for attr, value in query_result.__dict__.items():
        if attr != "id" and value is not None:
            fields.append(sql.Identifier(attr))
            values.append(value)
    values.append(query_result.id)
    update_sql = sql.SQL("UPDATE {table} SET {assignments} WHERE id = %s").format(
        table=sql.Identifier(config.PG_TABLE_ID),
        assignments=sql.SQL(", ").join(sql.SQL("{} = %s").format(f) for f in fields),
    )
    with conn.cursor() as cur:
        cur.execute(update_sql, values)
    print(f"Record with ID {query_result.id} has been updated.")
    if return_output:
        return fetch_record_from_postgresql(query_result.id)


@_with_reconnect
def clear_runid_postgresql(uniquerunid):
    if uniquerunid is None:
        print("Problem, should not have empty uniquerunid")
        raise NotImplementedError
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "UPDATE {table} SET iscurrentrow = FALSE WHERE iscurrentrow = TRUE AND uniquerunid = %s"
            ).format(table=sql.Identifier(config.PG_TABLE_ID)),
            (uniquerunid,),
        )
    print(
        f"Records with uniquerunid {uniquerunid} have been updated as iscurrentrow = FALSE"
    )


@_with_reconnect
def delete_runid_postgresql(uniquerunid):
    if uniquerunid is None:
        print("Problem, should not have empty uniquerunid")
        raise NotImplementedError
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "DELETE FROM {table} WHERE iscurrentrow = TRUE AND uniquerunid = %s"
            ).format(table=sql.Identifier(config.PG_TABLE_ID)),
            (uniquerunid,),
        )
    print(f"Deleted current rows for uniquerunid {uniquerunid}")


@_with_reconnect
def read_from_postgresql(where_clause: str) -> List[QueryResult]:
    conn = get_pg_connection()
    query = sql.SQL("SELECT * FROM {table} WHERE {where}").format(
        table=sql.Identifier(config.PG_TABLE_ID),
        where=sql.SQL(where_clause),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
    return [_row_to_query_result(dict(zip(cols, row))) for row in rows]
