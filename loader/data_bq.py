import datetime
from typing import List

from google.cloud import bigquery

import config
from models import QueryResult

_bq_client = None


def get_bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client.from_service_account_json(
            config.BQ_KEYFILE_LOCATION
        )
    return _bq_client


def write_to_bigquery(query_results):
    if not query_results:
        return
    client = get_bq_client()
    table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"

    schar = "'"

    def _esc(v):
        return (v or "").replace("'", "''")

    union_all_query = " UNION ALL ".join(
        [
            f"SELECT {result.id} AS id, {result.roworder} AS roworder, {result.uniquerunid} AS uniquerunid, '{_esc(result.query)}' AS query, '{_esc(result.appname)}' AS appname, '{_esc(result.targettbl)}' AS targettbl, '{_esc(result.status)}' AS status, '{_esc(result.namespace)}' AS namespace, {(schar + result.started_datetime.strftime('%Y-%m-%d %H:%M:%S.%f') + schar) if result.started_datetime else 'CAST(NULL AS TIMESTAMP)'} AS started_datetime, {(schar + result.finished_datetime.strftime('%Y-%m-%d %H:%M:%S.%f') + schar) if result.finished_datetime else 'CAST(NULL AS TIMESTAMP)'} AS finished_datetime, '{_esc(result.notes)}' AS notes, {'TRUE' if result.iscurrentrow else 'FALSE'} AS iscurrentrow"
            for result in query_results
        ]
    )

    merge_query = f"""
            MERGE INTO `{table_id}` T
            USING (
                {union_all_query}
            ) S
            ON T.id = S.id
            WHEN MATCHED THEN
                UPDATE SET
                    roworder = S.roworder,
                    uniquerunid = S.uniquerunid,
                    query = S.query,
                    appname = S.appname,
                    targettbl = S.targettbl,
                    status = S.status,
                    namespace = S.namespace,
                    started_datetime = S.started_datetime,
                    finished_datetime = S.finished_datetime,
                    notes = S.notes,
                    iscurrentrow = S.iscurrentrow
            WHEN NOT MATCHED THEN
                INSERT (id, roworder, uniquerunid, query, appname, targettbl, status, namespace, started_datetime, finished_datetime, notes, iscurrentrow)
                VALUES (S.id, S.roworder, S.uniquerunid, S.query, S.appname, S.targettbl, S.status, S.namespace, S.started_datetime, S.finished_datetime, S.notes, S.iscurrentrow)
        """

    query_job = client.query(merge_query)
    query_job.result()

    if query_job.errors:
        print("Encountered errors while merging rows: {}".format(query_job.errors))
    else:
        print("Rows have been merged successfully.")


def fetch_record_from_bigquery(record_id):
    client = get_bq_client()
    table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"

    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE id = {record_id}
    """

    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        return QueryResult(
            roworder=row.roworder,
            _id=row.id,
            uniquerunid=row.uniquerunid,
            query=row.query,
            appname=row.appname,
            targettbl=row.targettbl,
            status=row.status,
            namespace=row.namespace,
            started_datetime=row.started_datetime,
            finished_datetime=row.finished_datetime,
            notes=row.notes,
            iscurrentrow=row.iscurrentrow,
        )

    return None


def get_next_id_bigquery():
    client = get_bq_client()
    table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"

    query = f"""
        SELECT MAX(id) AS max_id
        FROM `{table_id}`
    """

    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        return row.max_id + 1 if row.max_id is not None else 1

    return 1


def update_record_in_bigquery(query_result, return_output=False):
    client = get_bq_client()
    table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"

    if query_result.id is None:
        print("Problem, should not have empty id")
        raise NotImplementedError

    update_fields = []
    for attr, value in query_result.__dict__.items():
        if attr != "id" and value is not None:
            if isinstance(value, datetime.datetime):
                value = f"CAST('{value.strftime('%Y-%m-%d %H:%M:%S.%f')}' AS TIMESTAMP)"
                update_fields.append(f"{attr} = {value}")
            elif isinstance(value, (int, float)):
                update_fields.append(f"{attr} = {value}")
            else:
                escaped = str(value).replace("'", "''")
                update_fields.append(f"{attr} = '{escaped}'")

    update_query = f"""
        UPDATE `{table_id}`
        SET {', '.join(update_fields)}
        WHERE id = {query_result.id}
    """

    query_job = client.query(update_query)
    query_job.result()

    print(
        f"Record with ID {query_result.id} has been updated: SELECT * FROM `{table_id}` WHERE id = {query_result.id}"
    )

    if return_output:
        return fetch_record_from_bigquery(query_result.id)


def clear_runid_bigquery(uniquerunid):
    client = get_bq_client()
    table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"

    if uniquerunid is None:
        print("Problem, should not have empty uniquerunid")
        raise NotImplementedError

    update_query = f"""
        UPDATE `{table_id}`
        SET iscurrentrow = FALSE
        WHERE iscurrentrow = TRUE AND uniquerunid = {uniquerunid}
    """

    query_job = client.query(update_query)
    query_job.result()

    print(
        f"Records with uniquerunid {uniquerunid} has been updated as iscurrentrow = FALSE"
    )


def delete_runid_bigquery(uniquerunid):
    client = get_bq_client()
    table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"

    if uniquerunid is None:
        print("Problem, should not have empty uniquerunid")
        raise NotImplementedError

    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE iscurrentrow = TRUE AND uniquerunid = {uniquerunid}
    """

    query_job = client.query(delete_query)
    query_job.result()

    print(f"Deleted current rows for uniquerunid {uniquerunid}")


def read_from_bigquery(where_clause) -> List[QueryResult]:
    client = get_bq_client()
    table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"

    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE {where_clause}
    """

    query_job = client.query(query)
    results = query_job.result()

    query_result_objects = []
    for row in results:
        query_result_objects.append(
            QueryResult(
                roworder=row.roworder,
                _id=row.id,
                uniquerunid=row.uniquerunid,
                query=row.query,
                appname=row.appname,
                targettbl=row.targettbl,
                status=row.status,
                namespace=row.namespace,
                started_datetime=row.started_datetime,
                finished_datetime=row.finished_datetime,
                notes=row.notes,
                iscurrentrow=row.iscurrentrow,
            )
        )
    return query_result_objects
