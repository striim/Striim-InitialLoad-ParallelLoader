import csv
import datetime

# pip install google google.cloud google-cloud-bigquery
from google.cloud import bigquery
from typing import List

import config

class QueryResult:
    def __init__(self, roworder, query, targettbl, appname = None, _id = None, status = None, namespace = None,
                 started_datetime = None, finished_datetime = None, notes = None, uniquerunid=None,
                 iscurrentrow=True):
        self.roworder = roworder
        self.id = _id
        self.query = query
        self.appname = appname
        self.targettbl = targettbl
        self.status = status
        self.namespace = namespace
        self.started_datetime = started_datetime
        self.finished_datetime = finished_datetime
        self.notes = notes
        self.uniquerunid = uniquerunid
        self.iscurrentrow = iscurrentrow


current_status: List[QueryResult] = []


def read_csv_to_query_results():
    query_results = []
    with open(config.QUERY_FILE_PATH, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=config.QUERY_FILE_DELIMITER)

        # New file requires only two columns on import.
        for order, row in enumerate(reader, start=1):
            # Skip any empty row
            if len(row) > 0:
                query_result = QueryResult(
                    roworder=order,
                    query=row[0],
                    targettbl=row[1]
                )
                query_results.append(query_result)
    return query_results


# New function to write data to BigQuery
def write_to_bigquery(query_results):
    client = bigquery.Client.from_service_account_json(config.BQ_KEYFILE_LOCATION)
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.{config.TABLE_ID}"


    values = ", ".join([
        f"({result.id}, {result.roworder}, '{result.uniquerunid}', '{result.query}', '{result.appname}', '{result.targettbl}', '{result.status}', '{result.namespace}', '{result.started_datetime.strftime('%Y-%m-%d %H:%M:%S.%f') if result.started_datetime else None}', '{result.finished_datetime.strftime('%Y-%m-%d %H:%M:%S.%f') if result.finished_datetime else None}', '{result.notes}', {result.iscurrentrow})"
        for result in query_results
    ])

    # Construct the MERGE query
    merge_query = f"""
            MERGE INTO `{table_id}` T
            USING (
                SELECT 
                    id,
                    roworder, 
                    uniquerunid, 
                    query, 
                    appname, 
                    targettbl, 
                    status, 
                    namespace, 
                    started_datetime, 
                    finished_datetime, 
                    notes, 
                    iscurrentrow
                FROM (VALUES {values}) AS S(id, roworder, uniquerunid, query, appname, targettbl, status, namespace, started_datetime, finished_datetime, notes, iscurrentrow)
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

    schar = "'"

    union_all_query = " UNION ALL ".join([
        f"SELECT {result.id} AS id, {result.roworder} AS roworder, {result.uniquerunid} AS uniquerunid, '{result.query}' AS query, '{result.appname if result.appname else ''}' AS appname, '{result.targettbl}' AS targettbl, '{result.status if result.status else ''}' AS status, '{result.namespace if result.namespace else ''}' AS namespace, {(schar + result.started_datetime.strftime('%Y-%m-%d %H:%M:%S.%f') + schar) if result.finished_datetime else 'CAST(NULL AS TIMESTAMP)'} AS started_datetime, {(schar + result.finished_datetime.strftime('%Y-%m-%d %H:%M:%S.%f') + schar) if result.finished_datetime else 'CAST(NULL AS TIMESTAMP)'} AS finished_datetime, '{result.notes if result.notes else ''}' AS notes, {result.iscurrentrow if result.iscurrentrow else True} AS iscurrentrow"
        for result in query_results
    ])

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

    # Wait for the job to complete
    query_job.result()

    if query_job.errors:
        print("Encountered errors while merging rows: {}".format(query_job.errors))
    else:
        print("Rows have been merged successfully.")


def fetch_record_from_bigquery(record_id):
    """
    Fetches a single record from BigQuery based on its ID.

    Args:
        record_id (int): The ID of the record to fetch.

    Returns:
        QueryResult: The QueryResult object representing the fetched record, or None if not found.
    """
    client = bigquery.Client.from_service_account_json(config.BQ_KEYFILE_LOCATION)
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.{config.TABLE_ID}"

    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE id = {record_id}
    """

    query_job = client.query(query)
    results = query_job.result()

    for row in results:  # Should only be one row if ID is unique
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
            iscurrentrow=row.iscurrentrow
        )

    return None  # Return None if no record found

def get_next_id():
    """
    Fetches the maximum ID from the BigQuery table and returns the next ID (max ID + 1).

    Returns:
        int: The next ID value.
    """
    client = bigquery.Client.from_service_account_json(config.BQ_KEYFILE_LOCATION)
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.{config.TABLE_ID}"

    query = f"""
        SELECT MAX(id) AS max_id
        FROM `{table_id}`
    """

    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        return row.max_id + 1 if row.max_id is not None else 1

    return 1  # Return 1 if the table is empty

def update_record_in_bigquery(query_result, return_output = False):
    """
    Updates or inserts a record in BigQuery based on the provided QueryResult object.

    Args:
        query_result (QueryResult): The QueryResult object representing the record to update or insert.

    Returns:
        QueryResult: The updated QueryResult object fetched from BigQuery after the upsert.
    """
    client = bigquery.Client.from_service_account_json(config.BQ_KEYFILE_LOCATION)
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.{config.TABLE_ID}"

    # Check if it's an update or insert
    if query_result.id is None:
        print("Problem, should not have empty id")
        raise NotImplementedError
    else:
        # Update existing record
        update_fields = []
        for attr, value in query_result.__dict__.items():
            if attr != 'id' and value is not None:
                if isinstance(value, datetime.datetime):
                    value = f"CAST('{value.strftime('%Y-%m-%d %H:%M:%S.%f')}' AS TIMESTAMP)"
                    update_fields.append(f"{attr} = {value}")
                elif isinstance(value, (int, float)):
                    update_fields.append(f"{attr} = {value}")
                else:
                    update_fields.append(f"{attr} = '{value}'")

        update_query = f"""
            UPDATE `{table_id}`
            SET {', '.join(update_fields)}
            WHERE id = {query_result.id}
        """

        query_job = client.query(update_query)
        query_job.result()

        print(f"Record with ID {query_result.id} has been updated: SELECT * FROM `{table_id}` WHERE id = {query_result.id}")

        # Fetch and return the updated record
        if return_output:
            return fetch_record_from_bigquery(query_result.id)

def clear_runid(uniquerunid):
    """
    Updates or inserts a record in BigQuery based on the provided QueryResult object.

    Args:
        query_result (QueryResult): The QueryResult object representing the record to update or insert.

    Returns:
        QueryResult: The updated QueryResult object fetched from BigQuery after the upsert.
    """
    client = bigquery.Client.from_service_account_json(config.BQ_KEYFILE_LOCATION)
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.{config.TABLE_ID}"

    # Check if it's an update or insert
    if uniquerunid is None:
        print("Problem, should not have empty uniquerunid")
        raise NotImplementedError
    else:
        # Update existing record

        update_query = f"""
            UPDATE `{table_id}`
            SET iscurrentrow = FALSE
            WHERE iscurrentrow = TRUE AND uniquerunid = {config.UNIQUE_RUN_ID}
        """

        query_job = client.query(update_query)
        query_job.result()

        print(
            f"Records with uniquerunid {config.UNIQUE_RUN_ID} has been updated as iscurrentrow = FALSE")


def read_from_bigquery(where_clause):
    client = bigquery.Client.from_service_account_json(config.BQ_KEYFILE_LOCATION)
    table_id = f"{config.PROJECT_ID}.{config.DATASET_ID}.{config.TABLE_ID}"

    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE {where_clause}
    """

    query_job = client.query(query)  # Make an API request.

    results = query_job.result()  # Wait for the job to complete.

    # Process the results and return them in a suitable format
    # For example, you can return a list of dictionaries
    query_result_objects = []

    for row in results:
        query_result = QueryResult(
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
            iscurrentrow=row.iscurrentrow)

        query_result_objects.append(query_result)
    return query_result_objects

def set_current_status(status):
    global current_status
    current_status = status

def update_and_get_current_status():
    global current_status
    # Clear the internal data structure before populating it with new results
    current_status = read_from_bigquery("iscurrentrow = True AND uniquerunid = " + str(config.UNIQUE_RUN_ID))
    return current_status

def get_current_status():
    global current_status
    return current_status
