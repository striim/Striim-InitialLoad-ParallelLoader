import csv
import datetime
from functools import reduce

# pip install google google.cloud google-cloud-bigquery
from google.cloud import bigquery
from typing import List
from tinydb import TinyDB, Query

import re

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

# Function to determine which database to use
def get_database():
    if config.STAGE_DB_LOCATION.upper() in ('BQ', 'BIGQUERY'):
        return 'BQ'
    else:
        return 'TinyDB'

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

# ************************************************************************************
# ************************************************************************************
# ********************************* tinyDB *******************************************
# ************************************************************************************
# ************************************************************************************

# New functions to write data to TinyDB
def write_to_tinydb(query_results):
    db = TinyDB(config.TINYDB_PATH)

    for result in query_results:
        result_dict = result.__dict__.copy()
        if result_dict.get('started_datetime'):
            result_dict['started_datetime'] = result_dict['started_datetime'].strftime('%Y-%m-%d %H:%M:%S.%f')
        if result_dict.get('finished_datetime'):
            result_dict['finished_datetime'] = result_dict['finished_datetime'].strftime('%Y-%m-%d %H:%M:%S.%f')
        db.insert(result_dict)


def fetch_record_from_tinydb(record_id):
    db = TinyDB(config.TINYDB_PATH)
    Record = Query()
    result = db.search(Record.id == record_id)
    if result:
        return result[0]  # Return the first match
    else:
        return None


def get_next_id_tinydb():
    db = TinyDB(config.TINYDB_PATH)
    if db.all():
        max_id = max(item['id'] for item in db.all())
        return max_id + 1
    else:
        return 1


def update_record_in_tinydb(query_result):
    db = TinyDB(config.TINYDB_PATH)
    Record = Query()
    result_dict = query_result.__dict__.copy()
    if result_dict.get('started_datetime'):
        result_dict['started_datetime'] = result_dict['started_datetime'].strftime('%Y-%m-%d %H:%M:%S.%f')
    if result_dict.get('finished_datetime'):
        result_dict['finished_datetime'] = result_dict['finished_datetime'].strftime('%Y-%m-%d %H:%M:%S.%f')
    db.update(result_dict, Record.id == query_result.id)


def clear_runid_tinydb(uniquerunid):
    db = TinyDB(config.TINYDB_PATH)
    Record = Query()
    db.update({'iscurrentrow': False}, Record.iscurrentrow == True and Record.uniquerunid == uniquerunid)


def read_from_tinydb(where_clause_str: str) -> List[QueryResult]:  # Added type hint for return
    db = TinyDB(config.TINYDB_PATH)
    Record = Query()  # TinyDB's Query object

    conditions = []

    # Parse "iscurrentrow = True" or "iscurrentrow = False"
    is_current_match = re.search(r"iscurrentrow\s*=\s*(True|False)", where_clause_str, re.IGNORECASE)
    if is_current_match:
        val = is_current_match.group(1).lower() == 'true'
        conditions.append(Record.iscurrentrow == val)

    # Parse "uniquerunid = <number>"
    unique_run_id_match = re.search(r"uniquerunid\s*=\s*(\d+)", where_clause_str)
    if unique_run_id_match:
        val = int(unique_run_id_match.group(1))
        conditions.append(Record.uniquerunid == val)

    # Add more parsers for other fields if needed in the future

    results_from_db = []
    if conditions:
        # Combine conditions using AND logic (implicit in TinyDB query construction)
        final_query = conditions[0]
        if len(conditions) > 1:
            # For TinyDB, if you have multiple conditions for Record.field,
            # you build them up using logical operators: (Query().field1 == val1) & (Query().field2 == val2)
            final_query = reduce(lambda acc, cond: acc & cond, conditions)  # Pass conditions directly
        results_from_db = db.search(final_query)
    elif not where_clause_str.strip():  # No where clause, get all (use with caution)
        # This case is unlikely given current usage by update_and_get_current_status
        print(f"Warning: read_from_tinydb called with empty where_clause. Returning all documents.")
        results_from_db = db.all()
    else:
        # If clause is present but not parsed by the above, it's an unhandled case.
        print(
            f"Warning: Unhandled or complex where_clause in read_from_tinydb: '{where_clause_str}'. For safety, returning no results.")
        results_from_db = []

    query_result_objects: List[QueryResult] = []  # Added type hint
    for row_dict in results_from_db:  # Renamed 'row' to 'row_dict' for clarity
        started_datetime_str = row_dict.get('started_datetime')
        started_datetime = datetime.datetime.strptime(started_datetime_str,
                                                      '%Y-%m-%d %H:%M:%S.%f') if started_datetime_str else None

        finished_datetime_str = row_dict.get('finished_datetime')
        finished_datetime = datetime.datetime.strptime(finished_datetime_str,
                                                       '%Y-%m-%d %H:%M:%S.%f') if finished_datetime_str else None

        # Ensure all necessary fields from QueryResult are handled, using .get() for robustness
        query_result = QueryResult(
            roworder=row_dict.get('roworder'),
            _id=row_dict.get('id'),  # In TinyDB, documents have 'doc_id', but you store your own 'id'
            uniquerunid=row_dict.get('uniquerunid'),
            query=row_dict.get('query'),
            appname=row_dict.get('appname'),
            targettbl=row_dict.get('targettbl'),
            status=row_dict.get('status'),
            namespace=row_dict.get('namespace'),
            started_datetime=started_datetime,
            finished_datetime=finished_datetime,
            notes=row_dict.get('notes'),
            iscurrentrow=row_dict.get('iscurrentrow', False)  # Default to False if missing
        )
        query_result_objects.append(query_result)
    return query_result_objects


# ************************************************************************************
# ************************************************************************************
# ******************************** BigQuery ******************************************
# ************************************************************************************
# ************************************************************************************


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

def get_next_id_bigquery():
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

def clear_runid_bigquery(uniquerunid):
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


# ************************************************************************************
# ************************************************************************************
# ********************************* General ******************************************
# ************************************************************************************
# ************************************************************************************

def write_data(query_results):
    db = get_database()
    if db == 'BQ':
        write_to_bigquery(query_results)
    else:
        write_to_tinydb(query_results)

def fetch_record(record_id):
    db = get_database()
    if db == 'BQ':
        return fetch_record_from_bigquery(record_id)
    else:
        return fetch_record_from_tinydb(record_id)

def get_next_id():
    db = get_database()
    if db == 'BQ':
        return get_next_id_bigquery()
    else:
        return get_next_id_tinydb()

def update_record(query_result, return_output = False):
    db = get_database()
    if db == 'BQ':
        update_record_in_bigquery(query_result, return_output)
    else:
        update_record_in_tinydb(query_result)
    if return_output:
        return query_result

def clear_runid(uniquerunid):
    db = get_database()
    if db == 'BQ':
        clear_runid_bigquery(uniquerunid)
    else:
        clear_runid_tinydb(uniquerunid)

def read_data(where_clause):
    db = get_database()
    if db == 'BQ':
        return read_from_bigquery(where_clause)
    else:
        return read_from_tinydb(where_clause)

def set_current_status(status):
    global current_status
    current_status = status

def update_and_get_current_status():
    global current_status
    # Clear the internal data structure before populating it with new results
    current_status = read_data("iscurrentrow = True AND uniquerunid = " + str(config.UNIQUE_RUN_ID))
    return current_status

def get_current_status():
    global current_status
    return current_status
