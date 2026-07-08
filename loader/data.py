import csv
import os
from typing import List

import config
import data_bq
import data_oracle
import data_pg
import data_tinydb
from models import QueryResult

current_status: List[QueryResult] = []


def get_database():
    val = config.STAGE_DB_LOCATION.upper()
    if val in ("BQ", "BIGQUERY"):
        return "BQ"
    elif val in ("PG", "POSTGRES", "POSTGRESQL"):
        return "PG"
    elif val in ("ORACLE", "ORA"):
        return "ORACLE"
    else:
        return "TinyDB"


DEMO_QUERYFILE_MARKER = "QASOURCE.ADDRESSES"


def read_csv_to_query_results():
    path = config.QUERY_FILE_PATH
    if not os.path.exists(path):
        raise SystemExit(
            f"No queryfile at {path}. Generate one first: ./manage.sh split  "
            f"(or `python manage.py split --table OWNER.TABLE --target OWNER.TGT`)."
        )
    query_results = []
    with open(path, "r") as csvfile:
        reader = csv.reader(csvfile, delimiter=config.QUERY_FILE_DELIMITER)
        for order, row in enumerate(reader, start=1):
            if len(row) == 0:
                continue
            if len(row) < 2:
                raise SystemExit(
                    f"{path} line {order} is missing the '{config.QUERY_FILE_DELIMITER}target' "
                    f"table reference: {row[0]!r}. Each non-blank line must be "
                    f"`query{config.QUERY_FILE_DELIMITER}OWNER.TARGET`."
                )
            query_results.append(
                QueryResult(roworder=order, query=row[0], targettbl=row[1])
            )
    if any(
        DEMO_QUERYFILE_MARKER in (qr.targettbl or "")
        or DEMO_QUERYFILE_MARKER in (qr.query or "")
        for qr in query_results
    ):
        print(
            f"WARNING: queryfile still references the shipped demo table {DEMO_QUERYFILE_MARKER}. "
            "Replace it with your real source/target before a production load."
        )
    return query_results


def write_data(query_results):
    db = get_database()
    if db == "BQ":
        data_bq.write_to_bigquery(query_results)
    elif db == "PG":
        data_pg.write_to_postgresql(query_results)
    elif db == "ORACLE":
        data_oracle.write_to_oracle(query_results)
    else:
        data_tinydb.write_to_tinydb(query_results)


def fetch_record(record_id):
    db = get_database()
    if db == "BQ":
        return data_bq.fetch_record_from_bigquery(record_id)
    elif db == "PG":
        return data_pg.fetch_record_from_postgresql(record_id)
    elif db == "ORACLE":
        return data_oracle.fetch_record_from_oracle(record_id)
    else:
        return data_tinydb.fetch_record_from_tinydb(record_id)


def get_next_id():
    db = get_database()
    if db == "BQ":
        return data_bq.get_next_id_bigquery()
    elif db == "PG":
        return data_pg.get_next_id_postgresql()
    elif db == "ORACLE":
        return data_oracle.get_next_id_oracle()
    else:
        return data_tinydb.get_next_id_tinydb()


def update_record(query_result, return_output=False):
    db = get_database()
    if db == "BQ":
        result = data_bq.update_record_in_bigquery(query_result, return_output)
    elif db == "PG":
        result = data_pg.update_record_in_postgresql(query_result, return_output)
    elif db == "ORACLE":
        result = data_oracle.update_record_in_oracle(query_result, return_output)
    else:
        data_tinydb.update_record_in_tinydb(query_result)
        result = query_result
    if return_output:
        return result


def clear_runid(uniquerunid):
    db = get_database()
    if db == "BQ":
        data_bq.clear_runid_bigquery(uniquerunid)
    elif db == "PG":
        data_pg.clear_runid_postgresql(uniquerunid)
    elif db == "ORACLE":
        data_oracle.clear_runid_oracle(uniquerunid)
    else:
        data_tinydb.clear_runid_tinydb(uniquerunid)


def delete_runid(uniquerunid):
    db = get_database()
    if db == "BQ":
        data_bq.delete_runid_bigquery(uniquerunid)
    elif db == "PG":
        data_pg.delete_runid_postgresql(uniquerunid)
    elif db == "ORACLE":
        data_oracle.delete_runid_oracle(uniquerunid)
    else:
        data_tinydb.delete_runid_tinydb(uniquerunid)


def read_data(where_clause):
    db = get_database()
    if db == "BQ":
        return data_bq.read_from_bigquery(where_clause)
    elif db == "PG":
        return data_pg.read_from_postgresql(where_clause)
    elif db == "ORACLE":
        return data_oracle.read_from_oracle(where_clause)
    else:
        return data_tinydb.read_from_tinydb(where_clause)


def update_and_get_current_status():
    global current_status
    current_status = read_data(
        "iscurrentrow = True AND uniquerunid = " + str(config.UNIQUE_RUN_ID)
    )
    return current_status
