import contextlib
import datetime
import json
import os
import re
import tempfile
from functools import reduce
from typing import List

from tinydb import TinyDB, Query
from tinydb.storages import Storage, touch

import config
from models import QueryResult


@contextlib.contextmanager
def _flock(lock_path):
    """Cross-process advisory lock around a read-modify-write. flock on POSIX;
    best-effort (no-op) where fcntl is unavailable (Windows)."""
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
    f = open(lock_path, "w")
    try:
        try:
            import fcntl

            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        except ImportError:
            pass
        yield
    finally:
        try:
            import fcntl

            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except ImportError:
            pass
        f.close()


class AtomicFlockStorage(Storage):
    """TinyDB Storage: temp-write + os.replace on flush (never a partial file),
    wrapped by a flock'd sidecar lock. Closes the corruption / partial-read / wipe
    failure mode (NOT the loader's read-modify-write lost-update race — see spec E)."""

    def __init__(self, path, encoding=None):
        self._path = path
        self._encoding = encoding
        touch(path, create_dirs=True)

    def _lock_path(self):
        return self._path + ".lock"

    def read(self):
        with _flock(self._lock_path()):
            try:
                with open(self._path, encoding=self._encoding) as f:
                    data = f.read()
            except FileNotFoundError:
                return None
            if not data.strip():
                return None
            try:
                return json.loads(data)
            except ValueError:
                return None

    def write(self, data):
        with _flock(self._lock_path()):
            d = os.path.dirname(self._path) or "."
            fd, tmp = tempfile.mkstemp(dir=d, prefix=".tinydb-", suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding=self._encoding) as f:
                    json.dump(data, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self._path)
            except Exception:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
                raise

    def close(self):
        pass


def write_to_tinydb(query_results):
    with TinyDB(config.TINYDB_PATH, storage=AtomicFlockStorage) as db:
        Record = Query()
        for result in query_results:
            result_dict = result.__dict__.copy()
            if result_dict.get("started_datetime"):
                result_dict["started_datetime"] = result_dict[
                    "started_datetime"
                ].strftime("%Y-%m-%d %H:%M:%S.%f")
            if result_dict.get("finished_datetime"):
                result_dict["finished_datetime"] = result_dict[
                    "finished_datetime"
                ].strftime("%Y-%m-%d %H:%M:%S.%f")
            if result_dict.get("id") is None:
                raise ValueError(f"Cannot upsert record with no id: {result_dict}")
            existing = db.search(Record.id == result_dict["id"])
            if existing:
                db.update(result_dict, Record.id == result_dict["id"])
            else:
                db.insert(result_dict)


def fetch_record_from_tinydb(record_id):
    with TinyDB(config.TINYDB_PATH, storage=AtomicFlockStorage) as db:
        Record = Query()
        result = db.search(Record.id == record_id)
        if result:
            return result[0]
        else:
            return None


def get_next_id_tinydb():
    with TinyDB(config.TINYDB_PATH, storage=AtomicFlockStorage) as db:
        all_docs = db.all()
        if all_docs:
            max_id = max(item["id"] for item in all_docs)
            return max_id + 1
        else:
            return 1


def update_record_in_tinydb(query_result):
    with TinyDB(config.TINYDB_PATH, storage=AtomicFlockStorage) as db:
        Record = Query()
        result_dict = query_result.__dict__.copy()
        if result_dict.get("started_datetime"):
            result_dict["started_datetime"] = result_dict["started_datetime"].strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
        if result_dict.get("finished_datetime"):
            result_dict["finished_datetime"] = result_dict[
                "finished_datetime"
            ].strftime("%Y-%m-%d %H:%M:%S.%f")
        db.update(result_dict, Record.id == query_result.id)


def clear_runid_tinydb(uniquerunid):
    with TinyDB(config.TINYDB_PATH, storage=AtomicFlockStorage) as db:
        Record = Query()
        db.update(
            {"iscurrentrow": False},
            (Record.iscurrentrow == True) & (Record.uniquerunid == uniquerunid),
        )


def delete_runid_tinydb(uniquerunid):
    with TinyDB(config.TINYDB_PATH, storage=AtomicFlockStorage) as db:
        Record = Query()
        db.remove((Record.iscurrentrow == True) & (Record.uniquerunid == uniquerunid))


def read_from_tinydb(where_clause_str: str) -> List[QueryResult]:
    with TinyDB(config.TINYDB_PATH, storage=AtomicFlockStorage) as db:
        Record = Query()
        conditions = []

        is_current_match = re.search(
            r"iscurrentrow\s*=\s*(True|False)", where_clause_str, re.IGNORECASE
        )
        if is_current_match:
            val = is_current_match.group(1).lower() == "true"
            conditions.append(Record.iscurrentrow == val)

        unique_run_id_match = re.search(r"uniquerunid\s*=\s*(\d+)", where_clause_str)
        if unique_run_id_match:
            val = int(unique_run_id_match.group(1))
            conditions.append(Record.uniquerunid == val)

        results_from_db = []
        if conditions:
            final_query = conditions[0]
            if len(conditions) > 1:
                final_query = reduce(lambda acc, cond: acc & cond, conditions)
            results_from_db = db.search(final_query)
        elif not where_clause_str.strip():
            print(
                f"Warning: read_from_tinydb called with empty where_clause. Returning all documents."
            )
            results_from_db = db.all()
        else:
            print(
                f"Warning: Unhandled or complex where_clause in read_from_tinydb: '{where_clause_str}'. For safety, returning no results."
            )
            results_from_db = []

        query_result_objects: List[QueryResult] = []
        for row_dict in results_from_db:
            started_datetime_str = row_dict.get("started_datetime")
            started_datetime = (
                datetime.datetime.strptime(started_datetime_str, "%Y-%m-%d %H:%M:%S.%f")
                if started_datetime_str
                else None
            )
            finished_datetime_str = row_dict.get("finished_datetime")
            finished_datetime = (
                datetime.datetime.strptime(
                    finished_datetime_str, "%Y-%m-%d %H:%M:%S.%f"
                )
                if finished_datetime_str
                else None
            )
            query_result_objects.append(
                QueryResult(
                    roworder=row_dict.get("roworder"),
                    _id=row_dict.get("id"),
                    uniquerunid=row_dict.get("uniquerunid"),
                    query=row_dict.get("query"),
                    appname=row_dict.get("appname"),
                    targettbl=row_dict.get("targettbl"),
                    status=row_dict.get("status"),
                    namespace=row_dict.get("namespace"),
                    started_datetime=started_datetime,
                    finished_datetime=finished_datetime,
                    notes=row_dict.get("notes"),
                    iscurrentrow=row_dict.get("iscurrentrow", False),
                )
            )
        return query_result_objects
