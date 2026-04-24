# Code Analysis: Striim-InitialLoad-ParallelLoader

**Analyst:** Claude Sonnet 4.6  
**Date:** 2026-04-23  
**Scope:** Full codebase review — bugs, security, design, code quality

---

## Repository Structure

```
main.py          — Orchestration logic, Striim API calls, app lifecycle management
config.py        — All configuration (credentials, paths, tuning params, env selection)
data.py          — State persistence: BigQuery and TinyDB backends, QueryResult model
queryfile.txt    — Input: pipe-delimited (query|targettable) rows to process
admin.SW.tql     — TQL template file with ~QUERYTEXT~ and ~TARGETTABLE~ placeholders
pvs.tql          — Property variable definitions for Striim connection strings
oracle_rowsplit.sql — Oracle SQL helper to generate ROWID-based parallel split ranges
BQ_TableCreate.sql  — DDL for BigQuery orchestration table
requirements.txt — Pinned Python dependencies
```

---

## Critical Bugs (P0 — Will Crash or Silently Corrupt Data)

### BUG-1: Missing `namespace` argument in recursive `runTQLFile` call
**File:** `main.py:239`

```python
def runTQLFile(filePath, namespace):
    ...
    if 'reason' in resp.text and 'tkn' in resp.text:
        time.sleep(1)
        return runTQLFile(filePath)   # ← missing namespace argument
```

Raises `TypeError` every time a token expiry error triggers a retry. This retry path is unreachable in practice.

---

### BUG-2: `runCommand` returns `None` (not a tuple) on empty string
**File:** `main.py:271–272`

```python
def runCommand(strCmd, returnResultOnly=False):
    if strCmd == '':
        return   # ← returns None
```

Every caller does `isSuccessful, failuremessage = runCommand(...)`. Passing an empty command causes:
```
TypeError: cannot unpack non-iterable NoneType object
```

**Fix:** `return False, 'Empty command'`

---

### BUG-3: `runTQLFile` returns `''` (empty string) on exception
**File:** `main.py:253–255`

```python
except Exception as e:
    print('Error at runFilePath:', filePath, e)
    return ''   # ← not a (bool, str) tuple
```

Same tuple-unpacking crash as BUG-2.

**Fix:** `return False, str(e)`

---

### BUG-4: TinyDB `clear_runid` uses Python `and` instead of `&`
**File:** `data.py:110`

```python
db.update({'iscurrentrow': False}, Record.iscurrentrow == True and Record.uniquerunid == uniquerunid)
```

Python's `and` short-circuits, evaluating to the last truthy operand. The effective query becomes just `Record.uniquerunid == uniquerunid`, ignoring `iscurrentrow`. This marks **all records of all run IDs** as non-current, corrupting the orchestration state for concurrent or re-used run IDs.

**Fix:**
```python
db.update({'iscurrentrow': False}, (Record.iscurrentrow == True) & (Record.uniquerunid == uniquerunid))
```

---

## Security Issues (P0–P1)

### SEC-1: SQL Injection via string interpolation in BigQuery queries
**File:** `data.py:194–195`, `240`, `365–376`

User-controlled values (`query`, `appname`, `notes`, `targettbl`) are interpolated directly into SQL strings:

```python
f"'{result.query}'"
f"'{result.notes if result.notes else ''}'"
```

A query string containing `'; DROP TABLE striim_orchestration; --` would execute arbitrary SQL against BigQuery. The `notes` field compounds this since it is assembled from API error messages which may include attacker-influenced content.

**Fix:** Use BigQuery parameterized queries (`query_parameters` in `QueryJobConfig`).

---

### SEC-2: TQL Injection via template substitution
**File:** `main.py:783`

```python
modified_content = content.replace('~QUERYTEXT~', queryText).replace('~TARGETTABLE~', targetTable)
```

No sanitization of `queryText` or `targetTable` from `queryfile.txt`. Malformed input can break out of the TQL `Query:` field and inject arbitrary TQL commands executed against the Striim cluster.

---

### SEC-3: Hardcoded credentials in committed config
**File:** `config.py:60–66`, `74`; `main.py:43`

- Default admin credentials: `STRIIM_ADMIN_USER = "admin"`, `STRIIM_ADMIN_PWD = "admin"`
- Personal absolute path to BQ service account JSON: `/Users/danielferrara/Documents/Striim420/.../daniel-sa-striimfieldproject-a326623e58fe.json`
- Commented-out API token in source: `# sToken = '2E9LbUtMvDpM.AgclhtHhPtgaDKsq'` (may be in git history)

**Fix:** Move all secrets to environment variables or a secrets manager. Use `.env` + `python-dotenv` or equivalent.

---

### SEC-4: Plaintext HTTP for all API communication
**File:** `config.py:58`

```python
STRIIM_URL_PREFIX = "http://"
```

Authentication tokens and passwords transmitted in plaintext. No HTTPS enforcement, including no override in the PROD config section.

---

### SEC-5: Plaintext credentials in `pvs.tql`
**File:** `pvs.tql:3–7`

Striim source/target usernames and passwords are in plaintext in a committed TQL file. This file should either not be committed or use placeholder values.

---

## Logic Bugs (P1)

### LOGIC-1: 503 recovery broken — string `in` on list
**File:** `main.py:746–758`

```python
result = runCommand("STATUS " + objectName + ";", True)   # returns a list
if expectedStatus in result:   # checks list element membership, not substring
```

`runCommand(..., True)` returns a parsed JSON list. Using `in` on a list checks for element equality, not substring presence. The status string will never match a list element, silently disabling 503 recovery.

**Fix:** Convert result to string before membership check: `if expectedStatus in str(result):`

---

### LOGIC-2: Duplicate query text used as record identity
**File:** `main.py:522`, `529`, `671`, `679`

```python
if query_results[i].query == qry.query:
```

Records are matched by query text instead of `id`. If two rows in `queryfile.txt` have the same query string (the sample file has 16 identical rows), only the first match is ever updated. Other records are silently left in their previous state.

---

### LOGIC-3: `namespaceCount = runningApps + 1` immediately overwritten
**File:** `main.py:544`, `550`

```python
namespaceCount = runningApps + 1   # line 544
...
namespaceCount = 1                 # line 550 — immediately overwritten
```

The first assignment is dead. The namespace search always starts from 1, making `runningApps + 1` pointless.

---

### LOGIC-4: `made_new_record_change` is always `False` — dead code branch
**File:** `main.py:445`, `543`

This flag is declared `False` and never set to `True` anywhere in `runReview()`. The `if made_new_record_change:` branches at lines 509 and 660 are permanently dead code. The `get_next_id()` calls and alternate record-creation paths are unreachable.

---

### LOGIC-5: PROD environment config is incomplete
**File:** `config.py:80–88`

The `elif ENV == "PROD"` block only overrides `APP_MONITOR_INTERVAL_SECONDS` and `DEPLOY_WAIT_TIME_SECONDS`. All other critical values (`STRIIM_NODE`, `STRIIM_ADMIN_USER`, `STRIIM_ADMIN_PWD`, `BQ_KEYFILE_LOCATION`, etc.) silently fall through to DEV values.

---

### LOGIC-6: `firstRun` logic is redundant
**File:** `main.py:817–841`

```python
firstRun = True
if not firstRun:                              # never executes (firstRun is True)
    query_results = update_and_get_current_status()
if firstRun:                                  # always executes
    query_results = update_and_get_current_status()
```

Both branches call the same function. The conditional structure accomplishes nothing.

---

## Design & Architecture Issues (P2)

### DESIGN-1: Module-level code runs authentication at import time
**File:** `main.py:39–47`

Network authentication, JSON parsing, and global `headers` initialization execute at module load, before the `if __name__ == '__main__'` guard. A network failure at startup raises an uncaught exception with no error handling. This also makes `main.py` impossible to import or test without a live Striim server.

---

### DESIGN-2: `write_to_bigquery` contains duplicate dead code
**File:** `data.py:194–235`

A `merge_query` using `VALUES (...)` syntax (lines 194–235) is constructed and assigned, then immediately overwritten on line 244 with a `UNION ALL` version. The first block and the `values` variable are entirely unused. The `schar = "'"` variable is also suspicious — single quotes inside an f-string as a workaround for Python 3.11 f-string limitations; should be refactored for clarity.

---

### DESIGN-3: `update_record` discards the BigQuery refresh result
**File:** `data.py:484–491`

```python
def update_record(query_result, return_output=False):
    if db == 'BQ':
        update_record_in_bigquery(query_result, return_output)  # return value discarded
    ...
    if return_output:
        return query_result   # returns original object, not DB-refreshed copy
```

`update_record_in_bigquery` fetches and returns the record from BQ after update, but `update_record` discards it and returns the original object. Callers expecting the DB-round-tripped state receive stale data.

---

### DESIGN-4: `write_to_tinydb` only inserts, never upserts
**File:** `data.py:65–74`

```python
def write_to_tinydb(query_results):
    db = TinyDB(config.TINYDB_PATH)
    for result in query_results:
        db.insert(result_dict)   # always inserts, no existence check
```

Calling `write_data()` a second time duplicates all records. There is no upsert logic for TinyDB.

---

### DESIGN-5: Global mutable state
**File:** `main.py:49`, `51`

`query_results` and `next_allowed_run` are module-level globals mutated throughout execution. This makes the code non-reentrant and untestable.

---

### DESIGN-6: Unbounded recursion in `runCommand` and `runTQLFile`
**File:** `main.py:284–285`, `239`

Both functions recurse with no depth counter when receiving token errors. Sustained connectivity issues or repeated bad tokens cause `RecursionError`.

---

## Code Quality Issues (P3)

### QUALITY-1: `queryfile.txt` sample data is misleading
All 16 rows are identical `SELECT * FROM QATEST.WF_PENDING_ACTIVITY|QATEST2.WF_PENDING_ACTIVITY` — the opposite of the README's documented example showing range-split queries. Would not parallelize anything.

### QUALITY-2: Unused imports
- `main.py:11` — `from collections import namedtuple` never used
- `main.py:13` — `import csv` never used (CSV handling is in `data.py`)

### QUALITY-3: Wildcard import
`from data import *` in `main.py:16` pollutes the namespace.

### QUALITY-4: `str` used as parameter name
`def isILApp(str):` in `main.py:337` shadows Python's built-in `str`.

### QUALITY-5: Dead utility code
- `IL_Clean_Done = False` (`main.py:31`) — defined, never referenced
- `doNSClean()` (`main.py:793`) — defined, never called from main loop
- `isCommandSuccessful()` (`main.py:765`) — defined, never called
- `TABLE_LIST` and `MAX_MEMORY_USAGE` in `config.py` — marked "Not yet implemented"

### QUALITY-6: Large commented-out code block
`main.py:317–334` — old `runMon` implementation left as comments.

### QUALITY-7: `BQ_TableCreate.sql` hardcodes personal project/schema
`striimfieldproject.Daniel.striim_orchestration` should use placeholders.

### QUALITY-8: `logDebug` custom flag redundant
`logDebug = False` (`main.py:29`) reimplements what `logging.setLevel(logging.DEBUG)` already does.

### QUALITY-9: Inconsistent logging
The code mixes `print()` and `logging.info()` without a consistent strategy.

### QUALITY-10: No tests
Zero test files in the repository.

---

## Summary Priority Table

| ID | Severity | Description | File |
|----|----------|-------------|------|
| BUG-1 | P0 Crash | Missing `namespace` in `runTQLFile` recursion | `main.py:239` |
| BUG-2 | P0 Crash | `runCommand` returns `None` on empty string | `main.py:271` |
| BUG-3 | P0 Crash | `runTQLFile` returns `''` on exception | `main.py:255` |
| BUG-4 | P0 Data Corruption | TinyDB `and` vs `&` in `clear_runid_tinydb` | `data.py:110` |
| SEC-1 | P0 Security | SQL injection in BigQuery queries | `data.py:195`, `240` |
| SEC-2 | P1 Security | TQL injection via template substitution | `main.py:783` |
| SEC-3 | P1 Security | Hardcoded credentials in committed config | `config.py`, `main.py:43` |
| SEC-4 | P1 Security | HTTP instead of HTTPS | `config.py:58` |
| SEC-5 | P1 Security | Plaintext credentials in `pvs.tql` | `pvs.tql` |
| LOGIC-1 | P1 Silent Failure | 503 recovery broken — `in` on list not string | `main.py:746` |
| LOGIC-2 | P1 Logic | Duplicate query text used as record identity | `main.py:522` |
| LOGIC-3 | P1 Logic | `namespaceCount` immediately overwritten | `main.py:544` |
| LOGIC-4 | P1 Logic | `made_new_record_change` always False — dead branch | `main.py:445` |
| LOGIC-5 | P1 Logic | PROD config missing critical values | `config.py:80` |
| LOGIC-6 | P2 Logic | `firstRun` conditional is redundant | `main.py:817` |
| DESIGN-1 | P2 Design | Auth runs at module import time | `main.py:39` |
| DESIGN-2 | P2 Design | Duplicate dead code in `write_to_bigquery` | `data.py:194` |
| DESIGN-3 | P2 Design | `update_record` discards BQ refresh result | `data.py:484` |
| DESIGN-4 | P2 Design | `write_to_tinydb` inserts duplicates | `data.py:65` |
| DESIGN-5 | P2 Design | Global mutable state | `main.py:49` |
| DESIGN-6 | P2 Design | Unbounded recursion | `main.py:284` |
| QUALITY-1–10 | P3 Quality | Various code quality issues | multiple |
