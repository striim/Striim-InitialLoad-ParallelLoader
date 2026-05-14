# Striim Initial Load Parallel Loader

A single Striim application reading a large Oracle table is limited to one concurrent reader. This tool breaks the load into many narrow SQL slices and runs them as independent Striim apps in parallel, keeping a configurable number of apps in flight at all times. When one finishes, the next slice is automatically deployed — the orchestrator manages the full app lifecycle so the load continues unattended.

## How it works

**Step 1 — Prepare queryfile.txt**

Create `queryfile.txt` with one `SQL query|target_table` line per slice. This is the file `main.py` reads.

**Step 2 — Reorder for better concurrency (optional)**

```
python make_assorted_queryfile.py
```

This produces `queryfile-assorted.txt` with lines reordered so concurrent apps hit different query types at the same time. Review the wave summary it prints. If you're happy with the ordering, copy it over your input file:

```
cp queryfile-assorted.txt queryfile.txt
```

**Step 3 — Run**

```
python main.py
```

For each slice in `queryfile.txt`, up to `CONCURRENT_APPS_MAX` at a time:

```
generate TQL from template  →  CREATE app  →  DEPLOY  →  START
         monitor until QUIESCED / COMPLETED
         UNDEPLOY  →  DROP  →  clean namespace  →  dispatch next slice
```

State is persisted in TinyDB (local JSON) or BigQuery so runs can be interrupted and resumed.

---

## File inventory

| File | Purpose |
|------|---------|
| `main.py` | Orchestrator — the only entry point you run |
| `config.py` | All tunable settings; credentials are read from env vars |
| `data.py` | State backend abstraction (TinyDB or BigQuery) |
| `admin.SW.tql` | TQL application template with `~QUERYTEXT~` / `~TARGETTABLE~` placeholders |
| `pvs.tql` | Property variable definitions for Striim (connection strings, credentials) |
| `queryfile.txt` | Input: one `query\|target_table` line per slice |
| `queryfile-assorted.txt` | Interleaved version of `queryfile.txt` (produced by `make_assorted_queryfile.py`) |
| `make_assorted_queryfile.py` | Reorders `queryfile.txt` for better concurrency interleaving |
| `oracle_rowsplit.sql` | Oracle SQL helper that generates ROWID-range slices for any table |
| `BQ_TableCreate.sql` | DDL for the BigQuery orchestration table |

---

## Setup

### 1. Python environment

Requires Python 3.9 or higher.

```bash
# Mac / Linux
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Environment variables

Credentials and connection details are read from environment variables — not hardcoded into `config.py`. Set them before running.

**Striim connection (required):**

```bash
export STRIIM_NODE="your-striim-host:9080"
export STRIIM_ADMIN_USER="admin"
export STRIIM_ADMIN_PWD="your-password"
```

**Or authenticate with a token instead of user/password:**

```bash
export STRIIM_API_TOKEN="your-api-token"
```

**BigQuery state backend (only needed if `STAGE_DB_LOCATION = 'BQ'`):**

```bash
export BQ_KEYFILE_LOCATION="/path/to/service-account.json"
export BQ_PROJECT_ID="your-project"
export BQ_DATASET_ID="your-dataset"
```

**Optional overrides:**

```bash
export ENV="PROD"               # defaults to DEV; PROD uses https and longer timing defaults
export STRIIM_URL_PREFIX="https://"
```

### 3. Striim property variables

The TQL template references property variables for all connection strings and credentials. Run `pvs.tql` once in your Striim environment (paste it into the Striim TQL console or execute via the API) and fill in your actual values:

```tql
use admin;
create or replace propertyvariable SourceConnectionString="jdbc:oracle:thin:@YOUR_SOURCE_HOST:1521:YOUR_SID";
create or replace propertyvariable SourceUsername="YOUR_SOURCE_USERNAME";
create or replace propertyvariable SourcePassword="YOUR_SOURCE_PASSWORD";
create or replace propertyvariable TargetConnectionString="jdbc:oracle:thin:@YOUR_TARGET_HOST:1521:YOUR_SID";
create or replace propertyvariable TargetUsername="YOUR_TARGET_USERNAME";
create or replace propertyvariable TargetPassword="YOUR_TARGET_PASSWORD";
```

These are shared across all generated apps and only need to be defined once per Striim environment.

---

## Queryfile pipeline

### Step 1 — Create queryfile.txt

`queryfile.txt` is a pipe-delimited file with one slice per line: the SQL query on the left, the target table on the right.

For column-value splits, write one line per value:

```
SELECT * FROM SCHEMA.TABLE WHERE STATUS = 'OPEN'|SCHEMA2.TABLE_TGT
SELECT * FROM SCHEMA.TABLE WHERE STATUS = 'CLOSED'|SCHEMA2.TABLE_TGT
```

For ROWID-based splits (useful when no suitable filter column exists), `oracle_rowsplit.sql` generates the slice boundaries for any Oracle table. Run it in SQL*Plus or SQL Developer against your table, copy the output, and paste it into `queryfile.txt`.

Multiple query groups — different source tables, different split strategies, different targets — can coexist in the same file:

```
SELECT * FROM SCHEMA.TABLE WHERE STATUS = 'OPEN'|SCHEMA2.TABLE_TGT
SELECT * FROM SCHEMA.TABLE WHERE STATUS = 'CLOSED'|SCHEMA2.TABLE_TGT
SELECT * FROM SCHEMA.TABLE WHERE ROWID BETWEEN '...' AND '...'|SCHEMA2.OTHER_TGT
```

### Step 2 — Produce an interleaved queryfile (recommended)

```bash
python make_assorted_queryfile.py \
  --input queryfile.txt \
  --output queryfile-assorted.txt \
  --seed 42
```

This reorders the lines so that adjacent entries — and therefore the apps that run concurrently — come from different query groups rather than the same one.

**Why this matters:** `main.py` deploys up to `CONCURRENT_APPS_MAX` apps at once, and they all start from the same region of `queryfile.txt`. Without interleaving, a wave of apps could all be reading the same source partition type at the same time — creating hot blocks or lock contention on Oracle — or all writing to the same target table concurrently, which may hit per-table concurrency limits or degrade write throughput. Interleaving ensures each wave is a varied mix of query types.

This is especially valuable when:
- Your source has partition or index contention on specific column values
- Your target system throttles or serializes writes per table
- Some slices are significantly heavier than others (e.g., complex joins vs. simple selects) and you want the load spread evenly over time rather than front-loaded

The script prints a wave-by-wave summary showing how the types are distributed, so you can verify the result before committing to it.

### Step 3 — Decide which ordering to use

Review the wave summary output. If the interleaved ordering looks good, you have two options:

**Option A — overwrite queryfile.txt with the assorted version (simpler):**

```bash
cp queryfile-assorted.txt queryfile.txt
```

`config.py` stays unchanged and `main.py` reads `queryfile.txt` as normal.

**Option B — keep both files and point config.py at the assorted version:**

```python
QUERY_FILE = "queryfile-assorted.txt"
```

If you prefer to keep the original order, leave `queryfile.txt` as-is and skip this step entirely.

---

## TQL template

`admin.SW.tql` is the application template deployed for every slice. It contains two mandatory placeholders:

- **`~QUERYTEXT~`** — replaced with the slice query, on the `DatabaseReader` source
- **`~TARGETTABLE~`** in `Tables: 'QUERY,~TARGETTABLE~'` — replaced with the target table, on the `DatabaseWriter` target

The template references property variables for all connection strings and credentials (defined in `pvs.tql`). Never hardcode credentials directly in the template.

The provided template targets Oracle on both ends. For other database types, adjust the `DatabaseProviderType`, `ConnectionURL` format, and adapter settings accordingly.

---

## Configuration reference

All settings live in `config.py`. Credentials always come from environment variables.

| Setting | Default | Description |
|---------|---------|-------------|
| `QUERY_FILE` | `queryfile.txt` | Input file — change to `queryfile-assorted.txt` when using the interleaved version |
| `UNIQUE_RUN_ID` | `100` | Identifies this load session in the state DB. Keep it unchanged across restarts to resume; change it to start a fresh session (history is preserved) |
| `CONCURRENT_APPS_MAX` | `5` | Maximum number of Striim apps running simultaneously |
| `APP_MONITOR_INTERVAL_SECONDS` | `15` | How often to poll app status. Not recommended below 15s; 60s+ is typical for larger clusters |
| `DEPLOY_WAIT_TIME_SECONDS` | `20` | Minimum pause between deploying successive apps, to avoid overwhelming Striim |
| `STAGE_DB_LOCATION` | `TinyDB` | State backend: `TinyDB` (local JSON, zero setup) or `BQ` (BigQuery) |
| `DEPLOYMENT_GROUP_TARGET` | `default` | Striim deployment group to use when deploying apps |
| `ILA_APP_NAME_BASE` | `OracleInitialLoadApp` | App name inside each namespace — must match the name in your TQL template |
| `LOG_OUTPUT_PATH` | `logging/striimautoloader.log` | Path for the log file |

---

## Running

```bash
python main.py
```

**First run:** If no records exist in the state DB for `UNIQUE_RUN_ID`, the program reads `QUERY_FILE`, registers all slices as `NEW`, and begins the orchestration loop.

**Resuming an interrupted run:** If records already exist for `UNIQUE_RUN_ID`, the program resumes from where it left off — any slice not yet in `COMPLETED` or `FAILED` is eligible for dispatch. Keep `UNIQUE_RUN_ID` unchanged between start and resume.

**Namespace scheme:** Each app is deployed into a dedicated Striim namespace named `ILA_{UNIQUE_RUN_ID}_{N}` (e.g. `ILA_100_1`, `ILA_100_2`, ...). The namespace is created before deployment and dropped after the app completes successfully. This means:

- All apps created by this tool are visible in the Striim UI under the `ILA_` prefix
- Namespaces do not leak between runs or between concurrent slots
- Multiple `UNIQUE_RUN_ID` values can coexist without collision

The program loops until every slice reaches `COMPLETED` or `FAILED`, marks the run finished in the state DB, and exits.

---

## State and monitoring

### Status values

| Status | Meaning |
|--------|---------|
| *(blank / NEW)* | Registered but not yet dispatched |
| `RUNNING` | App created, deployed, and started |
| `COMPLETED` | App quiesced or completed; undeploy and drop succeeded |
| `COMPLETED-FAILEDDROP` | App completed but undeploy or drop failed; manual cleanup may be needed |
| `FAILED` | An error occurred at create, deploy, or start; see `notes` for details |

### BigQuery orchestration table

Required when `STAGE_DB_LOCATION = 'BQ'`. Create it before the first run using `BQ_TableCreate.sql`, or with this DDL:

```sql
CREATE TABLE `your_project.your_dataset.striim_orchestration` (
    id INTEGER NOT NULL,
    roworder INTEGER,
    uniquerunid INTEGER,
    query STRING,
    appname STRING,
    targettbl STRING,
    status STRING,
    namespace STRING,
    started_datetime TIMESTAMP,
    finished_datetime TIMESTAMP,
    notes STRING,
    iscurrentrow BOOL
);
```

`iscurrentrow` is set to `FALSE` at the end of each run, so history from multiple `UNIQUE_RUN_ID` values accumulates in the same table.

**Useful monitoring queries:**

```sql
-- Progress summary for the current run
SELECT status, COUNT(*) AS cnt
FROM `your_project.your_dataset.striim_orchestration`
WHERE iscurrentrow = TRUE AND uniquerunid = 100
GROUP BY status
ORDER BY status;

-- Slices still in flight or pending
SELECT appname, targettbl, status, started_datetime, notes
FROM `your_project.your_dataset.striim_orchestration`
WHERE iscurrentrow = TRUE AND uniquerunid = 100
  AND status NOT IN ('COMPLETED', 'FAILED')
ORDER BY roworder;
```

---

## Cleanup

**Local state and generated files:**

```bash
# Mac / Linux
rm logging/*.json logging/*.log
rm stage/*.tql
```

```bash
# Windows
del logging\*.json logging\*.log stage\*.tql
```

**BigQuery — retire a run without deleting history:**

```sql
UPDATE `your_project.your_dataset.striim_orchestration`
SET iscurrentrow = FALSE
WHERE iscurrentrow = TRUE AND uniquerunid = 100;
```

**BigQuery — full reset:**

```sql
TRUNCATE TABLE `your_project.your_dataset.striim_orchestration`;
```

**Striim — clean up leftover apps and namespaces:**

If a run was interrupted and apps remain, find them in the Striim UI by the `ILA_` prefix, or run `mon;` in the TQL console. For each leftover app:

```tql
STOP APPLICATION ILA_100_1.OracleInitialLoadApp;
UNDEPLOY APPLICATION ILA_100_1.OracleInitialLoadApp;
DROP APPLICATION ILA_100_1.OracleInitialLoadApp CASCADE;
DROP NAMESPACE ILA_100_1 CASCADE;
```
