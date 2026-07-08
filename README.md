# Striim Initial Load Parallel Loader

A Python orchestration tool that breaks large database initial loads into parallel slices, runs them as independent Striim applications simultaneously, manages the full app lifecycle unattended, and hands off a CDC watermark so your change-data pipeline can catch up cleanly.

---

## Table of Contents

1. [Why You Need This Tool](#1-why-you-need-this-tool)
2. [Key Concepts](#2-key-concepts)
3. [Prerequisites](#3-prerequisites)
4. [End-to-End Walkthrough](#4-end-to-end-walkthrough)
5. [Configuration Reference](#5-configuration-reference)
6. [Management CLI Reference](#6-management-cli-reference)
7. [Probe Deep-Dive](#7-probe-deep-dive)
8. [Split Predicates Deep-Dive](#8-split-predicates-deep-dive)
9. [State Backends](#9-state-backends)
10. [Failure Handling and Recovery](#10-failure-handling-and-recovery)
11. [Cleanup](#11-cleanup)
12. [Troubleshooting](#12-troubleshooting)
13. [TQL Template and Namespace Scheme](#13-tql-template-and-namespace-scheme)
14. [File Inventory](#14-file-inventory)
15. [Source Engine Support](#15-source-engine-support)
16. [Known Limitations](#16-known-limitations)
17. [Appendix — Manual ROWID Split](#appendix--manual-rowid-split-oracle_rowsplitsql)

---

## 1. Why You Need This Tool

### The single-reader problem

A standard Striim application uses one `DatabaseReader` per app. For a small or medium table, this is fine — one sequential scan, one Striim app, done. But for a large table (tens of millions to billions of rows), a single reader becomes a bottleneck:

- The scan takes days, not hours
- There is no way to speed it up by adding more Striim nodes — the bottleneck is the single sequential read
- If the app fails partway through, the entire load must restart

### The solution: parallel slices

This tool divides the source table into many **disjoint SQL slices** — non-overlapping subsets of rows that together cover the full table — and runs each slice as a separate, independent Striim app. Instead of one app reading 500 million rows sequentially, you might have 64 apps each reading ~8 million rows in parallel.

```
Source table (500M rows)
        │
   ┌────┴────┐
   │  split  │  → 64 slices
   └────┬────┘
        │
  ┌─────▼─────────────────────────┐
  │  Orchestrator (this tool)     │
  │  Runs up to N apps at once    │
  │  App 1: rows 1–8M    [DONE]   │
  │  App 2: rows 8–16M   [DONE]   │
  │  App 3: rows 16–24M  [RUNNING]│
  │  App 4: rows 24–32M  [RUNNING]│
  │  App 5: rows 32–40M  [RUNNING]│
  │  App 6: rows 40–48M  [NEW]    │
  │  ...                          │
  └───────────────────────────────┘
        │
  Target table
```

The orchestrator manages the full lifecycle of every app: create → deploy → start → monitor → undeploy → drop → clean namespace. It keeps a configurable number of apps in flight at all times and starts the next slice automatically when one finishes. The load runs completely unattended.

### Don't guess — measure

The right split strategy and chunk count depend on your specific table, its indexes, its joins, and your source database. Guessing produces sub-optimal splits. The **probe** measures actual throughput on a bounded sample of your real data and recommends the strategy, chunk count, and concurrency. It takes a few minutes and saves hours of wasted run time.

### CDC hand-off

When an initial load finishes, a CDC pipeline typically starts replaying changes from the point the snapshot began. To find that point, this tool captures the source database's CDC watermark at the start of the load — an Oracle SCN, a PostgreSQL WAL LSN, or a SQL Server LSN, depending on the engine (see [CDC watermark](#cdc-watermark)). You give that watermark to your downstream CDC reader as its start position. The tool also writes it to a sidecar file so you have it in writing after the load completes.

---

## 2. Key Concepts

### Slices

A **slice** is one SQL query that reads a disjoint range of rows. The source table is divided into N slices; each slice runs as a separate Striim app. Slices are disjoint — no row appears in more than one slice — so they can run in parallel without conflicts.

### The queryfile

`queryfile.txt` is the input to the loader: one line per slice, the SQL query on the left and the target table on the right, separated by `|`:

```
SELECT s.* FROM PAY.CM_FB_SUBMISSION s WHERE s.ENTITY_TYPE='CASE' AND s.ROWID BETWEEN 'AAA' AND 'BBB'|PAY.CM_FB_SUBMISSION_TGT
SELECT s.* FROM PAY.CM_FB_SUBMISSION s WHERE s.ENTITY_TYPE='CASE' AND s.ROWID BETWEEN 'BBB' AND 'CCC'|PAY.CM_FB_SUBMISSION_TGT
...
```

The `split` command generates this file for you. You can also write it by hand for simple cases.

### The `~SPLIT~` token

You write your source query once with a `~SPLIT~` placeholder where the slice predicate should go. The splitter replaces `~SPLIT~` with the actual boundary condition for each slice:

```sql
-- Your query template:
SELECT s.* FROM PAY.CM_FB_SUBMISSION s WHERE s.ENTITY_TYPE = 'CASE' AND ~SPLIT~

-- Becomes 64 lines in queryfile.txt:
... WHERE s.ENTITY_TYPE = 'CASE' AND s.ROWID BETWEEN 'AAA...' AND 'BBB...'
... WHERE s.ENTITY_TYPE = 'CASE' AND s.ROWID BETWEEN 'BBB...' AND 'CCC...'
...
```

Joins, hints, JSON unnesting, subqueries, and projections pass through verbatim — only the `~SPLIT~` placeholder is replaced.

### Concurrency

`CONCURRENT_APPS_MAX` (default `5`) controls how many slices run simultaneously. The orchestrator keeps exactly this many apps in flight at all times: when one finishes, it immediately starts the next. The right number depends on your Striim cluster capacity and source database load tolerance.

### Split strategies

Three ways to divide the table. The physical strategy uses an engine-specific row address; column range and partition strategies are conceptually identical across engines.

| Strategy | Predicate (per engine) | Engine support | Best for |
|---|---|---|---|
| **Physical** | Oracle: `ROWID BETWEEN 'lo' AND 'hi'` · PostgreSQL: `ctid` range · SQL Server: clustered-key range | All supported engines (predicate syntax varies) | Default — fastest, no index dependency, works on any table |
| **Column range** | `col >= lo AND col < hi` (half-open; syntax identical across engines) | All supported engines | An indexed high-NDV column that lets the source engine use an index range scan or partition pruning — often wins on tables with joins or JSON unnesting |
| **Partition** | Oracle: `TABLE PARTITION (P)` · PostgreSQL: child-relation scan (`FROM schema.child`) · SQL Server: `$PARTITION.fn(col) = N` predicate | Engines with partition support | Already-partitioned tables — deterministic, no measurement needed |

### The probe

The probe races your exact query against candidate split strategies, measures actual MB/s on a bounded sample, and recommends the strategy, chunk count, and concurrency. It never full-scans the table. Run it before splitting — it turns a guessing game into a measurement.

### CDC watermark

At fresh-run start the tool captures the source database's CDC watermark — the "start here" position for your CDC pipeline. The mechanism is engine-specific:

| Source engine | Watermark captured | How |
|---|---|---|
| **Oracle** | System Change Number (SCN) | `SELECT current_scn FROM v$database`, falling back to `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER` |
| **PostgreSQL** | WAL Log Sequence Number (LSN) | `pg_current_wal_lsn()` + optional `pg_export_snapshot()` |
| **SQL Server** | LSN | `sys.fn_cdc_get_max_lsn()` / `@@DBTS` |

The watermark is:
- Printed as a prominent banner on the console
- Written to `logging/run_<id>_watermark.json` with the engine-supplied label and value

The initial load snapshot and CDC replay **intentionally overlap** — CDC starts before the load finishes and replays changes that occurred during the load. This overlap is only safe if your CDC pipeline applies changes as **upserts or merges** (not blind inserts).

### Idempotency and what happens when a slice fails

**This is the most important operational concept to understand.**

Striim's DatabaseWriter commits rows to the target in batches as it reads. There is no distributed transaction — each committed batch is permanent. If a slice fails midway (Striim error, network drop, stall timeout), the rows already committed remain on the target. There is no automatic rollback.

When you reset and re-run the failed slice, Striim re-reads the full slice from scratch and re-inserts everything. The TQL template handles the resulting duplicates with `IgnorableExceptionCode: 'DUPLICATE_ROW_EXISTS'` — **but this only works if the target table has a primary key or unique constraint.**

**Rule: always define a primary key on the target table before running the load. A heap target (no PK) silently accumulates duplicate rows on any re-run.**

### State backend

The orchestrator persists every slice's status in a configurable state backend. This means a run can be interrupted and resumed: keep `UNIQUE_RUN_ID` unchanged and re-run `python main.py`. Slices already `COMPLETED` are skipped; the remaining ones pick up where they left off.

---

## 3. Prerequisites

### What you need

1. **Python 3.9+** — runs on your local machine or a jump host; does not need to run on the Striim server
2. **Striim** — any recent version with `DatabaseReader` and `DatabaseWriter` adapters, running and reachable over HTTP or HTTPS
3. **A source database** — Oracle, PostgreSQL, SQL Server, or any JDBC source (see [Source Engine Support](#15-source-engine-support)). You need a **read-only account** with `SELECT` on the table(s) being loaded. Engine-specific extras:
   - **Oracle:** `SELECT` on `V$DATABASE` or `EXECUTE` on `DBMS_FLASHBACK` for SCN watermark capture (absence degrades gracefully to a warning); for ROWID splitting, `EXECUTE` on `DBMS_PARALLEL_EXECUTE` (preferred), `SELECT` on `DBA_EXTENTS`, or just a read account (falls back to `NTILE`)
   - **PostgreSQL:** a role that can read `pg_catalog` / `pg_stats` and call `pg_current_wal_lsn()` for the LSN watermark
   - **SQL Server:** read access to the `sys.*` catalog views; `sys.fn_cdc_get_max_lsn()` (or `@@DBTS`) for the LSN watermark
4. **JDBC driver for your engine** installed in Striim — needed by Striim's `DatabaseReader`, not by this tool
5. **Target table** — must exist and must have a **primary key or unique constraint** on the natural key before the load starts
6. **Striim property variables** — connection strings and credentials defined in Striim (see [Step 4](#step-4--set-up-striim-property-variables))

### Source database driver for probe and split

The probe and split commands connect directly to the source database from your machine to discover boundaries. The required driver depends on your source engine and is not included in `requirements.txt` — install only what you need:

| Source engine | Install command |
|---|---|
| Oracle | `pip install python-oracledb` |
| PostgreSQL | already in `requirements.txt` via `psycopg2-binary` |
| SQL Server | `pip install pyodbc` |

The `status`, `clear`, `reset`, `logs`, `board`, and `setup` commands do **not** need a source database driver.

#### Driver matrix (all source engines)

The probe/split source engine is selected with `SOURCE_DB_TYPE` (config.py) or the `--source-engine` flag. Drivers are lazy/optional — install only the engine(s) you actually probe or split against:

| Source engine | `pip install` | Required env vars | Notes |
|---|---|---|---|
| `oracle` (default) | `python-oracledb` | `ORACLE_DSN` **or** `ORACLE_HOST`+`ORACLE_SERVICE`; `ORACLE_USER`; `ORACLE_PASSWORD` (`ORACLE_PORT` defaults 1521) | Optional — thin mode, no Instant Client needed. |
| `postgres` | `psycopg2-binary` | `SOURCE_PG_HOST` / `SOURCE_PG_PORT` / `SOURCE_PG_DATABASE` / `SOURCE_PG_USER` / `SOURCE_PG_PASSWORD` / `SOURCE_PG_SSLMODE` | **Already bundled** in `requirements.txt` for the PG state backend; reused here. |
| `sqlserver` | `pyodbc` | `SQLSERVER_HOST` / `SQLSERVER_PORT` / `SQLSERVER_DATABASE` / `SQLSERVER_USER` / `SQLSERVER_PASSWORD` / `SQLSERVER_DRIVER` | Optional — also needs a system ODBC driver, e.g. `ODBC Driver 18 for SQL Server`. |
| `jdbc` | `JayDeBeApi` + `JPype1` | `JDBC_DRIVER_CLASS` / `JDBC_URL` / `JDBC_JAR_PATH` / `JDBC_USER` / `JDBC_PASSWORD` (optional `JDBC_ROW_LIMIT_SYNTAX`, `JDBC_WATERMARK_SQL`) | Optional — also needs a JRE/JDK (Java on `PATH`) and the vendor JDBC `.jar`. Best-effort (see [Source Engine Support](#15-source-engine-support)). |

### Network access

The machine running the loader must reach:
- **Striim REST API** — `STRIIM_NODE` host + port (default 9080 for HTTP)
- **Source database** — for probe, split, and watermark capture (read-only account is sufficient)
- **State backend** — only if using PostgreSQL, BigQuery, or Oracle as the state store

---

## 4. End-to-End Walkthrough

The recommended path: **install → configure → set up Striim → validate → write query → probe → split → run → monitor → reconcile.**

### Two ways to drive it

- **Guided (interactive):** run `./manage.sh` with no arguments to open the management console, then press **`G`** for the first-time guided walkthrough — or pick the numbered actions directly. Every wizard **validates as you type and re-prompts on bad input** (a non-numeric chunk count, a malformed `OWNER.TABLE`, an unknown engine or strategy, a bad port) rather than failing later, so you can't get far with a wrong value. This is the easiest way to start.
- **Scripted (CLI):** run the `python manage.py <command>` subcommands shown in each step below. These take the same inputs as flags and are what the wizards call under the hood; they're ideal for automation. Invalid flags are rejected immediately with a clear one-line message.

> Windows: run the `./manage.sh` TUI under WSL or Git Bash. The `python manage.py …` subcommands work natively in `cmd`/PowerShell.

The steps below show the scripted form; the guided menu walks you through the identical sequence.

### Step 1 — Install

```bash
# Mac / Linux
git clone <this-repo>
cd Striim-InitialLoad-ParallelLoader
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install python-oracledb   # for probe and split
```

```bash
# Windows (Command Prompt or PowerShell)
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
pip install python-oracledb   # for probe and split
```

### Step 2 — Set environment variables

Credentials and connection details are read from environment variables — never hardcoded. Export them in your shell or place them in a `.env` file loaded by your shell profile.

**Striim (required):**

```bash
export STRIIM_NODE="your-striim-host:9080"
export STRIIM_ADMIN_USER="admin"
export STRIIM_ADMIN_PWD="your-password"
# OR use a pre-issued API token instead of user/password:
# export STRIIM_API_TOKEN="your-api-token"
```

**Source database (needed for probe, split, and watermark capture).** Set `SOURCE_DB_TYPE` to your engine (`oracle` is the default) and export that engine's variables. Oracle shown here; PostgreSQL, SQL Server, and JDBC have their own blocks — see the [driver matrix](#driver-matrix-all-source-engines) and [Configuration Reference](#5-configuration-reference).

```bash
export SOURCE_DB_TYPE="oracle"          # or postgres | sqlserver | jdbc
export ORACLE_USER="readonly_user"
export ORACLE_PASSWORD="your-password"
export ORACLE_DSN="your-oracle-host:1521/SERVICE_NAME"
# OR separately:
# export ORACLE_HOST="your-oracle-host"
# export ORACLE_SERVICE="SERVICE_NAME"
# export ORACLE_PORT="1521"   # default
```

**DEV vs PROD:** set `ENV=PROD` to switch to HTTPS defaults and longer polling/deploy intervals.

```bash
export ENV="PROD"
```

### Step 3 — Configure `config.py`

Open `config.py` and set the values for your run. The most important ones:

```python
UNIQUE_RUN_ID = 100           # Change for each new load; keep the same to resume
CONCURRENT_APPS_MAX = 5       # Parallel Striim apps in flight simultaneously
STAGE_DB_LOCATION = "TinyDB"  # Start here; see State Backends section for PG/BQ/Oracle
```

For a first run, all other settings can stay at their defaults. See [Configuration Reference](#5-configuration-reference) for the full list.

### Step 4 — Set up Striim property variables

The TQL template uses Striim property variables for all credentials. Run `pvs.tql` **once per Striim environment** to define them:

```bash
# Via Striim console CLI:
$STRIIM_HOME/bin/console.sh <<'EOF'
use admin;
create or replace propertyvariable SourceConnectionString="jdbc:oracle:thin:@YOUR_SOURCE_HOST:1521:YOUR_SID";
-- PostgreSQL source example: jdbc:postgresql://HOST:5432/DB
-- SQL Server source example:  jdbc:sqlserver://HOST:1433;databaseName=DB
create or replace propertyvariable SourceUsername="YOUR_SOURCE_USERNAME";
create or replace propertyvariable SourcePassword="YOUR_SOURCE_PASSWORD";
create or replace propertyvariable TargetConnectionString="jdbc:oracle:thin:@YOUR_TARGET_HOST:1521:YOUR_SID";
-- PostgreSQL target example: jdbc:postgresql://HOST:5432/DB
-- SQL Server target example:  jdbc:sqlserver://HOST:1433;databaseName=DB
create or replace propertyvariable TargetUsername="YOUR_TARGET_USERNAME";
create or replace propertyvariable TargetPassword="YOUR_TARGET_PASSWORD";
EOF
```

Or paste the contents of `pvs.tql` (with your values) into the Striim web console. These are shared across all generated apps and only need to be defined once.

> **Security:** property variables are stored encrypted in Striim. Never paste credentials directly into `admin.SW.tql` or `queryfile.txt`.

### Step 5 — Validate your setup

Before running the load, verify connectivity and prepare the orchestration state table:

```bash
./manage.sh setup
# Windows:
python manage.py setup
```

This connects to Striim and your chosen state backend, creates the orchestration table if it doesn't exist, and prints a summary of what it found. Fix any errors before proceeding — this step catches mis-configured environment variables, unreachable hosts, and permission problems early.

The interactive wizard (`setup --interactive`, or option 12 in `./manage.sh`) prompts for each credential and **re-prompts on invalid input**: the source engine must be one of the four supported values, a port must be a positive integer, and the *use / reset / cancel* choice only accepts a recognized answer. A blank keeps the existing value. Non-numeric or out-of-range environment values (e.g. `PG_PORT=abc`) are reported with a clear message such as *"PG_PORT must be an integer"* instead of a stack trace.

### Step 6 — Write your query

Create a SQL file with your source query. Include a `~SPLIT~` token where the slice predicate should be injected, and alias your driving table:

```sql
-- my_query.sql
SELECT s.COL_A, s.COL_B, s.COL_C
FROM SCHEMA.SOURCE_TABLE s
WHERE s.ENTITY_TYPE = 'CASE'
AND ~SPLIT~
```

**Rules:**
- `~SPLIT~` is required for reliable injection; if omitted, the splitter appends `AND <pred>` / `WHERE <pred>` instead, but a marker is more reliable
- The driving table must have an alias (e.g. `s`) — the predicate is alias-qualified so it binds to the right table in a join
- Joins, hints, JSON unnesting, subqueries, and projections pass through verbatim
- **Do not append a `|TARGET` suffix to this template.** The target is supplied separately with `split --target OWNER.TGT`. The `query|target` line format belongs to the *generated* `queryfile.txt` and to pasted batch lines (option 15) — not to the `--query-file` template. A stray `|TARGET` here leaks into the SQL (`… WHERE <pred>|TARGET`) and every slice fails.

See `queryfile.txt.example` for a working example using the seeded `PAY.CM_FB_SUBMISSION` table.

> **Input is validated.** `--table`/`--target` must be `OWNER.TABLE` identifiers, `--alias` and `--column` must be plain identifiers, and numeric flags (`--chunks`, probe tunables) must be positive integers. Bad values are rejected with a clear one-line message (not a stack trace); in the interactive `./manage.sh` wizards you are simply asked to enter the value again.

### Step 7 — Probe (measure before you split)

Run the probe to find the best split strategy and chunk count for your specific query:

```bash
./manage.sh probe \
  --query-file my_query.sql \
  --table SCHEMA.SOURCE_TABLE \
  --depth bakeoff
# Windows:
python manage.py probe --query-file my_query.sql --table SCHEMA.SOURCE_TABLE --depth bakeoff
```

The probe races candidate strategies on a bounded sample of your real data and prints a recommendation panel:

```
================================================================
PROBE RECOMMENDATION
================================================================
  strategy:        column
  key:             CREATED_DT
  chunk_count:     64
  concurrency:     8
  winner_MB_per_sec: 41.20
----------------------------------------------------------------
  label          kind       MB/s  access_path               verdict
  ROWID          physical  28.10  TABLE ACCESS BY ROWID R…  good
  CREATED_DT     column    41.20  INDEX RANGE SCAN          good
  ENTITY_ID      column     3.40  TABLE ACCESS FULL         AMPLIFYING
----------------------------------------------------------------
  partition: table IS partitioned — consider --strategy partition
  warnings:
    - AMPLIFYING: candidate 'ENTITY_ID' full-scans PAY.SUBMSN_RESP (join amplification)
================================================================
```

The recommendation is also saved to `logging/probe_recommendation.json`.

> The probe never full-scans the real table. Every timing query is capped at `--sample-rows` rows (default 100,000) and a wall-clock `--time-budget-seconds` backstop (default 20s). Safe to run against trillion-row tables.

See [Probe Deep-Dive](#7-probe-deep-dive) for depth tiers and what each column means.

### Step 8 — Split (generate the slice list)

Apply the probe's recommendation to generate `queryfile.txt`:

```bash
./manage.sh split \
  --query-file my_query.sql \
  --table SCHEMA.SOURCE_TABLE \
  --target SCHEMA.TARGET_TABLE \
  --strategy column \
  --column CREATED_DT \
  --chunks 64
# Windows:
python manage.py split --query-file my_query.sql --table SCHEMA.SOURCE_TABLE \
  --target SCHEMA.TARGET_TABLE --strategy column --column CREATED_DT --chunks 64
```

This connects to the source database, discovers the actual value boundaries for `CREATED_DT`, and writes 64 lines to `queryfile.txt` — one SQL per slice, one target table per line.

**Optional: interleave for better concurrency**

By default, slices are written in boundary order. To avoid all concurrent apps hitting the same source partition or target rows at once, interleave the file:

```bash
./manage.sh split ... --assort    # interleave at split time (recommended)
# or separately:
python make_assorted_queryfile.py --input queryfile.txt --output queryfile-assorted.txt
```

`--assort` reorders lines so each wave of `CONCURRENT_APPS_MAX` apps is a varied mix, reducing hot-block contention and per-table write throttling.

### Step 9 — Run the load

```bash
python main.py
```

On first run, the loader:

1. Reads `queryfile.txt` and registers all slices in the state backend as `NEW`
2. Captures the source CDC watermark (Oracle SCN / PostgreSQL WAL LSN / SQL Server LSN) — **note this value** before starting your CDC pipeline downstream
3. Enters the orchestration loop: deploys up to `CONCURRENT_APPS_MAX` Striim apps simultaneously, monitors for completion, and starts new slices as each finishes

The loop runs until every slice reaches `COMPLETED` or `FAILED`, then exits.

**Resuming an interrupted run:** keep `UNIQUE_RUN_ID` unchanged in `config.py` and re-run `python main.py`. The loader picks up where it left off — slices already `COMPLETED` are skipped.

**Windows:** use `python main.py` directly (no bash wrapper needed for `main.py`).

### Step 10 — Monitor

Open a second terminal and launch the live status board:

```bash
./manage.sh         # interactive menu → option 11 (Live status board)
```

The board auto-refreshes every 5 seconds and shows:
- Run progress: percentage complete, slice counts by status, progress bar
- In-flight apps: slice number, target table, namespace, elapsed time, live rows/sec (when Striim is reachable)
- Recent completions
- Stage-file watch: apps spinning up (`+ appeared`) and finishing (`- finished`) in real time
- Log tail

For a quick text check without the board:

```bash
./manage.sh status
./manage.sh status --failed    # show only failed slices
```

> **Windows:** the full-screen board may render with broken formatting in Command Prompt or PowerShell. Use WSL for the best experience, or use `python manage.py board --json` for the raw JSON feed.

### Step 11 — Reconcile (completeness gate)

When the run reports `FINISHED`, it means no slice reported `FAILED` — it trusts Striim's COMPLETED/quiesced signal. Run reconcile to confirm that rows actually landed:

```bash
./manage.sh reconcile
# Windows:
python manage.py reconcile --run-id 100
```

Reconcile counts source rows `AS OF SCN <watermark>` per stored slice boundary. Any slice that quiesced on a partial read is caught here. The command also emits the corresponding `SELECT COUNT(*)` SQL for each target table — run those against your target to complete the row-count check.

---

## 5. Configuration Reference

All settings live in `config.py`. Credentials always come from environment variables.

### Core settings

| Setting | Default | Env var override | Description |
|---|---|---|---|
| `QUERY_FILE` | `queryfile.txt` | — | Input slice file. Point at `queryfile-assorted.txt` for the interleaved version. |
| `UNIQUE_RUN_ID` | `100` | — | Identifies this load session. Keep unchanged to resume; change to start fresh (history preserved). |
| `CONCURRENT_APPS_MAX` | `5` | — | Maximum Striim apps in flight simultaneously. |
| `APP_MONITOR_INTERVAL_SECONDS` | `15` (DEV) / `60` (PROD) | — | How often to poll app status. Not below 15s. |
| `DEPLOY_WAIT_TIME_SECONDS` | `20` (DEV) / `120` (PROD) | — | Minimum pause between deploying successive apps. |
| `STAGE_DB_LOCATION` | `TinyDB` | — | State backend: `TinyDB`, `BQ`, `PG`, or `ORACLE`. |
| `SLICE_MAX_RUNTIME_SECONDS` | `0` (off) | `SLICE_MAX_RUNTIME_SECONDS` | Stall timeout. A slice RUNNING past this is force-FAILED. `0` = disabled. |
| `DEPLOYMENT_GROUP_TARGET` | `default` | — | Striim deployment group for all generated apps. |
| `ILA_APP_NAME_BASE` | `OracleInitialLoadApp` | — | App name inside each namespace — must match the TQL template. |
| `SOURCE_TQL_FILE` | `admin.SW.tql` | — | TQL template filename. |
| `LOG_OUTPUT_PATH` | `logging/striimautoloader.log` | — | Log file; sidecars (watermark, probe recommendation) land in the same directory. |
| `ENV` | `DEV` | `ENV` | `DEV` or `PROD`. PROD uses HTTPS defaults and longer timing values. |
| `SOURCE_DB_TYPE` | `oracle` | `SOURCE_DB_TYPE` | Source engine: `oracle` (default), `postgres`, `sqlserver`, or `jdbc` — all implemented. Override per run with `--source-engine` on `probe`/`split`. |

### Striim connection (environment variables)

| Variable | Default | Description |
|---|---|---|
| `STRIIM_NODE` | `localhost:9080` | Striim host and port |
| `STRIIM_URL_PREFIX` | `http://` (DEV) / `https://` (PROD) | Protocol prefix |
| `STRIIM_ADMIN_USER` | `admin` | Striim admin username (use OR token, not both) |
| `STRIIM_ADMIN_PWD` | _(empty)_ | Striim admin password |
| `STRIIM_API_TOKEN` | _(empty)_ | Pre-issued API token; used instead of user/password if set |

### Oracle source (for probe, split, watermark)

| Variable | Default | Description |
|---|---|---|
| `ORACLE_DSN` | _(empty)_ | Full DSN, e.g. `host:1521/SERVICE` (use OR host+service) |
| `ORACLE_HOST` | _(empty)_ | Oracle hostname (alternative to DSN) |
| `ORACLE_SERVICE` | _(empty)_ | Oracle service name (alternative to DSN) |
| `ORACLE_PORT` | `1521` | Oracle port |
| `ORACLE_USER` | _(empty)_ | Oracle username |
| `ORACLE_PASSWORD` | _(empty)_ | Oracle password |

### Source engine selection (`SOURCE_DB_TYPE` / `TARGET_DB_TYPE`)

| Setting | Default | Env var override | Description |
|---|---|---|---|
| `SOURCE_DB_TYPE` | `oracle` | `SOURCE_DB_TYPE` | Active source engine: `oracle`, `postgres`, `sqlserver`, or `jdbc`. Overridden per-invocation by `--source-engine` on `probe`/`split`. |
| `TARGET_DB_TYPE` | _(= `SOURCE_DB_TYPE`)_ | `TARGET_DB_TYPE` | Target engine for the generated TQL; defaults to the source engine for homogeneous loads. |

> The `probe` and `split` commands also accept `--source-engine {oracle,postgres,sqlserver,jdbc}` to override `SOURCE_DB_TYPE` for a single run (default = `config.SOURCE_DB_TYPE`).

### PostgreSQL source (`SOURCE_DB_TYPE = 'postgres'`)

> **Distinct from the `PG_*` state-backend vars.** These `SOURCE_PG_*` variables configure the PostgreSQL database the **probe/split reads from**. The `PG_*` block (below) configures the PostgreSQL **state/orchestration backend** — they are independent and may point at different servers.

| Variable | Default | Description |
|---|---|---|
| `SOURCE_PG_HOST` | _(empty)_ | Source PostgreSQL hostname |
| `SOURCE_PG_PORT` | `5432` | Source PostgreSQL port |
| `SOURCE_PG_DATABASE` | _(empty)_ | Source database name |
| `SOURCE_PG_USER` | _(empty)_ | Source PostgreSQL username |
| `SOURCE_PG_PASSWORD` | _(empty)_ | Source PostgreSQL password |
| `SOURCE_PG_SSLMODE` | `prefer` | SSL mode; use `require` for managed/cloud PG |

### SQL Server source (`SOURCE_DB_TYPE = 'sqlserver'`)

| Variable | Default | Description |
|---|---|---|
| `SQLSERVER_HOST` | _(empty)_ | SQL Server hostname |
| `SQLSERVER_PORT` | `1433` | SQL Server port |
| `SQLSERVER_DATABASE` | _(empty)_ | Database name |
| `SQLSERVER_USER` | _(empty)_ | SQL Server username |
| `SQLSERVER_PASSWORD` | _(empty)_ | SQL Server password |
| `SQLSERVER_DRIVER` | `ODBC Driver 18 for SQL Server` | ODBC driver name `pyodbc` connects through (must be installed on the host) |

### Generic JDBC source (`SOURCE_DB_TYPE = 'jdbc'`)

| Variable | Default | Description |
|---|---|---|
| `JDBC_DRIVER_CLASS` | _(empty)_ | Fully-qualified JDBC driver class, e.g. `com.mysql.cj.jdbc.Driver` |
| `JDBC_URL` | _(empty)_ | Full JDBC URL, e.g. `jdbc:mysql://host:3306/db` |
| `JDBC_JAR_PATH` | _(empty)_ | Path to the vendor JDBC `.jar` |
| `JDBC_USER` | _(empty)_ | JDBC username |
| `JDBC_PASSWORD` | _(empty)_ | JDBC password |
| `JDBC_ROW_LIMIT_SYNTAX` | `rownum` | Row-limit dialect for bounded probe sampling (`rownum`, `limit`, `top`, `fetch`) |
| `JDBC_WATERMARK_SQL` | _(empty)_ | Optional SQL returning a CDC watermark value; blank disables watermark capture |

### Striim provider-type overrides (environment variables)

The generated TQL sets `DatabaseProviderType` on Striim's `DatabaseReader`/`DatabaseWriter` from `SOURCE_DB_TYPE`/`TARGET_DB_TYPE`. Override the mapping when Striim expects a provider string that differs from the engine name:

| Variable | Default | Description |
|---|---|---|
| `STRIIM_SOURCE_PROVIDER_TYPE` | _(derived from `SOURCE_DB_TYPE`)_ | Forces the source `DatabaseProviderType` string in the TQL |
| `STRIIM_TARGET_PROVIDER_TYPE` | _(derived from `TARGET_DB_TYPE`)_ | Forces the target `DatabaseProviderType` string in the TQL |

### PostgreSQL state backend (`STAGE_DB_LOCATION = 'PG'`)

| Variable | Default | Description |
|---|---|---|
| `PG_HOST` | `localhost` | PostgreSQL hostname |
| `PG_DATABASE` | _(empty)_ | Database name |
| `PG_USER` | _(empty)_ | PostgreSQL username |
| `PG_PASSWORD` | _(empty)_ | PostgreSQL password |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_TABLE_ID` | `striim_orchestration` | Orchestration table name |
| `PG_SSLMODE` | `prefer` | SSL mode; use `require` for RDS, Cloud SQL, or managed PG |

The orchestration table is created automatically on first run. Reference DDL: `PG_TableCreate.sql`.

### BigQuery state backend (`STAGE_DB_LOCATION = 'BQ'`)

| Variable | Default | Description |
|---|---|---|
| `BQ_KEYFILE_LOCATION` | _(empty)_ | Path to service account JSON keyfile |
| `BQ_PROJECT_ID` | _(empty)_ | GCP project ID |
| `BQ_DATASET_ID` | _(empty)_ | BigQuery dataset name |
| `BQ_TABLE_ID` | `striim_orchestration` | BigQuery table name |

The orchestration table must be created manually before first use. Reference DDL: `BQ_TableCreate.sql`.

### Oracle state backend (`STAGE_DB_LOCATION = 'ORACLE'`)

This is **independent of the source Oracle connection** above — it may point at the same Oracle instance but is configured separately.

| Variable | Default | Description |
|---|---|---|
| `ORCH_ORACLE_DSN` | _(empty)_ | Full DSN (use OR host+service) |
| `ORCH_ORACLE_HOST` | _(empty)_ | Oracle hostname |
| `ORCH_ORACLE_SERVICE` | _(empty)_ | Oracle service name |
| `ORCH_ORACLE_PORT` | `1521` | Oracle port |
| `ORCH_ORACLE_USER` | _(empty)_ | Oracle username |
| `ORCH_ORACLE_PASSWORD` | _(empty)_ | Oracle password |
| `ORCH_ORACLE_TABLE_ID` | `striim_orchestration` | Orchestration table name |

The orchestration table is auto-created on first run. Reference DDL: `ORA_TableCreate.sql`.

> **Two independent Oracle configs:** `ORACLE_*` is the read-only source account used by the splitter, probe, and watermark. `ORCH_ORACLE_*` is the state account used by the orchestration backend. Both must be set for a column-split run with Oracle state storage.

### Probe tunables

All of these also read from a matching environment variable of the same name.

| Setting | Default | Description |
|---|---|---|
| `PROBE_DEPTH_DEFAULT` | `bakeoff` | Default probe depth: `lightweight`, `bakeoff`, or `adaptive` |
| `PROBE_TARGET_SLICE_SECONDS` | `600` | Desired per-slice runtime in seconds; drives chunk count math |
| `PROBE_SAMPLE_ROWS` | `100000` | Max rows fetched per timing probe (bounded-sample cap) |
| `PROBE_TIME_BUDGET_SECONDS` | `20` | Wall-clock cap per timing probe |
| `PROBE_MAX_CONCURRENCY` | `32` | Ceiling for the adaptive concurrency ramp |

### Live board tunables

| Setting | Default | Description |
|---|---|---|
| `BOARD_REFRESH_SECONDS` | `5` | Refresh interval for TinyDB and PG backends |
| `BOARD_REFRESH_SECONDS_BQ` | `30` | Refresh interval for BigQuery (slower to avoid billable-query spam) |

---

## 6. Management CLI Reference

`manage.sh` is the operator console. Run with no arguments for the interactive menu, or call a subcommand directly:

```bash
./manage.sh <subcommand> [flags]
# Windows:
python manage.py <subcommand> [flags]
```

All subcommands act on `UNIQUE_RUN_ID` from `config.py` unless `--run-id N` is provided.

### Windows / cross-platform

The Python entry points are fully cross-platform — `python manage.py <subcommand>` (and `python main.py`) run natively in **cmd** or **PowerShell**, with no shell required. The Oracle driver uses thin mode (no Instant Client), and `manage.py board` emits plain JSON, so monitoring works anywhere.

The interactive `manage.sh` TUI is a **bash** script. On Windows, run it under **WSL** or **Git Bash**; native cmd/PowerShell cannot execute it. You lose nothing by skipping it — every menu action maps to a `python manage.py …` subcommand you can call directly.

### `setup`

Validate configuration, test connectivity, and create or verify the state table.

```bash
./manage.sh setup
./manage.sh setup --backend PG    # test a specific backend without changing config.py
```

Run this before your first load. It catches mis-configured variables and permission problems before the load starts.

### `status`

Show run progress: counts by status, in-flight slices, failed slices.

```bash
./manage.sh status
./manage.sh status --all-runs     # every run in the state backend
./manage.sh status --failed       # only FAILED slices
./manage.sh status --json         # machine-readable JSON
./manage.sh status --rows         # individual slice rows
```

### `probe`

Race candidate split predicates and print the recommendation. Does not modify data or write any files.

```bash
./manage.sh probe --query-file my_query.sql --table SCHEMA.TABLE
./manage.sh probe --query-file my_query.sql --table SCHEMA.TABLE --depth bakeoff
./manage.sh probe --query-file my_query.sql --table SCHEMA.TABLE --depth adaptive
./manage.sh probe --query-file my_query.sql --table SCHEMA.TABLE --source-engine postgres
```

| Flag | Default | Description |
|---|---|---|
| `--query-file FILE` / `--query SQL` | — | The SQL containing `~SPLIT~` |
| `--table OWNER.TABLE` | required | Driving / splitting table |
| `--depth lightweight\|bakeoff\|adaptive` | `bakeoff` | Probe depth tier |
| `--alias` | auto-detect | Driving-table alias for predicate qualification |
| `--target-slice-seconds N` | `600` | Desired per-slice runtime; drives chunk count |
| `--sample-rows N` | `100000` | Bounded-sample cap per timing probe |
| `--time-budget-seconds N` | `20` | Wall-clock cap per timing probe |
| `--max-concurrency N` | `32` | Ceiling for adaptive concurrency ramp |
| `--source-engine oracle\|postgres\|sqlserver\|jdbc` | `config.SOURCE_DB_TYPE` | Source engine to probe (overrides `SOURCE_DB_TYPE` for this run) |

### `split`

Generate `queryfile.txt` by injecting disjoint slice predicates into your query.

```bash
./manage.sh split \
  --query-file my_query.sql \
  --table SCHEMA.SOURCE_TABLE \
  --target SCHEMA.TARGET_TABLE \
  --strategy column \
  --column CREATED_DT \
  --chunks 64
```

| Flag | Default | Description |
|---|---|---|
| `--query-file FILE` / `--query SQL` | — | Source query |
| `--table OWNER.TABLE` | required | Driving table |
| `--target OWNER.TABLE` | required | Target table (written to each queryfile line) |
| `--strategy auto\|rowid\|column\|partition` | `auto` | Split strategy. `auto` uses partition if available, else ROWID. |
| `--column COL` | — | Required for `--strategy column` |
| `--chunks N` | — | Number of slices to generate |
| `--alias` | auto-detect | Driving-table alias |
| `--subpartitions` | off | One slice per subpartition (partition strategy only) |
| `--assort` | off | Interleave slices for better concurrency |
| `--explain` | off | Print the EXPLAIN access path for verification |
| `--source-engine oracle\|postgres\|sqlserver\|jdbc` | `config.SOURCE_DB_TYPE` | Source engine to split against (overrides `SOURCE_DB_TYPE` for this run) |

**Engine picker.** Both `probe` and `split` read from the engine named by `SOURCE_DB_TYPE`. Override it per run with `--source-engine`, or set it once for the whole session:

```bash
python manage.py probe --query-file my_query.sql --table OWNER.TABLE --source-engine postgres
python manage.py split --query-file my_query.sql --table OWNER.SRC --target OWNER.TGT \
  --strategy column --column CREATED_DT --chunks 64 --source-engine sqlserver

# …or set it once (no flag needed thereafter):
export SOURCE_DB_TYPE=postgres
```

The interactive `manage.sh` split/probe wizards prompt for the engine up front (Oracle = default) and append `--source-engine` automatically when you pick a non-Oracle engine.

### `reset`

Re-queue `FAILED` slices to `NEW` so the next `python main.py` retries them.

```bash
./manage.sh reset
./manage.sh reset --include-faileddrop      # also re-queue COMPLETED-FAILEDDROP slices
./manage.sh reset --with-striim-cleanup     # stop/undeploy/drop leftover ILA_ apps first
```

### `clear`

Retire a run from the active view (`iscurrentrow = FALSE`). Run history is preserved.

```bash
./manage.sh clear
./manage.sh clear --hard --yes              # hard-delete the run's rows
./manage.sh clear --with-striim-cleanup     # also clean up leftover ILA_ apps
```

### `logs`

Tail the loader log file.

```bash
./manage.sh logs
./manage.sh logs --lines 200
./manage.sh logs --follow                   # stream new lines (like tail -f)
./manage.sh logs --errors                   # show only ERROR lines
```

### `board`

Emit the live-board JSON data feed (consumed by the interactive board in menu option 11).

```bash
./manage.sh board --json
```

### `reconcile`

Run the SCN-anchored completeness check after the load finishes.

```bash
./manage.sh reconcile
./manage.sh reconcile --run-id 100
```

Counts source rows `AS OF SCN <watermark>` per slice. Emits `SELECT COUNT(*)` SQL for each target table for you to run manually.

---

## 7. Probe Deep-Dive

The probe answers: *for this specific query on this specific table, what is the fastest disjoint split strategy, how many chunks should I use, and how many should run in parallel?*

### What it measures

For each candidate strategy, the probe builds one representative slice (the first range) injected into your exact query, then:

1. **EXPLAIN** — captures the driving access path and flags any large inner table that gets full-scanned (join or JSON amplification)
2. **Bounded timing fetch** — executes that one slice and measures rows/sec and MB/s

### Bounded-sampling guarantee

The probe never full-scans the real table. Every timing query is wrapped with an engine-specific row limit:
- Oracle: `SELECT * FROM (<slice>) WHERE ROWNUM <= n`
- PostgreSQL: `SELECT * FROM (<slice>) LIMIT n`
- SQL Server: `SELECT TOP (n) * FROM (<slice>)`
- Plus a wall-clock `--time-budget-seconds` backstop (all engines)

No unbounded `COUNT(*)`, no `NTILE` over the full table. Column range boundaries come from a small `SAMPLE` clause. Safe to run against trillion-row tables.

### Candidates

| Candidate | Engine | Discovery source | Notes |
|---|---|---|---|
| **Physical** | Oracle: `ROWID` · PostgreSQL: `ctid` · SQL Server: clustered-key range | Always present | Cheapest disjoint access; no index dependency; stable boundaries. Predicate syntax is engine-specific. |
| **Column range** | All supported engines | Indexed NUMBER / FLOAT / DATE / TIMESTAMP columns with `num_distinct > 1` | Top 3 by (indexed flag, NDV); equi-depth boundaries from a sample |
| **Partition** | Engines with native partitioning | Reported when the table is partitioned | Deterministic — not raced; appears as a note only |

### Ranking

Candidates are ranked by measured MB/s, restricted to plans that are disjoint and non-amplifying. AMPLIFYING candidates — those that full-scan an inner table once per slice — are excluded from the ranking but printed with their verdict. Ties break toward physical (ROWID).

### Chunk count math

```
slice_bytes  = target_slice_seconds × winner_MB_per_sec
chunk_count  = ceil(segment_bytes / slice_bytes)
```

Each slice is sized to run for roughly `--target-slice-seconds` at the measured throughput. Segment size is engine-specific: Oracle uses `dba_segments` (falling back to `all_tables.blocks × 8K`); PostgreSQL uses `pg_relation_size`; SQL Server uses `sys.dm_db_partition_stats`.

### Depth tiers

| Depth | What it does |
|---|---|
| `lightweight` | Times one physical (ROWID) slice → chunk count only; keeps configured concurrency |
| `bakeoff` *(default)* | Races all candidates; picks the fastest disjoint strategy + chunk count |
| `adaptive` | bakeoff + concurrency-knee ramp (k = 1, 2, 4, … up to `--max-concurrency`; stops at the plateau) + full amplification audit |

Use `bakeoff` for most loads. Use `adaptive` when you want the probe to also recommend a concurrency value, or when your cluster's throughput plateaus at a specific parallelism level.

### Reading the recommendation panel

```
================================================================
PROBE RECOMMENDATION
================================================================
  strategy:        column
  key:             CREATED_DT
  chunk_count:     64
  concurrency:     8
  winner_MB_per_sec: 41.20
----------------------------------------------------------------
  label          kind       MB/s  access_path               verdict
  ROWID          physical  28.10  TABLE ACCESS BY ROWID R…  good
  CREATED_DT     column    41.20  INDEX RANGE SCAN          good
  ENTITY_ID      column     3.40  TABLE ACCESS FULL         AMPLIFYING
----------------------------------------------------------------
  partition: table IS partitioned — consider --strategy partition
  warnings:
    - AMPLIFYING: candidate 'ENTITY_ID' full-scans PAY.SUBMSN_RESP (join amplification)
================================================================
```

- `strategy` / `key` / `chunk_count` / `concurrency` → pass directly to `split`
- `concurrency: keep configured` (bakeoff/lightweight) means leave `CONCURRENT_APPS_MAX` as-is
- AMPLIFYING candidates full-scan an inner table per slice — they scale the source database load linearly with chunk count and are excluded from ranking
- Partition note appears when the table is partitioned; that path is deterministic and needs no probe
- Full per-candidate report also in the log file, greppable and reproducible

### Recommendation sidecar

The recommendation is written to `logging/probe_recommendation.json`:

```json
{ "strategy": "column", "key": "CREATED_DT", "chunk_count": 64,
  "concurrency": 8, "table": "SCHEMA.TABLE",
  "query_file": "my_query.sql", "depth": "bakeoff" }
```

The interactive wizard reads this file on "apply" to prefill the split step.

---

## 8. Split Predicates Deep-Dive

### Three strategies

| Strategy | Predicate injected | Best for |
|---|---|---|
| **physical** (`rowid`) | `alias.ROWID BETWEEN 'lo' AND 'hi'` (first slice `ROWID <= 'hi'`, last slice `ROWID >= 'lo'` — see below) | Default — works on any table; no index needed; cheapest disjoint access path |
| **column** | `alias.col >= lo AND alias.col < hi` (half-open) | An indexed high-NDV column that lets the source engine use an index range scan or partition pruning — often wins on tables with joins or `JSON_TABLE` |
| **partition** | Engine-specific, one slice per partition: Oracle rewrites the table reference to `OWNER.TABLE PARTITION (P)`; PostgreSQL swaps the parent for the child relation (`FROM schema.child`); SQL Server injects a `$PARTITION.fn(col) = N` predicate | Already-partitioned table — deterministic, no measurement needed; add `--subpartitions` for one slice per subpartition (Oracle) |

### The `~SPLIT~` token and alias rule

Write your query with `~SPLIT~` where the predicate belongs and alias the driving table:

```sql
SELECT s.A, s.B
FROM PAY.CM_FB_SUBMISSION s
JOIN PAY.CM_FB_SUBMSN_RESP r ON r.T = s.T
WHERE s.ENTITY_TYPE = 'CASE'
AND ~SPLIT~
```

- Pass `--alias s` to `split` and `probe` (or let it auto-detect from the query)
- The predicate is alias-qualified (e.g. Oracle: `s.ROWID BETWEEN … AND …`) — so it binds to the driving table, not the joined one
- If `~SPLIT~` is absent, the splitter appends `AND <pred>` / `WHERE <pred>` instead; a marker is more reliable

### Column range details

- Column must be indexed, numeric/date/timestamp, and have `num_distinct > 1`
- Boundaries are **half-open** (`>= lo AND < hi`) — prevents double-counting rows that fall on a boundary value
- The probe discovers and ranks the top-3 candidates automatically; pass the winner to `split --column`

### Physical chunking methods *(Oracle)*

For Oracle, the splitter auto-picks the best available ROWID-chunking method in order of preference:

1. **`DBMS_PARALLEL_EXECUTE`** — Oracle's official block-range API; most precise; requires `EXECUTE` privilege
2. **`dba_extents`** — extent-level ranges; requires `SELECT` on `DBA_EXTENTS`
3. **`NTILE`** — pure-SQL fallback using row-number bucketing; requires only a read account; slightly less efficient but functionally equivalent

All three produce disjoint, non-overlapping ROWID slices. Other engines use equivalent mechanisms (PostgreSQL: `pg_class.relpages` block ranges; SQL Server: clustered-index / partition stats).

**Open tails (completeness).** The first and last ROWID slices are rendered with an open bound — the first as `ROWID <= 'hi'` (no lower bound) and the last as `ROWID >= 'lo'` (no upper bound); a single-slice table becomes `1=1` (whole table). Middle slices keep the closed `BETWEEN`. This guarantees that any row in a block allocated below the minimum or above the maximum boundary — for example a row inserted between chunking and the read — is still captured by initial load rather than falling through a gap. Because the ranges are disjoint and contiguous, opening the tails adds no double-counting.

### Interleaving slices

By default, slices are written in sequential order (first chunk, second chunk, …). If you have many slices and `CONCURRENT_APPS_MAX` apps in flight, the first wave will hit the same range of data simultaneously — which can create hot blocks or write contention on the target.

Use `--assort` (or `make_assorted_queryfile.py`) to reorder the slices so each concurrent wave is a spread across the full data range. The script prints a wave-by-wave summary so you can verify the result.

---

## 9. State Backends

The orchestrator tracks every slice's status in a configurable backend. All backends use the same schema.

| Backend | `STAGE_DB_LOCATION` | When to use |
|---|---|---|
| **TinyDB** | `TinyDB` (default) | Development and single-machine runs; zero setup; stored as a local JSON file |
| **PostgreSQL** | `PG` | Production and on-premises; durable, queryable, safe for concurrent access |
| **BigQuery** | `BQ` | Cloud runs; shareable history; use a slower refresh rate to avoid billable-query costs |
| **Oracle** | `ORACLE` | Production when Oracle is already the state store of record |

> **TinyDB note:** TinyDB is a local JSON file. It is not safe for concurrent writes and is susceptible to partial-read corruption if the process is killed mid-write. For scale or production runs, use PostgreSQL or Oracle.

### Querying state directly

**PostgreSQL:**

```sql
-- Progress summary
SELECT status, COUNT(*) AS cnt
FROM striim_orchestration
WHERE iscurrentrow = TRUE AND uniquerunid = 100
GROUP BY status ORDER BY status;

-- In-flight slices
SELECT appname, targettbl, status, started_datetime, notes
FROM striim_orchestration
WHERE iscurrentrow = TRUE AND uniquerunid = 100
  AND status NOT IN ('COMPLETED', 'FAILED', 'COMPLETED-FAILEDDROP')
ORDER BY roworder;
```

**BigQuery:**

```sql
SELECT status, COUNT(*) AS cnt
FROM `your_project.your_dataset.striim_orchestration`
WHERE iscurrentrow = TRUE AND uniquerunid = 100
GROUP BY status ORDER BY status;
```

**Oracle:**

```sql
SELECT status, COUNT(*) AS cnt
FROM striim_orchestration
WHERE iscurrentrow = 1 AND uniquerunid = 100
GROUP BY status ORDER BY status;
```

`iscurrentrow` is set to `FALSE` when a run completes or is cleared. History from multiple `UNIQUE_RUN_ID` values accumulates in the same table without collision.

---

## 10. Failure Handling and Recovery

### What happens when a slice fails — and why target PKs are mandatory

Striim's `DatabaseWriter` commits rows to the target in batches as it reads from the source. There is no distributed transaction between source read and target write: once a batch commits to the target, it is permanent.

If a slice fails midway — whether due to a Striim error, network drop, Oracle timeout, or stall timeout — the rows already committed to the target remain. There is no automatic rollback.

When you reset and re-run the failed slice, Striim re-reads the full slice from scratch. The TQL template handles the resulting duplicate inserts with:

```
IgnorableExceptionCode: 'DUPLICATE_ROW_EXISTS'
```

**This suppression only works if the target table has a primary key or unique constraint.** Without a PK:

- `DUPLICATE_ROW_EXISTS` is never raised
- The `DatabaseWriter` inserts every row again
- The target silently accumulates duplicate rows
- No error is reported; the re-run appears successful

**Always define a primary key on the target table before running the load.**

### Slice status values

| Status | Meaning |
|---|---|
| *(blank / NEW)* | Registered; not yet dispatched |
| `RUNNING` | App created, deployed, and started in Striim |
| `COMPLETED` | App quiesced; undeploy and namespace drop succeeded |
| `COMPLETED-FAILEDDROP` | App completed (data transferred) but undeploy/drop failed; manual cleanup needed |
| `FAILED` | Error at create, deploy, start, or stall timeout; see `notes` column in state table |

### Recovering from failures

**Re-queue failed slices and retry:**

```bash
./manage.sh reset
python main.py
```

**Include COMPLETED-FAILEDDROP (data transferred; re-run cleanup only):**

```bash
./manage.sh reset --include-faileddrop
python main.py
```

**With Striim cleanup (stop/undeploy/drop leftover ILA_ apps before reset):**

```bash
./manage.sh reset --with-striim-cleanup
python main.py
```

### Fresh-run guard

If the state backend shows zero rows for a run ID that previously had rows (e.g. you wiped the state manually), the loader refuses to start — preventing an accidental duplicate full load. Override when you genuinely want a fresh start:

```bash
python main.py --force-fresh
# or:
FORCE_FRESH=1 python main.py
```

The fresh-run marker is deleted automatically on clean completion and by `manage.py clear`.

### Run lock

A second concurrent `python main.py` on the same `UNIQUE_RUN_ID` is refused (PID + start-time lock file at `logging/run_<id>.lock`). If a prior run crashed without releasing the lock:

```bash
rm logging/run_100.lock      # Mac / Linux
del logging\run_100.lock     # Windows
```

### Stall detection

`SLICE_MAX_RUNTIME_SECONDS` (default `0` = disabled) force-FAILs any slice that has been `RUNNING` longer than the configured number of seconds. Useful as a safety net:

```bash
export SLICE_MAX_RUNTIME_SECONDS=7200   # fail slices stuck longer than 2 hours
python main.py
```

Stalled slices appear in `./manage.sh status --failed` with a `[stalled > Ns]` note.

### Completeness: FINISHED vs. reconcile

A green `FINISHED` at the end of the run means no slice reported `FAILED`. It does **not** prove every row landed — a slice that quiesces on a partial read still shows `COMPLETED`. `manage.py reconcile` is the completeness gate: it counts source rows `AS OF SCN <watermark>` per slice so a falsely-COMPLETED slice is caught.

---

## 11. Cleanup

### After a successful run

The run is automatically cleared from the active view (`iscurrentrow = FALSE`) when all slices complete. Generated TQL files in `stage/` are removed. Log files and sidecars remain.

Manual cleanup of logs and sidecars:

```bash
# Mac / Linux
rm logging/*.json logging/*.log
rm stage/*.tql

# Windows
del logging\*.json logging\*.log
del stage\*.tql
```

### Retire a run (keep history)

```bash
./manage.sh clear       # sets iscurrentrow = FALSE; history preserved
```

Direct SQL:

```sql
-- PostgreSQL:
UPDATE striim_orchestration SET iscurrentrow = FALSE
WHERE iscurrentrow = TRUE AND uniquerunid = 100;

-- BigQuery:
UPDATE `your_project.your_dataset.striim_orchestration`
SET iscurrentrow = FALSE
WHERE iscurrentrow = TRUE AND uniquerunid = 100;

-- Oracle:
UPDATE striim_orchestration SET iscurrentrow = 0
WHERE iscurrentrow = 1 AND uniquerunid = 100;
COMMIT;
```

### Hard-delete a run

```bash
./manage.sh clear --hard --yes
# or full reset:
# TRUNCATE TABLE striim_orchestration;
```

### Clean up leftover Striim apps and namespaces

If a run was interrupted and apps remain in Striim:

```bash
./manage.sh clear --with-striim-cleanup
# or:
./manage.sh reset --with-striim-cleanup
```

To clean up a specific app manually via TQL:

```tql
STOP APPLICATION ILA_100_1.OracleInitialLoadApp;
UNDEPLOY APPLICATION ILA_100_1.OracleInitialLoadApp;
DROP APPLICATION ILA_100_1.OracleInitialLoadApp CASCADE;
DROP NAMESPACE ILA_100_1 CASCADE;
```

---

## 12. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Authentication failed. Check STRIIM_NODE and credentials.` | Wrong `STRIIM_NODE`, `STRIIM_ADMIN_USER`, or `STRIIM_ADMIN_PWD`; Striim unreachable | Verify env vars; confirm Striim is reachable on that host + port |
| Slices fail at CREATE with `failureMessage` | TQL template error or property variables not defined in Striim | Run `./manage.sh setup`; paste `pvs.tql` contents into Striim console; check `admin.SW.tql` |
| All slices stay `RUNNING` indefinitely, never `COMPLETED` | `QuiesceOnILCompletion: true` missing from DatabaseReader | Confirm `admin.SW.tql` has `QuiesceOnILCompletion: true` in the DatabaseReader block |
| Duplicate rows on target after reset + re-run | Target table has no primary key | Add a PK or unique constraint on the natural key before the next run |
| `DPI-1047` or `python-oracledb` not found | Oracle driver not installed | `pip install python-oracledb`; verify Oracle Instant Client is present if required |
| Probe returns empty column candidates | No indexed high-NDV columns on driving table | Use `--strategy rowid` or add an appropriate index |
| `✗ --table must be OWNER.TABLE, got 'CM_CASES'` | `--table`/`--target` given without an owner, or with spaces/typos | Pass the fully-qualified `OWNER.TABLE` (e.g. `PAY.CM_CASES`). In the wizard just re-enter it |
| `✗ invalid alias identifier` / `invalid column identifier` | `--alias`/`--column` contains characters outside `A-Z 0-9 _ $ #` (a typo, or an injection attempt) | Use a plain identifier; a column may be `alias.col` |
| `✗ target must be non-empty and must not contain '\|'` | `--target` was empty or contained the `\|` queryfile delimiter | Supply a plain `OWNER.TABLE`; don't put `\|TARGET` in the `--query-file` template |
| `argument --chunks: must be a positive integer (>= 1)` | A count/duration flag (`--chunks`, `--sample-rows`, `--target-slice-seconds`, `--max-concurrency`, `--lines`) was `0`, negative, or non-numeric | Pass a positive integer. Prevents the divide-by-zero and degenerate single-slice results a bad value used to cause |
| `ValueError: PG_PORT must be an integer, got 'abc'` at startup | A numeric env var / `.env` value (port or probe tunable) is non-numeric or below its minimum | Fix the value in your environment or `.env` |
| Every probe/split slice fails right after connecting | A `\|TARGET` suffix was left on the `--query-file` template, so the injected SQL ends `… WHERE <pred>\|TARGET` | Remove the `\|TARGET` from the template; pass the target with `split --target` (see Step 6) |
| `REFUSE: run had N rows — use --force-fresh` | State backend was cleared externally while a run existed | Run `python main.py --force-fresh` if you intend a fresh start |
| Lock file present after crash | Prior run died without releasing lock | Remove `logging/run_<id>.lock` manually |
| Live board renders garbled on Windows | ANSI escape codes not supported by the terminal | Use WSL or run `python manage.py board --json` for the raw feed |
| `BigQuery setup verified` but status queries time out | BQ billing project or credentials issue | Check `BQ_KEYFILE_LOCATION`, `BQ_PROJECT_ID`, `BQ_DATASET_ID`; verify the service account has BigQuery Data Editor |
| `COMPLETED-FAILEDDROP` slices after run | Striim undeploy/drop failed (503, network blip) | Run `./manage.sh reset --include-faileddrop --with-striim-cleanup` then `python main.py` |

---

## 13. TQL Template and Namespace Scheme

### The TQL template (`admin.SW.tql`)

A new copy of the template is instantiated and deployed as a Striim app for every slice. It contains four placeholders, all substituted at generation time by `main.py`:

- **`~QUERYTEXT~`** — replaced with the slice SQL (the left side of the `|` in `queryfile.txt`)
- **`~TARGETTABLE~`** — replaced with the target table (the right side of the `|`)
- **`~PROVIDER_TYPE_SRC~`** — the source `DatabaseProviderType`, derived from `SOURCE_DB_TYPE` (override with `STRIIM_SOURCE_PROVIDER_TYPE`)
- **`~PROVIDER_TYPE_TGT~`** — the target `DatabaseProviderType`, derived from `TARGET_DB_TYPE` (override with `STRIIM_TARGET_PROVIDER_TYPE`)

**The template must reference Striim property variables for all credentials** — defined in Step 4 of the walkthrough. Never hardcode credentials in the template or in `queryfile.txt`.

Key settings in the default template:

| Setting | Value | Purpose |
|---|---|---|
| `DatabaseProviderType` | `~PROVIDER_TYPE_SRC~` / `~PROVIDER_TYPE_TGT~` | Source/target engine, substituted from `SOURCE_DB_TYPE` / `TARGET_DB_TYPE` at generation time |
| `QuiesceOnILCompletion` | `true` | DatabaseReader quiesces when the query is exhausted — required for slice completion detection |
| `IgnorableExceptionCode` | `DUPLICATE_ROW_EXISTS` | Suppresses PK violations on re-run — requires a target PK to function |
| `BatchPolicy` | `EventCount:100,Interval:10` | Target write batch size |
| `CommitPolicy` | `EventCount:100,Interval:10` | Target commit frequency |
| `ConnectionRetryPolicy` | `retryInterval=30,maxRetries=3` | Retry policy for target connection drops |

The template is engine-neutral: `DatabaseProviderType` is filled from `SOURCE_DB_TYPE` / `TARGET_DB_TYPE` at generation time, so the same template serves Oracle, PostgreSQL, and SQL Server. You still supply the matching `ConnectionURL` format through the `SourceConnectionString` / `TargetConnectionString` property variables (see Step 4), and may need to adjust adapter-specific settings for heterogeneous (cross-engine) loads.

### Namespace scheme

Each slice app is deployed into a dedicated Striim namespace:

```
ILA_{UNIQUE_RUN_ID}_{N}
```

For example: `ILA_100_1`, `ILA_100_2`, `ILA_100_3`, …

The namespace is:
- Created fresh before each app is deployed
- Dropped automatically after the app completes successfully
- Prefixed with `ILA_` for easy identification in the Striim UI

Multiple `UNIQUE_RUN_ID` values can coexist without namespace collision. The namespace is also the unit of cleanup — all leftover apps from a run can be found and dropped by prefix.

### Watermark sidecar

At fresh-run start, the tool writes a watermark sidecar to `logging/run_<id>_watermark.json`. The `label` and `value` fields are supplied by the active source dialect:

```json
{
  "run_id": 100,
  "label": "Oracle SCN",           // Oracle: SCN  |  PostgreSQL: WAL LSN  |  SQL Server: LSN
  "value": "0000000000123456789",   // engine-specific format
  "captured_at": "2026-06-27T14:03:11Z",
  "source_dsn": "your-host:1521/SERVICE"
}
```

Set your downstream CDC reader's start point to this value. The console banner also displays it:

```
================================================================
 INITIAL LOAD START WATERMARK [Oracle SCN]: 0000000000123456789
 captured 2026-06-27T14:03:11Z  (run 100)
 -> Set the downstream CDC reader's start point to this value.
================================================================
```

Watermark capture is best-effort: a missing source connection or privilege logs a warning and does **not** block the load. If the banner is absent, set the CDC start point manually.

---

## 14. File Inventory

| File | Purpose |
|---|---|
| `main.py` | Orchestrator — the only entry point you run to start or resume a load |
| `manage.sh` | Bash operator console: interactive menu and all CLI subcommands |
| `manage.py` | CLI engine backing `manage.sh` (`status` / `clear` / `reset` / `logs` / `split` / `probe` / `board` / `setup` / `reconcile`) |
| `config.py` | All tunable settings; credentials read from environment variables |
| `admin.SW.tql` | TQL application template; deployed as a new app for each slice |
| `pvs.tql` | Property variable definitions for Striim; run once per environment |
| `queryfile.txt` | Generated slice list (one line per slice: `SQL|TARGET_TABLE`) |
| `queryfile.txt.example` | Working example for the seeded `PAY.CM_FB_SUBMISSION` table |
| `data.py` | State-backend dispatch: routes to TinyDB / BigQuery / PostgreSQL / Oracle |
| `data_tinydb.py` | TinyDB backend implementation |
| `data_bq.py` | BigQuery backend implementation |
| `data_pg.py` | PostgreSQL backend implementation |
| `data_oracle.py` | Oracle state backend implementation |
| `models.py` | `QueryResult` data class shared across backends |
| `source_dialect.py` | `SourceDialect` interface + `get_dialect()` factory |
| `oracle_dialect.py` | Oracle provider: candidates, boundaries, EXPLAIN, sizing, SCN watermark |
| `oracle_boundaries.py` | ROWID and column boundary discovery |
| `oracle_client.py` | Read-only Oracle connection helper (lazy import) |
| `probe.py` | Engine-agnostic bakeoff: timing, ranking, chunk count math, recommendation panel |
| `query_split.py` | Predicate injection (`~SPLIT~` substitution) |
| `split_runner.py` | Queryfile generation driver |
| `watermark.py` | CDC watermark banner and sidecar persistence |
| `run_safety.py` | Fresh-run guard (marker + `decide_startup`), run lock, stall detection |
| `reconcile.py` | `final_verdict`, `reconcile_count_sql`, `is_snapshot_too_old` helpers |
| `striim_monitor.py` | Best-effort Striim per-app throughput helper for the live board |
| `make_assorted_queryfile.py` | Reorders `queryfile.txt` for concurrency interleaving |
| `seed_data.py` | Seeds the example `PAY.CM_FB_SUBMISSION` table for testing |
| `BQ_TableCreate.sql` | BigQuery orchestration table DDL (must be run manually before first BQ use) |
| `PG_TableCreate.sql` | PostgreSQL orchestration table DDL (reference; auto-created on first use) |
| `ORA_TableCreate.sql` | Oracle orchestration table DDL (reference; auto-created on first use) |
| `oracle_rowsplit.sql` | Manual SQL*Plus ROWID-split helper (legacy; see Appendix) |
| `requirements.txt` | Python dependencies (`pip install -r requirements.txt`) |
| `requirements-dev.txt` | Dev and test dependencies |

---

## 15. Source Engine Support

`SOURCE_DB_TYPE` (default `oracle`) selects the active source engine; **`oracle`, `postgres`, `sqlserver`, and `jdbc` are all implemented**. Override per run with `--source-engine` on `probe`/`split`. Drivers are lazy/optional — see the [driver matrix](#driver-matrix-all-source-engines).

### Engine capability matrix

| Capability | `oracle` | `postgres` | `sqlserver` | `jdbc` |
|---|---|---|---|---|
| Physical split (ROWID / ctid / clustered-index range) | yes | yes | yes | no |
| Column-range split | yes | yes | yes | yes |
| Partition split | yes | yes | yes | no |
| Native EXPLAIN (`--explain`) | yes | yes | yes | no |
| Probe chunk-count discovery | yes | yes | yes | pass explicit `--chunks` |
| Reconcile anchoring | snapshot (flashback SCN) | best-effort | live row counts | live row counts |

- **Postgres and SQL Server** support physical, column, and partition probing/splitting and native EXPLAIN — essentially feature-parity with Oracle.
- **JDBC is best-effort:** column-range splitting only — no physical/partition splitting and no native EXPLAIN. Because it cannot size itself from physical stats, pass an explicit `--chunks N` to `split`.
- **Reconcile** completeness is snapshot-anchored only on Oracle (flashback SCN gives a stable point-in-time count). PostgreSQL reconcile is best-effort; SQL Server and JDBC reconcile against **live** row counts, which can drift on an append-heavy source.

The same parallel-extract design maps each engine onto the shared `SourceDialect` interface:

| Concern | Oracle | PostgreSQL | SQL Server |
|---|---|---|---|
| Physical split key | `ROWID` block ranges | `ctid` ranges via `pg_class.relpages` | Clustered-index key range / partition |
| Boundary discovery | `DBMS_PARALLEL_EXECUTE` / `dba_extents` / `NTILE` | Block ranges from `relpages` | `sys.dm_db_partition_stats` |
| Partitions | `all_tab_partitions` | `pg_inherits` / declarative | `sys.partition_range_values` |
| CDC watermark | `current_scn` from `v$database` | `pg_current_wal_lsn()` | `sys.fn_cdc_get_max_lsn()` / `@@DBTS` |
| EXPLAIN | `EXPLAIN PLAN FOR` | `EXPLAIN (FORMAT JSON)` | `SET SHOWPLAN_XML ON` |
| Size estimate | `dba_segments.bytes` | `pg_relation_size` | `sys.dm_db_partition_stats` |
| Row-limit cap (probe) | `ROWNUM <= n` wrap | `LIMIT n` | `TOP (n)` |

PostgreSQL maps almost 1:1 onto the Oracle design (`ctid` ≈ `ROWID`, WAL LSN ≈ SCN). Adding a new engine is additive — the orchestrator, probe math, CLI, logging, chunk sizing, watermark, and live board are all engine-agnostic.

---

## 16. Known Limitations

- **TinyDB:** not safe for concurrent writes; susceptible to partial-read corruption if the process is killed mid-write. Use PostgreSQL or Oracle for scale and production runs.
- **BigQuery state table:** must be created manually before first use via `BQ_TableCreate.sql` — auto-creation is not implemented for BigQuery.
- **Schema verification:** PostgreSQL and BigQuery backends verify that the state table exists but do not check column-level schema. If you have a table with an outdated schema shape, compare the live table against the reference DDL manually.
- **Partitioned-table ROWID (dba_extents path):** may produce a cartesian join on some partitioned tables — narrow trigger, and avoided automatically when `DBMS_PARALLEL_EXECUTE` is available.
- **CLOB columns:** a read-after-fetchall limitation in some Oracle driver versions applies when the query returns CLOBs — verify before running at scale.
- **JDBC source:** best-effort only — column-range splitting, no physical/partition splitting, no native EXPLAIN, and no automatic chunk-count discovery (always pass an explicit `--chunks N`).
- **Reconcile anchoring:** snapshot-anchored (point-in-time) only on Oracle. PostgreSQL is best-effort; SQL Server and JDBC reconcile against live row counts, which can over- or under-count on an append-heavy source mid-load.
- **Deferred:** auto-retry/backoff for Striim API transient errors; persistent attempt counters per slice; poison-quarantine for repeatedly-failing slices; cluster backpressure; cross-backend schema migration utility.

---

## Appendix — Manual ROWID Split (`oracle_rowsplit.sql`)

> **Advanced / legacy.** The `probe` → `split` flow above supersedes this for almost all cases. Use it only when you cannot run the Python splitter (no Oracle driver available in your environment) and need to paste ROWID slices by hand.

`oracle_rowsplit.sql` is a self-contained SQL\*Plus / SQL Developer script that generates ROWID-range slice lines for any Oracle table. Run it against your table, copy the output, and paste the resulting `query|target` lines into `queryfile.txt`:

```
SELECT * FROM SCHEMA.TABLE WHERE ROWID BETWEEN 'AAAFxpAABAAALMsAAA' AND 'AAAFxpAABAAALMsAAH'|SCHEMA2.TABLE_TGT
SELECT * FROM SCHEMA.TABLE WHERE ROWID BETWEEN 'AAAFxpAABAAALMsAAI' AND 'AAAFxpAABAAALMsAAQ'|SCHEMA2.TABLE_TGT
```

These lines coexist with any other query groups in the same `queryfile.txt`. You can still interleave with `make_assorted_queryfile.py` before running `python main.py`.

This approach produces only physical ROWID slices with a fixed chunk count and no throughput measurement — the probe exists precisely to replace that guesswork.
