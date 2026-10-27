# Postgres quick start

Runs the ParallelLoader end to end against a Docker Postgres and Striim:

1. builds a sample database,
2. runs the probe on four scenarios and prints each recommendation panel,
3. splits a query into slices,
4. loads the slices through parallel Striim apps,
5. reconciles source and target row counts.

The Docker stack comes from
[striim-testing-framework-utility](https://github.com/striim/striim-testing-framework-utility).

## Prerequisites

- Docker, with about 2 GB free disk for the default data size (plus the framework's Striim images).
- This repo's virtual environment:
  `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt` (from the repo root).
- striim-testing-framework-utility cloned **beside** this repo and installed per its README,
  "Run your first test". The framework needs Python 3.12 or later. If it lives elsewhere, set
  `SLT_FRAMEWORK_HOME`.
- A Striim licence for the framework's Docker mode: `export STRIIM_HOME=<a licensed Striim install>`,
  or export `COMPANY_NAME`, `CLUSTER_NAME`, `PRODUCT_KEY` and `LICENCE_KEY`. To use your own Striim
  server instead, see "Your own Striim" below.

## Run it

```bash
cd quickstart/postgres
./quickstart.sh all --step
```

`--step` prints what the next stage does and waits: Enter runs it, `s` skips it, `q` stops.

```
>> next: probe
   Race split strategies on each scenario query and print the probe's recommendation panel.
   [Enter] run   [s] skip   [q] quit:
```

`./quickstart.sh all` runs the same stages without stopping. On a laptop with the stack
already up, it takes about 7 minutes at the default size.

| Stage | What it does |
|---|---|
| `preflight` | Checks Docker, the venv, the framework, the licence and port 5432. Changes nothing. |
| `stack` | Runs one `striim-test` case (`striim-test/cases/loader-slice`). This starts `slt-postgres` and `slt-striim` if they are down, deploys one slice app made from the loader's TQL template, and compares its 25 target rows exactly. |
| `build` | Creates `split_demo` (source) and `split_demo_tgt` (target) with `orders`, `order_lines` and monthly-partitioned `events`. |
| `pvs` | Sets the Striim property variables in `run/pvs.tql`. |
| `probe` | Runs the four scenarios below. Panels are saved in `run/logging/probe-*.txt`. |
| `split` | Splits region 7's orders into 6 `order_id` ranges in `run/queryfile.txt`. |
| `load` | Runs `main.py`: one Striim app per slice, 5 at a time, each created, deployed, started, monitored and dropped. Watch them at http://localhost:9080 (`admin` / `striim`). |
| `reconcile` | Compares source and target counts and checks for duplicate keys. |
| `check` | Shows whether the databases exist, their row counts and sizes. |
| `clean` | Retires the loader run, drops any leftover `ILA_*` apps and empties the target. `clean --all` also drops both databases. |

Any stage runs on its own: `./quickstart.sh probe 3`, `./quickstart.sh load --keep-target`.

## The probe scenarios

Panels below are from a run at the default size. MB/s varies from run to run; the verdicts do not.

**1. Whole `orders` table.** Physical (`ctid`) and the primary key (`order_id`) are close. `order_ts`
is random relative to the order rows are stored in, so its index reads scattered pages and is
slower. `customer_id`'s first slice is empty, because 40% of orders are customer 1 and the first cut
falls on that value, so it is not ranked.

```
  label                kind           MB/s  access_path              verdict
  ctid                 physical      98.58  Tid Range Scan           good
  order_id             column        97.88  Index Scan               good
  order_ts             column        72.37  Bitmap Heap Scan         good
  customer_id          column         0.00  Index Scan               empty-sample
```

**2. `orders` joined to `order_lines`.** `order_lines` has no index on `order_id`, so every
candidate's slice full-scans it. Running 12 slices would read `order_lines` 12 times. The probe
flags this before anything runs.

```
  ctid                 physical      25.44  Seq Scan                 AMPLIFYING
  order_id             column        21.90  Seq Scan                 AMPLIFYING
  ...
    - AMPLIFYING: candidate 'ctid' full-scans order_lines (join amplification).
```

**3. Partitioned `events`.** A range on the partition key reads only the matching partitions. The
three candidates are within a few percent of each other. The panel also points to
`--strategy partition`, which the stage then runs: one slice per month.

```
  event_id             column        85.15  Bitmap Heap Scan         good
  event_ts             column        83.37  Seq Scan (2 of 24 partitions) good
  ctid                 physical      83.06  Tid Range Scan           good
  partition: table IS partitioned — consider --strategy partition (deterministic, no probe needed).
-- split --strategy partition (one slice per partition):
  24 slices, e.g.:
    SELECT e.* FROM demo.events_2024_01 e WHERE 1=1
```

**4. A skewed split key.** `split` cuts `customer_id` at equal row counts. Customer 1 holds 40% of
the rows, so repeated cuts collapse: 16 requested slices become 12, two are empty, and one holds 44%
of the table. That slice decides the run time. This is why the probe ranks keys by distinct values.

```
  got 12 slices:
  slice  1:         0 rows   o.customer_id < 1
  slice  2:    437498 rows   o.customer_id >= 1 AND o.customer_id < 31725
  slice  3:     62501 rows   o.customer_id >= 31725 AND o.customer_id < 83553
  ...
  slice 12:         0 rows   o.customer_id IS NULL
```

## The load

`split` writes one line per slice, interleaved with `--assort` so concurrent apps work on different
parts of the key range:

```
  7 slices in run/queryfile.txt:
  slice  1:     17000 rows   o.region_id = 7 AND o.order_id >= 833334
  slice  2:     16666 rows   o.region_id = 7 AND o.order_id < 166667
  slice  3:         0 rows   o.region_id = 7 AND o.order_id IS NULL
  ...
```

`load` prints the slice counts as they change, then `reconcile` checks the result:

```
  source rows (load query): 100000
  target rows:              100000
  distinct order_id:        100000
  RECONCILED
```

Running `load --keep-target` again over a full target adds nothing. The template ignores
`DUPLICATE_ROW_EXISTS`, and the target has a primary key.

## Options

| Setting | Default | Effect |
|---|---|---|
| `--scale N` | `0.2` | Data size: `1` is 5M orders, 15M order lines and 10M events (about 5 GB). |
| `LOAD_CHUNKS` | `6` | Slices for the load. Each slice is one Striim app, deployed about 40 s after the previous one, so run time grows with this number. |
| `SLICE_SECONDS` | `1` | `--target-slice-seconds` passed to the probe (see below). |
| `PG_CONTAINER` | `slt-postgres` | Postgres container used for `psql`. |

**Reading `chunk_count`.** The probe sizes slices from how fast it can fetch rows itself, which is
far faster than Striim writes them. At this data size the default of 600 seconds per slice would
always give 1, so the quick start passes 1 second. For a real load, choose the slice count from how
many apps your cluster can run at once.

## Your own Striim

`run/.env.example` (copied to `run/.env` on first use) holds the loader's settings, and
`run/pvs.tql` holds the connection strings Striim uses. Both are set to the framework's Docker
defaults. For your own Striim server, set `STRIIM_URL`, `STRIIM_USER` and `STRIIM_PASS` for the
framework, `STRIIM_NODE` and `STRIIM_ADMIN_*` in `run/.env`, and in `run/pvs.tql` a Postgres
address your server can reach. `run/.env.example` also sets
`STRIIM_SOURCE_PROVIDER_TYPE=Postgres` and `STRIIM_TARGET_PROVIDER_TYPE=Postgres`, the
`DatabaseProviderType` value Striim accepts (checked on 5.0.6 and 5.4).

The stages that build and check the sample data run `psql` inside the Postgres container
(`PG_CONTAINER`, default `slt-postgres`). For your own source database, use the loader directly:
see "Use your existing infrastructure" in the [main README](../../README.md#use-your-existing-infrastructure).

## Files

| Path | Purpose |
|---|---|
| `quickstart.sh` | The stages above. |
| `sql/` | Database creation, schema, data generator (`-v scale=`), indexes and target table. |
| `queries/` | One `~SPLIT~` query per scenario, plus the load query. |
| `run/` | The loader's working directory: `.env.example`, `admin.SW.tql`, `pvs.tql`; output goes to `logging/`. |
| `striim-test/` | The framework case used by `stack`, and its `.env.example`. |

## Known limits

- Source and target share one Postgres container. Load timings show orchestration cost more than
  database speed.
- Column boundaries come from `percentile_disc` over the whole table, not from a sample.
- Every column split gets an extra `IS NULL` slice, even on a `NOT NULL` key. It is one more
  (empty) app.
