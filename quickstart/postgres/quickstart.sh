#!/usr/bin/env bash
# Postgres quick start for the ParallelLoader. See README.md in this folder.
#
#   ./quickstart.sh all --step         guided run, pausing before each stage
#   ./quickstart.sh <stage> [options]  one stage
#
# Runs against the striim-testing-framework-utility Docker stack: Postgres in
# container slt-postgres (localhost:5432) and Striim at localhost:9080.
set -euo pipefail

QS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOADER_HOME="$(cd "$QS_DIR/../.." && pwd)"
SQL_DIR="$QS_DIR/sql"

PG_CONTAINER="${PG_CONTAINER:-slt-postgres}"
PG_SUPERUSER="${PG_SUPERUSER:-postgres}"
SRC_DB=split_demo
TGT_DB=split_demo_tgt

RUN_DIR="$QS_DIR/run"
Q_DIR="$QS_DIR/queries"
LOADER_PY="${LOADER_PY:-$LOADER_HOME/.venv/bin/python}"

SCALE="${SCALE:-0.2}"
STEP=0
# The probe sizes chunk_count from its own fetch speed; at quick-start sizes 1 second
# per slice gives a visible chunk count. See README.md, "Reading chunk_count".
SLICE_SECONDS="${SLICE_SECONDS:-1}"

die() { echo "ERROR: $*" >&2; exit 1; }
say() { printf '\n== %s\n' "$*"; }

# psql inside the Postgres container; extra args pass through.
psql_in() {
    local db="$1"; shift
    docker exec -i "$PG_CONTAINER" psql -U "$PG_SUPERUSER" -d "$db" -v ON_ERROR_STOP=1 -q "$@"
}

# Single value from a query.
psql_val() {
    psql_in "$1" -tAc "$2"
}

# ---------------------------------------------------------------- build / check
stage_build() {
    say "build: sample databases at SCALE=$SCALE (5M orders x SCALE)"
    local t0=$SECONDS
    psql_in postgres < "$SQL_DIR/00_databases.sql"
    psql_in "$SRC_DB" < "$SQL_DIR/10_source_schema.sql"
    echo "generating rows..."
    psql_in "$SRC_DB" -v scale="$SCALE" < "$SQL_DIR/20_generate.sql" > /dev/null
    echo "keys, indexes, ANALYZE..."
    psql_in "$SRC_DB" < "$SQL_DIR/30_indexes_analyze.sql"
    psql_in "$TGT_DB" < "$SQL_DIR/40_target_schema.sql"
    echo "built in $((SECONDS - t0))s"
    stage_check
}

stage_check() {
    say "check: sample databases"
    local n
    n=$(psql_val postgres "SELECT count(*) FROM pg_database WHERE datname IN ('$SRC_DB','$TGT_DB')")
    if [ "$n" != "2" ]; then
        echo "databases missing: run ./quickstart.sh build"
        return 1
    fi
    psql_in "$SRC_DB" -c "
        SELECT 'demo.orders' AS source_table, count(*) AS row_count FROM demo.orders
        UNION ALL SELECT 'demo.order_lines', count(*) FROM demo.order_lines
        UNION ALL SELECT 'demo.events', count(*) FROM demo.events"
    psql_in "$TGT_DB" -c "SELECT 'demo.orders' AS target_table, count(*) AS row_count FROM demo.orders"
    psql_in postgres -c "
        SELECT datname AS database, pg_size_pretty(pg_database_size(datname)) AS size
        FROM pg_database WHERE datname IN ('$SRC_DB','$TGT_DB') ORDER BY 1"
    local empty
    empty=$(psql_val "$SRC_DB" "SELECT (NOT EXISTS (SELECT 1 FROM demo.orders))
                                    OR (NOT EXISTS (SELECT 1 FROM demo.order_lines))
                                    OR (NOT EXISTS (SELECT 1 FROM demo.events))")
    [ "$empty" = "f" ] || { echo "a source table is empty: run ./quickstart.sh build"; return 1; }
}

# ---------------------------------------------------------------- loader helpers
# The loader reads .env, admin.SW.tql and queryfile.txt from its working directory.
loader() {
    [ -x "$LOADER_PY" ] || die "loader Python not found at $LOADER_PY (create $LOADER_HOME/.venv; see README.md)"
    [ -f "$RUN_DIR/.env" ] || cp "$RUN_DIR/.env.example" "$RUN_DIR/.env"
    (cd "$RUN_DIR" && "$LOADER_PY" "$LOADER_HOME/manage.py" "$@")
}

# Run a command while holding this run's lock, so no loader can start until it exits.
# Exits 75 without running it when a live loader holds the lock. Uses the loader's own
# lock logic (pid plus process start time), so a stale lock left by a crash does not count.
with_run_lock() {
    (cd "$RUN_DIR" && "$LOADER_PY" - "$LOADER_HOME" "$@" <<'PY'
import os, subprocess, sys
sys.path[:0] = [sys.argv[1], os.path.join(sys.argv[1], "loader")]
import config, run_safety
ok, msg = run_safety.acquire_lock(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
if not ok:
    print(msg, file=sys.stderr)
    sys.exit(75)
try:
    child = subprocess.Popen(sys.argv[2:])
    while True:
        try:
            rc = child.wait()
            break
        except KeyboardInterrupt:
            pass  # the child got the same Ctrl-C; hold the lock until it has exited
finally:
    run_safety.release_lock(config.LOG_OUTPUT_PATH, config.UNIQUE_RUN_ID)
sys.exit(rc)
PY
    )
}

# Run a ~SPLIT~ query once with the placeholder replaced by TRUE, so every probe
# starts from a warm cache and scenario order does not change the result.
warm() {
    local q
    q="$(cat "$1")"
    psql_val "$SRC_DB" "SELECT count(*) FROM (${q//\~SPLIT\~/TRUE}) s" > /dev/null
}

# Probe one scenario and keep its output in run/logging/probe-<name>.txt.
probe_one() {
    local name="$1" table="$2"; shift 2
    local out="$RUN_DIR/logging/probe-$name.txt"
    mkdir -p "$RUN_DIR/logging"
    warm "$Q_DIR/$name.sql"
    loader probe --query-file "$Q_DIR/$name.sql" --table "$table" \
        --target-slice-seconds "$SLICE_SECONDS" "$@" 2>&1 \
        | sed -n '/^=\{10,\}/,$p' | tee "$out"
}

# Row count of every line in run/queryfile.txt.
slice_counts() {
    local i=0 q t n
    while IFS='|' read -r q t; do
        i=$((i + 1))
        n=$(psql_val "$SRC_DB" "SELECT count(*) FROM ($q) s" < /dev/null)
        printf '  slice %2d: %9d rows   %s\n' "$i" "$n" "${q#*WHERE }"
    done < "$RUN_DIR/queryfile.txt"
}

# ---------------------------------------------------------------- probe scenarios
scenario_1() {
    say "scenario 1: whole orders table: physical (ctid) vs indexed columns"
    probe_one 01_baseline demo.orders
}

scenario_2() {
    say "scenario 2: orders joined to order_lines: the probe checks every plan for join amplification"
    probe_one 02_orders_with_lines demo.orders --alias o
}

scenario_3() {
    say "scenario 3: monthly-partitioned events"
    probe_one 03_events_partitioned demo.events
    echo "-- split --strategy partition (one slice per partition):"
    loader split --query-file "$Q_DIR/03_events_partitioned.sql" --table demo.events \
        --target demo.events --strategy partition > /dev/null
    echo "  $(wc -l < "$RUN_DIR/queryfile.txt" | tr -d ' ') slices, e.g.:"
    head -2 "$RUN_DIR/queryfile.txt" | cut -d'|' -f1 | sed 's/^/    /'
}

scenario_4() {
    say "scenario 4: skewed customer_id (40% of orders are customer 1), asked for 16 slices"
    warm "$Q_DIR/04_skewed_customer.sql"
    loader split --query-file "$Q_DIR/04_skewed_customer.sql" --table demo.orders \
        --target demo.orders --strategy column --column customer_id --chunks 16 > /dev/null
    echo "  got $(wc -l < "$RUN_DIR/queryfile.txt" | tr -d ' ') slices:"
    slice_counts
}

stage_probe() {
    local only="${1:-}"
    if [ -n "$only" ]; then
        "scenario_$only"
    else
        scenario_1; scenario_2; scenario_3; scenario_4
    fi
}

# ---------------------------------------------------------------- Striim load
# The load query is written by stage_split: enough whole regions for about LOAD_ROWS
# rows at any SCALE. Regions are spread evenly over order_id (1000-order batches), so
# every slice gets a similar share. A range filter on the split key would not be: the
# split cuts the whole table, so filtered-out ranges would become empty slices.
LOAD_QUERY="$RUN_DIR/load_query.sql"
LOAD_ROWS="${LOAD_ROWS:-5000000}"
# Each slice is its own Striim app, and the loader deploys them about 40 s apart, so
# run time grows with the slice count. 6 slices of about 100k rows keep 2-3 apps
# running at once and finish in about 6 minutes.
LOAD_CHUNKS="${LOAD_CHUNKS:-6}"

# Striim property variables used by admin.SW.tql. Applied through the loader's own
# REST helper (main.runCommand), which posts TQL to /api/v2/tungsten.
stage_pvs() {
    say "pvs: Striim property variables for source and target (run/pvs.tql)"
    [ -f "$RUN_DIR/.env" ] || cp "$RUN_DIR/.env.example" "$RUN_DIR/.env"
    (cd "$RUN_DIR" && "$LOADER_PY" - "$LOADER_HOME" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
import contextlib, io
import main
main.headers = main.authenticate()
with contextlib.redirect_stdout(io.StringIO()):  # runCommand prints the raw response
    ok, msg = main.runCommand(open("pvs.tql").read())
if not ok:
    sys.exit("pvs.tql failed: " + str(msg))
print("property variables set in namespace admin")
PY
    )
}

stage_split() {
    local orders regions
    orders=$(psql_val "$SRC_DB" "SELECT count(*) FROM demo.orders")
    regions=$(( (LOAD_ROWS * 10 + orders / 2) / orders ))
    [ "$regions" -ge 1 ] || regions=1
    [ "$regions" -le 10 ] || regions=10
    printf 'SELECT o.* FROM demo.orders o WHERE o.region_id <= %d AND ~SPLIT~\n' "$regions" > "$LOAD_QUERY"
    say "split: orders in regions 1-$regions into $LOAD_CHUNKS order_id ranges, interleaved (--assort)"
    echo "  query: $(cat "$LOAD_QUERY")"
    warm "$LOAD_QUERY"
    loader split --query-file "$LOAD_QUERY" --table demo.orders --target demo.orders \
        --strategy column --column order_id --chunks "$LOAD_CHUNKS" --assort > /dev/null
    # --assort writes the interleaved order to a separate file; main.py reads queryfile.txt.
    mv "$RUN_DIR/queryfile-assorted.txt" "$RUN_DIR/queryfile.txt"
    echo "  $(wc -l < "$RUN_DIR/queryfile.txt" | tr -d ' ') slices in run/queryfile.txt:"
    slice_counts
}

stage_load() {
    say "load: one Striim app per slice, 5 at a time (watch http://localhost:9080)"
    [ -s "$RUN_DIR/queryfile.txt" ] && [ -f "$LOAD_QUERY" ] || die "no slices: run ./quickstart.sh split"
    # load never empties the target. main.py takes the run lock and decides FRESH, RESUME
    # or REFUSE itself; a resumed run keeps its completed slices' rows. clean empties it.
    local t0=$SECONDS log="$RUN_DIR/logging/load.log" pid rc last="" now
    mkdir -p "$RUN_DIR/logging"
    echo "  orchestrator output: run/logging/load.log"
    pid="" stop=0
    # A background job ignores Ctrl-C in a script, so pass it on: stop the loader too.
    # A signal that lands before $! is known is acted on right after it is.
    stop_load() {
        stop=1
        [ -n "$pid" ] || return 0
        kill "$pid" 2> /dev/null || true
        wait "$pid" 2> /dev/null || true
        die "load stopped. Run ./quickstart.sh load to resume it, or clean for a fresh load"
    }
    trap stop_load INT TERM
    (cd "$RUN_DIR" && exec "$LOADER_PY" "$LOADER_HOME/main.py") > "$log" 2>&1 &
    pid=$!
    [ "$stop" = 0 ] || stop_load
    # Print the loader's slice counts each time they change.
    while kill -0 "$pid" 2> /dev/null; do
        now=$(loader status 2> /dev/null | sed -n 's/^  counts: //p')
        if [ -n "$now" ] && [ "$now" != "$last" ]; then
            printf '  %4ds  %s\n' "$((SECONDS - t0))" "$now"
            last="$now"
        fi
        sleep 5
    done
    rc=0; wait "$pid" || rc=$?
    trap - INT TERM
    grep -E "Run completed|DID NOT COMPLETE|non-COMPLETED" "$log" | sed 's/^/  /' || true
    [ "$rc" = 0 ] || die "load failed (exit $rc); see run/logging/load.log"
    echo "load finished in $((SECONDS - t0))s"
}

stage_reconcile() {
    say "reconcile: source rows vs target rows"
    local q src tgt dist
    q="$(cat "$LOAD_QUERY")"
    src=$(psql_val "$SRC_DB" "SELECT count(*) FROM (${q//\~SPLIT\~/TRUE}) s")
    tgt=$(psql_val "$TGT_DB" "SELECT count(*) FROM demo.orders")
    dist=$(psql_val "$TGT_DB" "SELECT count(DISTINCT order_id) FROM demo.orders")
    printf '  source rows (load query): %s\n  target rows:              %s\n  distinct order_id:        %s\n' \
        "$src" "$tgt" "$dist"
    if [ "$src" = "$tgt" ] && [ "$tgt" = "$dist" ]; then
        echo "  RECONCILED"
    else
        echo "  MISMATCH"
        return 1
    fi
}

# Retire the loader run and remove its Striim apps; --all also drops both databases.
# The reset runs as a child of with_run_lock, so a load started meanwhile is refused.
stage_clean() {
    say "clean: retire loader run, remove ILA_ apps, empty the target"
    local rc=0
    with_run_lock bash "$QS_DIR/quickstart.sh" _reset "${1:-}" || rc=$?
    [ "$rc" != 75 ] || die "a load is running; stop it (Ctrl-C in its terminal) before clean"
    return "$rc"
}

stage_reset() {
    local all="${1:-}"
    loader clear --with-striim-cleanup --yes || true
    rm -f "$RUN_DIR/queryfile.txt" "$LOAD_QUERY"
    if [ "$all" = "--all" ]; then
        psql_in postgres -c "DROP DATABASE IF EXISTS $SRC_DB WITH (FORCE)" \
                         -c "DROP DATABASE IF EXISTS $TGT_DB WITH (FORCE)"
        echo "dropped $SRC_DB and $TGT_DB"
    elif [ "$(psql_val postgres "SELECT count(*) FROM pg_database WHERE datname = '$TGT_DB'")" = "1" ]; then
        psql_in "$TGT_DB" -c "TRUNCATE demo.orders"
    fi
}

# ---------------------------------------------------------------- preflight / stack
FW_HOME="${SLT_FRAMEWORK_HOME:-$LOADER_HOME/../striim-testing-framework-utility}"
[ -d "$FW_HOME" ] && FW_HOME="$(cd "$FW_HOME" && pwd)"
ST_DIR="$QS_DIR/striim-test"

# Checks only; changes nothing. Each failure names its fix.
stage_preflight() {
    say "preflight: Docker, the loader venv, the framework, a Striim licence, port 5432"
    local fail=0
    ok()  { echo "  [ ok ] $*"; }
    bad() { echo "  [FAIL] $*"; fail=1; }

    if docker info > /dev/null 2>&1; then ok "Docker is running"
    else bad "Docker is not reachable: start Docker Desktop (or the Docker daemon)"; fi

    if [ -x "$LOADER_PY" ]; then ok "loader venv: $LOADER_PY"
    else bad "no loader venv at $LOADER_PY: cd $LOADER_HOME && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"; fi

    if [ -x "$FW_HOME/.venv/bin/striim-test" ]; then ok "framework: $FW_HOME"
    else bad "striim-testing-framework-utility not found at $FW_HOME: clone it beside this repo and install it (its README, 'Run your first test'), or set SLT_FRAMEWORK_HOME"; fi

    if [ -n "${STRIIM_URL:-}" ]; then
        ok "Striim: your own server at STRIIM_URL=$STRIIM_URL"
    elif [ -n "${STRIIM_HOME:-}" ] && [ -f "$STRIIM_HOME/conf/startUp.properties" ]; then
        ok "Striim licence: from STRIIM_HOME=$STRIIM_HOME"
    elif [ -n "${COMPANY_NAME:-}" ] && [ -n "${CLUSTER_NAME:-}" ] && [ -n "${PRODUCT_KEY:-}" ] && [ -n "${LICENCE_KEY:-}" ]; then
        ok "Striim licence: from COMPANY_NAME / CLUSTER_NAME / PRODUCT_KEY / LICENCE_KEY"
    else
        bad "no Striim licence for Docker mode: export STRIIM_HOME=<a licensed Striim install>, or the four licence variables, or STRIIM_URL for your own server"
    fi

    if [ "$(docker ps --filter "name=^${PG_CONTAINER}\$" --format '{{.Names}}' 2>/dev/null)" = "$PG_CONTAINER" ]; then
        ok "Postgres: $PG_CONTAINER is running"
    elif (exec 3<>/dev/tcp/127.0.0.1/5432) 2>/dev/null; then
        bad "port 5432 is taken by something other than $PG_CONTAINER: stop that Postgres first"
    else
        ok "port 5432 is free ($PG_CONTAINER starts in the stack stage)"
    fi

    [ "$fail" = 0 ] || die "preflight failed; fix the [FAIL] lines above"
}

# Bring up Postgres + Striim by running one framework case. The case also proves the
# loader's TQL template on Postgres: one slice app, compared exactly with its rows.
stage_stack() {
    say "stack: striim-test runs one slice app from the loader template (starts Postgres and Striim if needed)"
    [ -f "$ST_DIR/.env" ] || cp "$ST_DIR/.env.example" "$ST_DIR/.env"
    SLT_FRAMEWORK_HOME="$FW_HOME" SLT_PROJECT_ROOT="$ST_DIR" SLT_LIVE_CASES="$ST_DIR/cases" \
        "$FW_HOME/.venv/bin/striim-test" run "$ST_DIR/cases/loader-slice"
}

# ---------------------------------------------------------------- all / --step
# Stage descriptions shown before each stage under --step.
describe() {
    case "$1" in
        preflight) echo "Check Docker, the loader venv, the framework and a Striim licence. Changes nothing." ;;
        stack)     echo "Run one striim-test case: starts slt-postgres and slt-striim if they are not up, deploys one slice app made from the loader's TQL template, and compares its 25 target rows exactly." ;;
        build)     echo "Create split_demo and split_demo_tgt and generate orders, order_lines and monthly-partitioned events at SCALE=$SCALE." ;;
        pvs)       echo "Set the Striim property variables (connection URLs and logins) the load apps use." ;;
        probe)     echo "Race split strategies on each scenario query and print the probe's recommendation panel." ;;
        split)     echo "Split about $LOAD_ROWS orders into $LOAD_CHUNKS order_id ranges: one line per Striim app in run/queryfile.txt." ;;
        load)      echo "Create, deploy, start, monitor and drop one Striim app per slice, 5 at a time. Watch http://localhost:9080." ;;
        reconcile) echo "Compare source and target row counts and check for duplicates." ;;
    esac
}

# Under --step: describe the stage, then wait for Enter (s = skip, q = quit).
gate() {
    [ "$STEP" = 1 ] || return 0
    printf '\n>> next: %s\n   %s\n   [Enter] run   [s] skip   [q] quit: ' "$1" "$(describe "$1")"
    local reply
    read -r reply < /dev/tty
    case "$reply" in
        q|Q) echo "stopped."; exit 0 ;;
        s|S) return 1 ;;
    esac
    return 0
}

summary() {
    say "summary"
    local f
    for f in "$RUN_DIR"/logging/probe-*.txt; do
        [ -f "$f" ] || continue
        printf '  %-28s %s %s\n' "$(basename "$f" .txt | sed 's/^probe-//')" \
            "$(grep -m1 'strategy:' "$f" | awk '{print $2}')" \
            "$(grep -m1 'key:' "$f" | awk '{print $2}')"
    done
    echo "  total time: $((SECONDS - ALL_T0))s"
}

stage_all() {
    ALL_T0=$SECONDS
    local s
    for s in preflight stack build pvs probe split load reconcile; do
        gate "$s" || continue
        "stage_$s"
    done
    summary
}

# ---------------------------------------------------------------- main
usage() {
    cat <<'EOF'
Usage: ./quickstart.sh <stage> [--scale N] [--step]

  ./quickstart.sh all --step     guided run: describes each stage and waits for Enter

Stages:
  all        preflight, stack, build, pvs, probe, split, load, reconcile, then a summary
  preflight  check Docker, the loader venv, the framework and a Striim licence
  stack      start Postgres and Striim via one striim-test case (and check the TQL template)
  build      drop and recreate split_demo / split_demo_tgt and generate the data
  check      show whether the databases exist, row counts and sizes
  probe [N]  run the probe scenarios (or only scenario N, 1-4)
  pvs        set the Striim property variables from run/pvs.tql
  split      split the load query into run/queryfile.txt
  load       run the parallel load through Striim (never empties the target; see clean)
  reconcile  compare source and target row counts
  clean      retire the run and its Striim apps, empty the target (--all: drop databases)

Options:
  --scale N  data size multiplier (default 0.2; 1 = 5M orders, about 5 GB)
  --step     pause before each stage (with "all")
EOF
}

main() {
    [ $# -ge 1 ] || { usage; exit 2; }
    local stage="$1"; shift
    local args=()
    while [ $# -gt 0 ]; do
        case "$1" in
            --scale) SCALE="${2:?--scale needs a value}"; shift 2 ;;
            --step)  STEP=1; shift ;;
            *)       args+=("$1"); shift ;;
        esac
    done
    case "$stage" in
        all) stage_all ;;
        preflight) stage_preflight ;;
        stack) stage_stack ;;
        build) stage_build ;;
        check) stage_check ;;
        probe) stage_probe "${args[@]:-}" ;;
        pvs) stage_pvs ;;
        split) stage_split ;;
        load) stage_load "${args[@]:-}" ;;
        reconcile) stage_reconcile ;;
        clean) stage_clean "${args[@]:-}" ;;
        _reset) stage_reset "${args[@]:-}" ;;  # internal: clean's body, under the run lock
        -h|--help|help) usage ;;
        *) usage; exit 2 ;;
    esac
}

main "$@"
