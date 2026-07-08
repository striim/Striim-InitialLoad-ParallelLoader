#!/usr/bin/env bash
#
# manage.sh — management console for the Striim InitialLoad ParallelLoader.
#
# A polished front-end (modeled on striim-gcp-manager.sh) over the manage.py
# engine, which does all the real work. With arguments, this passes straight
# through to the engine for scripting:  ./manage.sh status --json
# With no arguments, it shows the interactive console.
#
# Portable bash (works on macOS bash 3.2): no ${var,,}, no associative arrays,
# no mapfile. Deliberately NOT `set -e` — a failing action must return to the
# menu, not kill the console.
set -o pipefail
cd "$(dirname "$0")"
# The venv bootstrap + scripting pass-through run further down, right after the
# print/confirm helpers that bootstrap_venv depends on.

# ======================== COLORS ========================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
WHITE='\033[1;37m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m' # No Color

# ======================== PRINT HELPERS ========================
print_header() {
  echo ""
  echo -e "${CYAN}=============================================="
  echo -e "$1"
  echo -e "==============================================${NC}"
  echo ""
}
print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_error()   { echo -e "${RED}✗ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }
print_info()    { echo -e "${BLUE}ℹ $1${NC}"; }

pause() { echo ""; read -rp "Press Enter to continue..." _; }

# Returns 0 (yes) / 1 (no). Portable case-insensitive match. Default NO.
confirm() {
  local ans
  while true; do
    read -rp "$1 [y/N]: " ans
    case "$ans" in
      y|Y|yes|YES|Yes) return 0 ;;
      n|N|no|NO|No|"") return 1 ;;
      *) print_warning "Please answer y or n." ;;
    esac
  done
}

# Like confirm() but default YES (blank Enter = yes).
confirm_yes() {
  local ans
  while true; do
    read -rp "$1 [Y/n]: " ans
    case "$ans" in
      n|N|no|NO|No) return 1 ;;
      y|Y|yes|YES|Yes|"") return 0 ;;
      *) print_warning "Please answer y or n." ;;
    esac
  done
}

# -----------------------------------------------------------------------------
# Re-prompt helpers — read until the value is valid instead of silently coercing
# to a default or aborting. bash 3.2 has no namerefs, so each sets the global
# PROMPT_RESULT and (where relevant) returns non-zero to signal "cancelled".
# -----------------------------------------------------------------------------

# prompt_int PROMPT DEFAULT -> a positive integer (blank uses DEFAULT). Re-prompts.
prompt_int() {
  local prompt="$1" default="$2" val re='^[1-9][0-9]*$'
  while true; do
    read -rp "$prompt" val
    val=${val:-$default}
    [[ "$val" =~ $re ]] && { PROMPT_RESULT="$val"; return 0; }
    print_warning "Please enter a positive whole number."
  done
}

# prompt_opt_int PROMPT -> "" (blank = use the tool default) or a positive integer.
prompt_opt_int() {
  local prompt="$1" val re='^[1-9][0-9]*$'
  while true; do
    read -rp "$prompt" val
    [[ -z "$val" ]] && { PROMPT_RESULT=""; return 0; }
    [[ "$val" =~ $re ]] && { PROMPT_RESULT="$val"; return 0; }
    print_warning "Enter a positive whole number, or leave blank for the default."
  done
}

# prompt_enum PROMPT DEFAULT "a b c" -> one of the set (blank uses DEFAULT). Re-prompts.
prompt_enum() {
  local prompt="$1" default="$2" allowed="$3" val w
  while true; do
    read -rp "$prompt" val
    val=${val:-$default}
    for w in $allowed; do
      [[ "$val" == "$w" ]] && { PROMPT_RESULT="$val"; return 0; }
    done
    print_warning "Please enter one of: $allowed"
  done
}

# prompt_owner_table PROMPT -> a validated OWNER.TABLE (or bare TABLE). Blank cancels
# (returns 1). Blocks spaces/quotes/';'/'|' so nothing hostile reaches python/SQL.
prompt_owner_table() {
  local prompt="$1" val re='^[A-Za-z0-9_$#]+(\.[A-Za-z0-9_$#]+)?$'
  while true; do
    read -rp "$prompt" val
    [[ -z "$val" ]] && { PROMPT_RESULT=""; return 1; }
    [[ "$val" =~ $re ]] && { PROMPT_RESULT="$val"; return 0; }
    print_warning "Enter as OWNER.TABLE (e.g. PAY.CM_CASES) — letters, digits, _ \$ # only. Blank to cancel."
  done
}

# prompt_opt_ident PROMPT -> "" (blank) or a plain identifier (e.g. a table alias).
prompt_opt_ident() {
  local prompt="$1" val re='^[A-Za-z0-9_$#]+$'
  while true; do
    read -rp "$prompt" val
    [[ -z "$val" ]] && { PROMPT_RESULT=""; return 0; }
    [[ "$val" =~ $re ]] && { PROMPT_RESULT="$val"; return 0; }
    print_warning "Must be a plain identifier (letters, digits, _ \$ #), or blank to auto-detect."
  done
}

# Report ✓/✗ from the last command's exit code.
report_rc() {
  local rc="$1" ok="$2" bad="$3"
  if [[ "$rc" -eq 0 ]]; then
    print_success "$ok"
  else
    print_error "$bad (exit $rc)"
  fi
}

# ======================== VENV BOOTSTRAP ========================
# Ensure we run against .venv with the base dependencies installed. On a fresh
# clone (no .venv) offer to create it; if the base deps are missing offer to
# install them. Engine drivers (oracledb, pyodbc, …) are intentionally NOT in
# requirements.txt — they keep their own per-engine install hints. Silent on the
# happy path; never prompts when stdin is not a TTY (scripted pass-through), and
# all non-interactive notices go to stderr so `--json` stdout stays clean.
bootstrap_venv() {
  local interactive=0
  [[ -t 0 ]] && interactive=1

  if [[ ! -d .venv ]]; then
    if [[ "$interactive" -eq 1 ]]; then
      print_warning "No .venv found in $(pwd)."
      if confirm_yes "Create a .venv and install requirements now?"; then
        if ! command -v python3 >/dev/null 2>&1; then
          print_error "python3 not found on PATH — install Python 3, then re-run."
          return 0
        fi
        if python3 -m venv .venv; then
          # shellcheck disable=SC1091
          source .venv/bin/activate
          print_info "Installing requirements (this can take a minute)…"
          if pip install -r requirements.txt; then
            print_success "Virtual environment ready."
          else
            print_error "pip install failed — see the output above."
          fi
        else
          print_error "Could not create .venv — continuing with system python."
        fi
      else
        print_warning "Continuing without a .venv (using system python)."
      fi
    else
      echo "manage.sh: no .venv found; using system python (non-interactive)." >&2
    fi
  elif [[ -z "${VIRTUAL_ENV:-}" ]]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
  fi

  # Base-dependency check: tinydb ships in requirements.txt and backs the
  # default TinyDB state store, so a miss means requirements were never installed.
  if ! python -c 'import tinydb' >/dev/null 2>&1; then
    if [[ "$interactive" -eq 1 ]]; then
      print_warning "Base dependencies are missing (could not import tinydb)."
      if confirm_yes "Install requirements now (pip install -r requirements.txt)?"; then
        if pip install -r requirements.txt; then
          print_success "Dependencies installed."
        else
          print_error "pip install failed — see the output above."
        fi
      fi
    else
      echo "manage.sh: base dependencies missing (run: pip install -r requirements.txt)." >&2
    fi
  fi
}

bootstrap_venv

# Direct pass-through for scripting / the documented subcommands.
if [[ $# -gt 0 ]]; then
  exec python manage.py "$@"
fi

# ======================== SOURCE ENGINE HELPERS ========================
# Prompt for the SOURCE ENGINE the probe/split reads. Sets the global SE_ENGINE.
# Oracle is the default; when oracle is chosen the wizards omit --source-engine
# so the generated command stays byte-identical to the pre-engine behavior.
prompt_source_engine() {
  echo -e "${CYAN}Source engine${NC}"
  echo -e "  ${DIM}oracle (default), postgres, sqlserver, jdbc — the source DB the probe/split reads.${NC}"
  prompt_enum "  Engine oracle/postgres/sqlserver/jdbc [oracle]: " "oracle" "oracle postgres sqlserver jdbc"
  SE_ENGINE="$PROMPT_RESULT"
}

# Print the engine-aware driver + connectivity hint shown when a probe/split fails.
print_engine_driver_hint() {
  local eng="$1"
  case "$eng" in
    postgres)
      print_info "The postgres probe/split needs the psycopg2 driver. If you saw a missing-module error:"
      echo -e "    ${DIM}pip install psycopg2-binary${NC}"
      print_info "Env: SOURCE_PG_HOST / SOURCE_PG_PORT / SOURCE_PG_DATABASE / SOURCE_PG_USER / SOURCE_PG_PASSWORD / SOURCE_PG_SSLMODE."
      ;;
    sqlserver)
      print_info "The sqlserver probe/split needs the pyodbc driver plus an ODBC driver. If you saw a missing-module error:"
      echo -e "    ${DIM}pip install pyodbc${NC}   ${DIM}# also install an ODBC driver, e.g. 'ODBC Driver 18 for SQL Server'${NC}"
      print_info "Env: SQLSERVER_HOST / SQLSERVER_PORT / SQLSERVER_DATABASE / SQLSERVER_USER / SQLSERVER_PASSWORD / SQLSERVER_DRIVER."
      ;;
    jdbc)
      print_info "The jdbc probe/split needs JayDeBeApi + JPype1 plus Java on PATH. If you saw a missing-module error:"
      echo -e "    ${DIM}pip install JayDeBeApi JPype1${NC}"
      print_info "Env: JDBC_DRIVER_CLASS / JDBC_URL / JDBC_JAR_PATH / JDBC_USER / JDBC_PASSWORD (optional JDBC_ROW_LIMIT_SYNTAX / JDBC_WATERMARK_SQL)."
      ;;
    *)
      print_info "The oracle probe/split needs the Oracle driver. If you saw a missing-module error:"
      echo -e "    ${DIM}pip install python-oracledb --index-url https://pypi.org/simple${NC}"
      print_info "Env: ORACLE_DSN (or ORACLE_HOST + ORACLE_SERVICE) + ORACLE_USER + ORACLE_PASSWORD."
      ;;
  esac
  print_info "Other causes: wrong OWNER.TABLE, a join needing --alias, or the engine env vars above not set."
}

# Branch on a failed probe/split exit code. Exit 3 means the engine is missing
# credentials (manage.py classifies "Missing … settings" and returns 3): offer
# the setup wizard for this engine, seeded with SOURCE_DB_TYPE so it collects
# the right fields. Any other non-zero gets the driver/connectivity hint.
# Returns 0 when the caller should RETRY the command, 1 otherwise.
handle_engine_failure() {
  local rc="$1" eng="${2:-oracle}"
  if [[ "$rc" -eq 3 ]]; then
    print_warning "This looks like missing database credentials for the ${eng} engine."
    if confirm_yes "Run the setup wizard now to enter them?"; then
      SOURCE_DB_TYPE="$eng" python manage.py setup --interactive
      echo ""
      if confirm_yes "Credentials saved — retry now?"; then
        return 0
      fi
    fi
    return 1
  fi
  print_error "Failed (exit $rc)."
  print_engine_driver_hint "$eng"
  return 1
}

# ======================== ENGINE-DRIVEN STATUS ========================
# Populates ST_* from `manage.py status --json` + config. jq may be missing, so
# we parse with python (always present — it runs the engine).
load_status() {
  local json parsed
  json=$(python manage.py status --json 2>/dev/null || true)
  parsed=$(STATUS_JSON="$json" python - <<'PY' 2>/dev/null || true
import json, os
try:
    import config
    run_default = getattr(config, "UNIQUE_RUN_ID", "?")
    backend = getattr(config, "STAGE_DB_LOCATION", "?")
    queryfile = getattr(config, "QUERY_FILE", "queryfile.txt")
except Exception:
    run_default, backend, queryfile = "?", "?", "queryfile.txt"
try:
    d = json.loads(os.environ.get("STATUS_JSON", "") or "{}")
except Exception:
    d = {}
counts = d.get("counts", {}) or {}
def c(k):
    return counts.get(k, 0)
total = sum(counts.values()) if counts else 0
print(d.get("run_id", run_default))
print(d.get("state", "UNKNOWN"))
print(total)
print(c("NEW"))
print(c("RUNNING"))
print(c("COMPLETED"))
print(c("FAILED"))
print(c("COMPLETED-FAILEDDROP"))
print(backend)
print(queryfile)
PY
)
  # Defaults guard against a parse miss.
  ST_RUN_ID="?"; ST_STATE="UNKNOWN"; ST_TOTAL=0
  ST_NEW=0; ST_RUNNING=0; ST_DONE=0; ST_FAILED=0; ST_FAILEDDROP=0
  ST_BACKEND="?"; ST_QUERYFILE="queryfile.txt"
  {
    IFS= read -r ST_RUN_ID
    IFS= read -r ST_STATE
    IFS= read -r ST_TOTAL
    IFS= read -r ST_NEW
    IFS= read -r ST_RUNNING
    IFS= read -r ST_DONE
    IFS= read -r ST_FAILED
    IFS= read -r ST_FAILEDDROP
    IFS= read -r ST_BACKEND
    IFS= read -r ST_QUERYFILE
  } <<< "$parsed"
  : "${ST_RUN_ID:=?}" "${ST_STATE:=UNKNOWN}" "${ST_TOTAL:=0}"
  : "${ST_NEW:=0}" "${ST_RUNNING:=0}" "${ST_DONE:=0}" "${ST_FAILED:=0}" "${ST_FAILEDDROP:=0}"
  : "${ST_BACKEND:=?}" "${ST_QUERYFILE:=queryfile.txt}"
}

render_status_line() {
  local state_color
  case "$ST_STATE" in
    FINISHED)      state_color="$GREEN" ;;
    "IN PROGRESS") state_color="$YELLOW" ;;
    "NOT STARTED") state_color="$DIM" ;;
    *)             state_color="$DIM" ;;
  esac
  local failed_seg
  if [[ "${ST_FAILED:-0}" -gt 0 ]]; then
    failed_seg="${RED}FAILED ${ST_FAILED}${NC}"
  else
    failed_seg="${DIM}FAILED 0${NC}"
  fi
  echo -e "  ${CYAN}Run:${NC} ${WHITE}${ST_RUN_ID}${NC}  │  ${CYAN}Backend:${NC} ${MAGENTA}${ST_BACKEND}${NC}  │  ${CYAN}State:${NC} ${state_color}${ST_STATE}${NC}  │  ${CYAN}Queryfile:${NC} ${BLUE}${ST_QUERYFILE}${NC}"
  local slices="  ${CYAN}Slices:${NC} ${WHITE}${ST_TOTAL}${NC} total │ ${BLUE}NEW ${ST_NEW}${NC} │ ${YELLOW}RUNNING ${ST_RUNNING}${NC} │ ${GREEN}DONE ${ST_DONE}${NC} │ ${failed_seg}"
  if [[ "${ST_FAILEDDROP:-0}" -gt 0 ]]; then
    slices="${slices} │ ${MAGENTA}FAILEDDROP ${ST_FAILEDDROP}${NC}"
  fi
  echo -e "$slices"
}

# Colored per-slice table (the show_vm_status analog), driven by --rows JSON.
# Data is passed to the parser via an env var, NOT a pipe: `python - <<'PY'`
# reads its *program* from the heredoc, so piped stdin would be discarded.
render_slice_table() {
  local rows_json rows_tsv
  rows_json=$(python manage.py status --json --rows 2>/dev/null || true)
  rows_tsv=$(ROWS_JSON="$rows_json" python - <<'PY' 2>/dev/null || true
import json, os
raw = os.environ.get("ROWS_JSON", "")
try:
    rows = (json.loads(raw) or {}).get("rows", []) or []
except Exception:
    rows = []
def field(v, n):
    s = "" if v is None else str(v)
    s = s.replace("\t", " ").replace("\n", " ")
    return s if len(s) <= n else s[: n - 2] + ".."
for r in rows:
    print("\t".join([
        str(r.get("roworder", "")),
        field(r.get("status"), 22),
        field(r.get("targettbl"), 26),
        field(r.get("appname"), 24),
        field(r.get("notes"), 40),
    ]))
PY
)
  if [[ -z "$rows_tsv" ]]; then
    echo -e "${DIM}  (no slices yet — run not started, or queryfile not loaded)${NC}"
    return
  fi
  echo -e "${CYAN}  #      Status                 Target table               App                      Notes${NC}"
  echo "  ------------------------------------------------------------------------------------------------------------"
  local ro st tt ap no sc shown=0 total_rows=0
  while IFS=$'\t' read -r ro st tt ap no; do
    [[ -z "$ro" ]] && continue
    total_rows=$((total_rows + 1))
    [[ $shown -ge 60 ]] && continue
    sc="$NC"
    case "$st" in
      NEW)                  sc="$BLUE" ;;
      RUNNING)              sc="$YELLOW" ;;
      COMPLETED)            sc="$GREEN" ;;
      FAILED)               sc="$RED" ;;
      COMPLETED-FAILEDDROP) sc="$MAGENTA" ;;
    esac
    printf "  %-6s ${sc}%-22s${NC} %-26s %-24s %s\n" "$ro" "$st" "$tt" "$ap" "$no"
    shown=$((shown + 1))
  done <<< "$rows_tsv"
  if [[ $total_rows -gt $shown ]]; then
    echo -e "${DIM}  … and $((total_rows - shown)) more rows (option 3 shows failures only)${NC}"
  fi
  echo ""
  echo -e "${DIM}  Status legend: ${BLUE}NEW${NC} ${YELLOW}RUNNING${NC} ${GREEN}COMPLETED${NC} ${RED}FAILED${NC} ${MAGENTA}COMPLETED-FAILEDDROP${NC}"
}

# ======================== WALKTHROUGH / STAGE DETECTION ========================
# Map the ST_* status globals (populated by load_status) + a queryfile file check
# onto a lifecycle position. Sets WT_STAGE (BACKEND/QUERYFILE/RUN/MONITOR/VERIFY),
# WT_HINT (one-line "next step"), and WT_FIRST_RUN=1 on a true cold start.
# No new engine calls — pure reuse of what load_status already fetched.
detect_stage() {
  local qf="${ST_QUERYFILE:-queryfile.txt}"
  local qf_ok=0
  [[ -s "$qf" ]] && qf_ok=1
  WT_FIRST_RUN=0
  if [[ "$qf_ok" -eq 0 && "${ST_TOTAL:-0}" -eq 0 ]]; then
    WT_STAGE="QUERYFILE"; WT_FIRST_RUN=1
    WT_HINT="Generate a queryfile (you have none yet) — try the guided walkthrough"
  elif [[ "$qf_ok" -eq 0 ]]; then
    WT_STAGE="QUERYFILE"
    WT_HINT="Generate a queryfile (option 9 split, or 10 probe)"
  elif [[ "${ST_TOTAL:-0}" -eq 0 ]]; then
    WT_STAGE="RUN"
    WT_HINT="Queryfile ready — run the load (option 13)"
  elif [[ "$ST_STATE" == "IN PROGRESS" || "${ST_RUNNING:-0}" -gt 0 || "${ST_NEW:-0}" -gt 0 ]]; then
    WT_STAGE="MONITOR"
    WT_HINT="Load in progress — watch it (option 11 board)"
  elif [[ "$ST_STATE" == "FINISHED" ]]; then
    WT_STAGE="VERIFY"
    WT_HINT="Load finished — verify completeness (option 14 reconcile)"
  else
    WT_STAGE="QUERYFILE"
    WT_HINT="Generate a queryfile to begin"
  fi
  # A FAILED slice trumps the stage hint regardless of where we are.
  if [[ "${ST_FAILED:-0}" -gt 0 ]]; then
    WT_HINT="${ST_FAILED} slice(s) FAILED — reset them (option 4), then re-run"
  fi
}

# One cell of the lifecycle map: highlighted (▶ bold white) when it's the current
# stage, DIM otherwise. $1 label, $2 stage-key, $3 current stage-key.
_wt_cell() {
  if [[ "$2" == "$3" ]]; then
    printf '%b' "${WHITE}${BOLD}▶ $1${NC}"
  else
    printf '%b' "${DIM}$1${NC}"
  fi
}

# Print the five-stage lifecycle path with $1 (a WT_STAGE key) highlighted.
render_lifecycle_map() {
  local cur="$1" a
  a=$(printf '%b' " ${DIM}→${NC} ")
  echo -e "  $(_wt_cell Backend BACKEND "$cur")${a}$(_wt_cell Queryfile QUERYFILE "$cur")${a}$(_wt_cell Run RUN "$cur")${a}$(_wt_cell Monitor MONITOR "$cur")${a}$(_wt_cell Verify VERIFY "$cur")"
}

# ======================== ACTIONS ========================
action_dashboard() {
  print_header "Run Status Dashboard"
  load_status
  render_status_line
  echo ""
  render_slice_table
}

action_watch() {
  local interval=15
  while true; do
    clear
    print_header "Watch Live — refresh ${interval}s"
    load_status
    render_status_line
    echo ""
    render_slice_table
    echo ""
    echo -e "${DIM}Auto-refresh in ${interval}s — press any key to return to the menu.${NC}"
    # read returns non-zero on timeout (keep looping); a keypress breaks out.
    if read -r -t "$interval" -n 1 _; then
      break
    fi
  done
}

action_failed() {
  print_header "Failed Slice Details"
  python manage.py status --failed
  report_rc "$?" "Listed failed slices." "Could not read run status."
}

action_reset() {
  print_header "Reset Failed Slices → NEW"
  load_status
  if [[ "${ST_FAILED:-0}" -eq 0 && "${ST_FAILEDDROP:-0}" -eq 0 ]]; then
    print_info "No FAILED slices to reset for run ${ST_RUN_ID}."
    return
  fi
  echo -e "  This re-queues ${RED}FAILED${NC} slices back to ${BLUE}NEW${NC} so the next"
  echo -e "  ${DIM}python main.py${NC} run will redo them. History is preserved."
  echo ""
  if ! confirm "Reset ${ST_FAILED} FAILED slice(s) for run ${ST_RUN_ID}?"; then
    print_warning "Reset cancelled."
    return
  fi
  local extra=""
  if [[ "${ST_FAILEDDROP:-0}" -gt 0 ]]; then
    if confirm "Also reset ${ST_FAILEDDROP} COMPLETED-FAILEDDROP slice(s)?"; then
      extra="--include-faileddrop"
    fi
  fi
  python manage.py reset --yes $extra
  report_rc "$?" "Failed slices reset to NEW." "Reset failed."
}

action_clear_retire() {
  print_header "Clear Run (retire — keeps history)"
  load_status
  echo -e "  Retires every current row for run ${WHITE}${ST_RUN_ID}${NC}"
  echo -e "  ${DIM}(iscurrentrow=FALSE)${NC} — the data stays in the backend for history."
  echo ""
  if ! confirm "Retire ${ST_TOTAL} row(s) for run ${ST_RUN_ID}?"; then
    print_warning "Clear cancelled."
    return
  fi
  python manage.py clear --yes
  report_rc "$?" "Run retired." "Clear failed."
}

action_clear_hard() {
  print_header "Clear Run (HARD DELETE)"
  load_status
  echo -e "  ${RED}WARNING: permanently DELETES every row for run ${ST_RUN_ID}.${NC}"
  echo -e "  ${RED}This cannot be undone.${NC}"
  echo ""
  local confirm_txt
  read -rp "Type 'DELETE' to confirm: " confirm_txt
  if [[ "$confirm_txt" != "DELETE" ]]; then
    print_warning "Hard delete cancelled."
    return
  fi
  python manage.py clear --hard --yes
  report_rc "$?" "Run hard-deleted." "Hard delete failed."
}

action_logs() {
  print_header "Recent Log (last 80 lines)"
  python manage.py logs --lines 80
  report_rc "$?" "Showed recent log." "No log to show."
}

action_tail_errors() {
  print_header "Tail Errors (live — Ctrl-C to stop)"
  print_info "Following the log, error/fail/exception lines only…"
  echo ""
  python manage.py logs --follow --errors
  # follow exits 0 on Ctrl-C (engine traps KeyboardInterrupt).
  print_success "Stopped tailing."
}

# ---- Split wizard (replaces the raw argparse dump) ----
action_split_wizard() {
  print_header "Generate Queryfile — Guided Split Wizard"
  echo -e "  Splits one driving query into N parallel slices by ROWID range or by"
  echo -e "  partition, writing ${BLUE}${ST_QUERYFILE:-queryfile.txt}${NC} for the loader."
  echo ""

  # Step 0: source engine (oracle default = byte-identical legacy command)
  prompt_source_engine
  echo ""

  # Step 1: query file
  echo -e "${CYAN}Step 1: Source query file${NC}"
  echo -e "  ${DIM}Should contain a ${NC}${YELLOW}~SPLIT~${NC}${DIM} token where the slice predicate goes,${NC}"
  echo -e "  ${DIM}and reference the driving table as ${NC}${YELLOW}OWNER.TABLE alias${NC}${DIM} for ROWID joins.${NC}"
  local qfile
  read -rp "  Query file path (blank to cancel): " qfile
  [[ -z "$qfile" ]] && { print_warning "Cancelled."; return; }
  if [[ ! -f "$qfile" ]]; then
    print_error "No file at: $qfile"
    return
  fi
  if ! grep -q '~SPLIT~' "$qfile" 2>/dev/null; then
    print_warning "File has no ~SPLIT~ token — the splitter will append a WHERE/AND predicate instead."
  fi
  echo ""

  # Step 2: driving table
  echo -e "${CYAN}Step 2: Driving table${NC}"
  local table
  prompt_owner_table "  OWNER.TABLE (required, blank to cancel): " || { print_warning "Cancelled."; return; }
  table="$PROMPT_RESULT"
  echo ""

  # Step 3: target table
  echo -e "${CYAN}Step 3: Target table${NC}"
  local target
  prompt_owner_table "  OWNER.TARGET (required, blank to cancel): " || { print_warning "Cancelled."; return; }
  target="$PROMPT_RESULT"
  echo ""

  # Pre-fill strategy/column/chunks from the probe recommendation, if present.
  local rec_path rec_strategy="" rec_key="" rec_chunks=""
  rec_path=$(python -c "import config, os; print(os.path.join(os.path.dirname(config.LOG_OUTPUT_PATH) or '.', 'probe_recommendation.json'))" 2>/dev/null || echo "logging/probe_recommendation.json")
  if [[ -f "$rec_path" ]]; then
    local rec_parsed
    rec_parsed=$(REC_PATH="$rec_path" python - <<'PY' 2>/dev/null || true
import json, os
try:
    with open(os.environ.get("REC_PATH", "")) as f:
        d = json.load(f)
    print(d.get("strategy", "") or "")
    print(d.get("key", "") or "")
    print(d.get("chunk_count", "") or "")
except Exception:
    print(""); print(""); print("")
PY
)
    { IFS= read -r rec_strategy; IFS= read -r rec_key; IFS= read -r rec_chunks; } <<< "$rec_parsed"
    [[ -n "$rec_strategy" ]] && print_info "Probe recommendation: strategy=${rec_strategy} column=${rec_key:-n/a} chunks=${rec_chunks:-?}"
  fi

  # Step 4: chunks
  echo -e "${CYAN}Step 4: Number of slices (chunks)${NC}"
  local chunks
  prompt_int "  Chunks [${rec_chunks:-16}]: " "${rec_chunks:-16}"
  chunks="$PROMPT_RESULT"
  echo ""

  # Step 5: strategy
  echo -e "${CYAN}Step 5: Split strategy${NC}"
  echo -e "  ${DIM}auto = partition if the table is partitioned, else ROWID ranges${NC}"
  echo -e "  ${DIM}column = range-split on a numeric/date column (insert-safe; needs --column)${NC}"
  local strategy
  prompt_enum "  Strategy auto/rowid/partition/column [${rec_strategy:-auto}]: " "${rec_strategy:-auto}" "auto rowid partition column"
  strategy="$PROMPT_RESULT"
  local split_col=""
  if [[ "$strategy" == "column" ]]; then
    prompt_opt_ident "  Split column [${rec_key}]: " ; split_col="${PROMPT_RESULT:-$rec_key}"
    [[ -z "$split_col" ]] && { print_error "Column strategy needs a column — aborting."; return; }
  fi
  if [[ "$strategy" == "rowid" || "$strategy" == "partition" ]]; then
    print_warning "ROWID/partition boundaries freeze at split time; rows inserted before the load-start watermark can be missed on an append-heavy source. Prefer 'column' if the table is insert-heavy."
  fi
  echo ""

  # Step 6: options
  echo -e "${CYAN}Step 6: Options${NC}"
  local alias_in subpart explain assort
  prompt_opt_ident "  Table alias (optional, blank = auto-detect): " ; alias_in="$PROMPT_RESULT"
  subpart="n"; confirm "  Use SUBPARTITIONS (partition strategy only)?" && subpart="y"
  explain="y"
  confirm_yes "  Run EXPLAIN PLAN on slice #1 (recommended)?" || explain="n"
  assort="n"; confirm "  Also build queryfile-assorted.txt (--assort)?" && assort="y"
  echo ""

  # Step 7: summary panel
  local alias_disp="${alias_in:-auto-detect}"
  local sub_disp explain_disp assort_disp
  [[ "$subpart" == "y" ]] && sub_disp="${GREEN}yes${NC}" || sub_disp="${DIM}no${NC}"
  [[ "$explain" == "y" ]] && explain_disp="${GREEN}yes${NC}" || explain_disp="${DIM}no${NC}"
  [[ "$assort" == "y" ]] && assort_disp="${GREEN}yes${NC}" || assort_disp="${DIM}no${NC}"
  echo ""
  echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
  echo -e "${CYAN}║${NC}  ${WHITE}${BOLD}Split Configuration${NC}"
  echo -e "${CYAN}╠══════════════════════════════════════════════════════════╣${NC}"
  echo -e "${CYAN}║${NC}  Query file:     ${GREEN}${qfile}${NC}"
  echo -e "${CYAN}║${NC}  Driving table:  ${GREEN}${table}${NC}"
  echo -e "${CYAN}║${NC}  Target table:   ${GREEN}${target}${NC}"
  echo -e "${CYAN}║${NC}  Chunks:         ${YELLOW}${chunks}${NC}"
  echo -e "${CYAN}║${NC}  Strategy:       ${YELLOW}${strategy}${NC}${split_col:+  ${DIM}(column: ${split_col})${NC}}"
  echo -e "${CYAN}║${NC}  Alias:          ${DIM}${alias_disp}${NC}"
  echo -e "${CYAN}║${NC}  Subpartitions:  ${sub_disp}"
  echo -e "${CYAN}║${NC}  Explain plan:   ${explain_disp}"
  echo -e "${CYAN}║${NC}  Assort output:  ${assort_disp}"
  echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
  echo ""

  if ! confirm "Run the split with these settings?"; then
    print_warning "Split cancelled."
    return
  fi

  # Step 8: build + run
  local cmd=(python manage.py split --query-file "$qfile" --table "$table"
             --target "$target" --chunks "$chunks" --strategy "$strategy")
  [[ "${SE_ENGINE:-oracle}" != "oracle" ]] && cmd+=(--source-engine "$SE_ENGINE")
  [[ -n "$alias_in" ]] && cmd+=(--alias "$alias_in")
  [[ "$strategy" == "column" && -n "$split_col" ]] && cmd+=(--column "$split_col")
  [[ "$subpart" == "y" ]] && cmd+=(--subpartitions)
  [[ "$explain" == "y" ]] && cmd+=(--explain)
  [[ "$assort" == "y" ]] && cmd+=(--assort)

  echo -e "${DIM}\$ ${cmd[*]}${NC}"
  echo ""
  local rc out="${ST_QUERYFILE:-queryfile.txt}"
  while true; do
    "${cmd[@]}"
    rc=$?
    echo ""
    if [[ $rc -eq 0 ]]; then
      print_success "Queryfile generated."
      if [[ -f "$out" ]]; then
        echo -e "${DIM}First lines of ${out}:${NC}"
        head -n 8 "$out"
      fi
      break
    fi
    # rc==3 → offer setup + retry; anything else → driver hint and stop.
    handle_engine_failure "$rc" "${SE_ENGINE:-oracle}" || break
    echo -e "${DIM}\$ ${cmd[*]}${NC}"
    echo ""
  done
}

# ---- Probe & optimize wizard ----
action_probe_wizard() {
  print_header "Probe & Optimize — Recommend Best Split"
  echo -e "  Races ROWID vs column-range split predicates on a bounded sample and"
  echo -e "  recommends the predicate, chunk count, and concurrency."
  echo -e "  ${DIM}The probe only RECOMMENDS — it does not modify any data.${NC}"
  echo ""

  # Step 0: source engine (oracle default = byte-identical legacy command)
  prompt_source_engine
  echo ""

  # Step 1: query file
  echo -e "${CYAN}Step 1: Source query file${NC}"
  echo -e "  ${DIM}Should contain a ${NC}${YELLOW}~SPLIT~${NC}${DIM} token where the slice predicate goes.${NC}"
  local qfile
  read -rp "  Query file path (blank to cancel): " qfile
  [[ -z "$qfile" ]] && { print_warning "Cancelled."; return; }
  if [[ ! -f "$qfile" ]]; then
    print_error "No file at: $qfile"
    return
  fi
  if ! grep -q '~SPLIT~' "$qfile" 2>/dev/null; then
    print_warning "File has no ~SPLIT~ token — the probe will still run but split predicates may not apply cleanly."
  fi
  echo ""

  # Step 2: driving table
  echo -e "${CYAN}Step 2: Driving table${NC}"
  local table
  prompt_owner_table "  OWNER.TABLE (required, blank to cancel): " || { print_warning "Cancelled."; return; }
  table="$PROMPT_RESULT"
  echo ""

  # Step 3: depth
  echo -e "${CYAN}Step 3: Probe depth${NC}"
  echo -e "  ${DIM}lightweight = fastest (ROWID-only, no bakeoff)${NC}"
  echo -e "  ${DIM}bakeoff     = race ROWID vs column-range (recommended)${NC}"
  echo -e "  ${DIM}adaptive    = bakeoff + sampling-driven tuning${NC}"
  local depth
  prompt_enum "  Depth lightweight/bakeoff/adaptive [bakeoff]: " "bakeoff" "lightweight bakeoff adaptive"
  depth="$PROMPT_RESULT"
  echo ""

  # Step 4: optional alias
  echo -e "${CYAN}Step 4: Options${NC}"
  local alias_in
  prompt_opt_ident "  Table alias (optional, blank = auto-detect): " ; alias_in="$PROMPT_RESULT"
  echo ""

  # Step 5: probe tunables. Each prompt shows its CONFIGURED value (from config.py /
  # environment) in [brackets]; pressing Enter keeps it. Surfaced directly instead of
  # hidden behind an "advanced?" gate so a first-time operator sees exactly what drives
  # the recommendation — target-slice-seconds is the knob that decides the chunk count.
  echo -e "${CYAN}Step 5: Probe tunables${NC}"
  echo -e "  ${DIM}Press Enter on any line to keep the shown value (from config.py / your environment).${NC}"
  echo ""

  # Read the configured values once so the prompts can show real numbers, not "(default)".
  local _cfg d_tgt d_samp d_time d_conc d_pdeg d_pruns
  _cfg=$(python - <<'PY' 2>/dev/null
import config
print(config.PROBE_TARGET_SLICE_SECONDS)
print(config.PROBE_SAMPLE_ROWS)
print(config.PROBE_TIME_BUDGET_SECONDS)
print(config.PROBE_MAX_CONCURRENCY)
print(",".join(str(x) for x in config.PROBE_PARALLEL_DEGREES))
print(config.PROBE_PARALLEL_RUNS)
PY
)
  {
    IFS= read -r d_tgt
    IFS= read -r d_samp
    IFS= read -r d_time
    IFS= read -r d_conc
    IFS= read -r d_pdeg
    IFS= read -r d_pruns
  } <<< "$_cfg"
  # Fall back to config.py's documented defaults if the read failed (keeps the wizard usable).
  : "${d_tgt:=600}" "${d_samp:=100000}" "${d_time:=20}" "${d_conc:=32}"
  : "${d_pdeg:=1,2,4,8}" "${d_pruns:=1}"

  local tgt_slice samp_rows time_budget max_conc
  echo -e "  ${DIM}Target slice seconds: how long you want EACH chunk to take.${NC}"
  echo -e "  ${DIM}  Lower this for MORE (smaller) chunks; raise it for fewer, bigger chunks.${NC}"
  prompt_int "  Target slice seconds [${d_tgt}]: " "$d_tgt" ; tgt_slice="$PROMPT_RESULT"
  echo ""
  echo -e "  ${DIM}Sample rows: rows read per candidate while timing the race (bigger = more accurate, slower probe).${NC}"
  prompt_int "  Sample rows [${d_samp}]: " "$d_samp" ; samp_rows="$PROMPT_RESULT"
  echo ""
  echo -e "  ${DIM}Time budget seconds: cap on time spent timing each candidate slice.${NC}"
  prompt_int "  Time budget seconds [${d_time}]: " "$d_time" ; time_budget="$PROMPT_RESULT"
  echo ""
  echo -e "  ${DIM}Max concurrency: only used by 'adaptive' depth — most parallel connections it will test.${NC}"
  prompt_int "  Max concurrency [${d_conc}]: " "$d_conc" ; max_conc="$PROMPT_RESULT"
  echo ""

  # Oracle PARALLEL-degree sweep (Oracle only — other engines have no inline hint).
  local do_psweep=0 pdeg="$d_pdeg" pruns="$d_pruns"
  if [[ "${SE_ENGINE:-oracle}" == "oracle" ]]; then
    echo -e "  ${DIM}Parallel sweep: also race Oracle PARALLEL(n) degrees on the winning split and${NC}"
    echo -e "  ${DIM}  recommend a degree. Measures BOTH server scan speed and end-to-end fetch;${NC}"
    echo -e "  ${DIM}  it only recommends parallel if the fetch (what the load actually pays) speeds up.${NC}"
    if confirm "  Run the parallel-degree sweep?"; then
      do_psweep=1
      # Degrees: comma-separated positive ints; blank keeps the configured default.
      local _pin re_deg='^[1-9][0-9]*(,[1-9][0-9]*)*$'
      while true; do
        read -rp "  Degrees to test [${d_pdeg}]: " _pin
        _pin="${_pin// /}"; _pin="${_pin:-$d_pdeg}"
        [[ "$_pin" =~ $re_deg ]] && { pdeg="$_pin"; break; }
        print_warning "Enter comma-separated whole numbers, e.g. 1,2,4,8."
      done
      echo -e "  ${DIM}Runs per degree: set 3 for a thorough, noise-resistant test (first run is warm-up).${NC}"
      prompt_int "  Runs per degree [${d_pruns}]: " "$d_pruns" ; pruns="$PROMPT_RESULT"
    fi
    echo ""
  fi

  # Build and run the probe
  local cmd=(python manage.py probe --query-file "$qfile" --table "$table" --depth "$depth")
  [[ "${SE_ENGINE:-oracle}" != "oracle" ]] && cmd+=(--source-engine "$SE_ENGINE")
  [[ -n "$alias_in" ]]    && cmd+=(--alias "$alias_in")
  [[ -n "$tgt_slice" ]]   && cmd+=(--target-slice-seconds "$tgt_slice")
  [[ -n "$samp_rows" ]]   && cmd+=(--sample-rows "$samp_rows")
  [[ -n "$time_budget" ]] && cmd+=(--time-budget-seconds "$time_budget")
  [[ -n "$max_conc" ]]    && cmd+=(--max-concurrency "$max_conc")
  if [[ "$do_psweep" == "1" ]]; then
    cmd+=(--parallel-sweep --parallel-degrees "$pdeg" --parallel-runs "$pruns")
  fi

  echo -e "${DIM}\$ ${cmd[*]}${NC}"
  echo ""
  local probe_rc
  while true; do
    "${cmd[@]}"
    probe_rc=$?
    echo ""
    [[ $probe_rc -eq 0 ]] && break
    # rc==3 → offer setup + retry; anything else → driver hint and stop.
    handle_engine_failure "$probe_rc" "${SE_ENGINE:-oracle}" || return
    echo -e "${DIM}\$ ${cmd[*]}${NC}"
    echo ""
  done

  print_success "Probe complete. Recommendation shown above."
  echo ""

  # Resolve sidecar path (same formula as manage.py: dirname(LOG_OUTPUT_PATH)/probe_recommendation.json)
  local rec_path
  rec_path=$(python -c "import config, os; print(os.path.join(os.path.dirname(config.LOG_OUTPUT_PATH) or '.', 'probe_recommendation.json'))" 2>/dev/null || echo "logging/probe_recommendation.json")

  if ! confirm "Apply this recommendation now (generate the queryfile via split)?"; then
    print_info "Recommendation saved to: $rec_path"
    print_info "Run option 9 (split wizard — now column-aware) later to apply it; it pre-fills strategy/column/chunks from this recommendation."
    return
  fi

  # Parse probe_recommendation.json — data via env var, program via heredoc (same pattern as load_status)
  local rec_parsed
  rec_parsed=$(REC_PATH="$rec_path" python - <<'PY' 2>/dev/null || true
import json, os
path = os.environ.get("REC_PATH", "")
try:
    with open(path) as f:
        d = json.load(f)
    print(d.get("strategy", "rowid"))
    print(d.get("key", "") or "")
    print(d.get("chunk_count", 16))
    print(d.get("parallel_degree", 1) or 1)
except Exception:
    print("rowid")
    print("")
    print("16")
    print("1")
PY
)
  local strategy="" key="" chunks="" pdegree=""
  {
    IFS= read -r strategy
    IFS= read -r key
    IFS= read -r chunks
    IFS= read -r pdegree
  } <<< "$rec_parsed"
  : "${strategy:=rowid}" "${chunks:=16}" "${pdegree:=1}"

  # Let the operator ACCEPT or OVERRIDE the recommendation before it is applied.
  # Each prompt shows the recommended value in [brackets]; press Enter to keep it.
  echo -e "${CYAN}Review the recommendation${NC}"
  echo -e "  ${DIM}Probe recommends: strategy=${GREEN}${strategy}${NC}${DIM}  column=${key:-n/a}  chunks=${chunks}${NC}"
  echo -e "  ${DIM}Press Enter to accept each value, or type a different one to override.${NC}"
  echo -e "  ${DIM}  rowid = block-range (best for whole-table SELECT *) · column = range on a key column${NC}"
  echo -e "  ${DIM}  partition = one slice per partition · auto = let the splitter decide${NC}"
  prompt_enum "  Strategy rowid/column/partition/auto [${strategy}]: " "$strategy" "rowid column partition auto"
  strategy="$PROMPT_RESULT"
  if [[ "$strategy" == "column" ]]; then
    # Column strategy needs a key column; default to the recommended one (may be empty).
    local kin re_col='^[A-Za-z0-9_$#.]+$'
    while true; do
      read -rp "  Column key [${key:-required}]: " kin
      kin="${kin:-$key}"
      [[ -z "$kin" ]] && { print_warning "Column strategy needs a key column — type one."; continue; }
      [[ "$kin" =~ $re_col ]] && { key="$kin"; break; }
      print_warning "Enter a plain column name (letters, digits, _ \$ #)."
    done
  else
    key=""
  fi
  prompt_int "  Chunks [${chunks}]: " "$chunks" ; chunks="$PROMPT_RESULT"
  # Oracle PARALLEL degree to bake into each generated SELECT (1 = off). Only prompt
  # for Oracle; the recommended value is 1 unless the probe's parallel sweep found a win.
  if [[ "${SE_ENGINE:-oracle}" == "oracle" ]]; then
    echo -e "  ${DIM}Parallel degree: injects /*+ PARALLEL(n) */ into every generated SELECT (1 = off).${NC}"
    prompt_int "  Parallel degree [${pdegree}]: " "$pdegree" ; pdegree="$PROMPT_RESULT"
  else
    pdegree=1
  fi
  echo ""

  # Target table (probe doesn't know it)
  echo -e "${CYAN}Target table for split${NC}"
  local target
  prompt_owner_table "  OWNER.TGT (required, blank to cancel): " || { print_warning "Apply cancelled."; return; }
  target="$PROMPT_RESULT"

  local do_explain="n"
  confirm "Run EXPLAIN PLAN on slice #1?" && do_explain="y"
  echo ""

  # Build split command
  local split_cmd=(python manage.py split
    --query-file "$qfile"
    --table "$table"
    --target "$target"
    --strategy "$strategy"
    --chunks "$chunks")
  [[ "${SE_ENGINE:-oracle}" != "oracle" ]] && split_cmd+=(--source-engine "$SE_ENGINE")
  [[ -n "$alias_in" ]] && split_cmd+=(--alias "$alias_in")
  [[ "$strategy" == "column" && -n "$key" ]] && split_cmd+=(--column "$key")
  [[ -n "$pdegree" && "$pdegree" -gt 1 ]] && split_cmd+=(--parallel "$pdegree")
  [[ "$do_explain" == "y" ]] && split_cmd+=(--explain)

  echo -e "${DIM}\$ ${split_cmd[*]}${NC}"
  echo ""
  "${split_cmd[@]}"
  local split_rc=$?
  echo ""
  report_rc "$split_rc" "Queryfile generated." "Split failed."

  if [[ $split_rc -eq 0 ]]; then
    local out="${ST_QUERYFILE:-queryfile.txt}"
    if [[ -f "$out" ]]; then
      echo -e "${DIM}First lines of ${out}:${NC}"
      head -n 8 "$out"
    fi
  fi
}

# ---- Build wizard: paste many queries -> one queryfile ----
# Paste queries (SQL|TARGET), one per line, blank = done. Splittable lines
# (~SPLIT~) are probed+split; plain lines are appended verbatim.
action_build_wizard() {
  print_header "Build Queryfile — Paste Your Queries"
  echo -e "  Paste queries and this builds ${BLUE}${ST_QUERYFILE:-queryfile.txt}${NC} in one pass:"
  echo -e "  ${DIM}lines with a ${NC}${YELLOW}~SPLIT~${NC}${DIM} token are probed + split into slices;${NC}"
  echo -e "  ${DIM}lines without one are appended verbatim as a single load job.${NC}"
  echo ""

  # Step 0: source engine
  prompt_source_engine
  echo ""

  # Step 1: split parameter (the SQL/TARGET field delimiter)
  echo -e "${CYAN}Step 1: Split parameter${NC}"
  echo -e "  ${DIM}Character that separates the query from its target, e.g. ${NC}${YELLOW}SQL|TARGET${NC}${DIM}.${NC}"
  echo -e "  ${DIM}Pick something else if your SQL itself contains ${NC}${YELLOW}|${NC}${DIM} (e.g. Oracle ${NC}${YELLOW}||${NC}${DIM} concatenation).${NC}"
  local qdelim
  read -rp "  Split parameter [|]: " qdelim
  qdelim=${qdelim:-|}
  echo ""

  # Step 2: paste loop
  echo -e "${CYAN}Step 2: Paste your queries${NC}"
  echo -e "  ${DIM}Format: ${NC}${YELLOW}SQL${qdelim}TARGET${NC}${DIM} — one query per line. Blank line = done.${NC}"
  echo -e "  ${DIM}No ${NC}${YELLOW}${qdelim}${NC}${DIM} in a line? I'll ask you for the target after.${NC}"
  echo -e "  ${DIM}A line like ${NC}${YELLOW}@/path/to/file${NC}${DIM} imports that file's lines.${NC}"
  echo ""
  local -a lines=()
  while true; do
    local ln
    IFS= read -rp "  q$(( ${#lines[@]} + 1 ))> " ln || break
    [[ -z "$ln" ]] && break
    if [[ "$ln" == @* ]]; then
      local f="${ln:1}" before="${#lines[@]}"
      if [[ -f "$f" ]]; then
        local fl
        while IFS= read -r fl; do [[ -n "$fl" ]] && lines+=("$fl"); done < "$f"
        print_info "Imported $(( ${#lines[@]} - before )) line(s) from $f"
      else
        print_error "No file at: $f"
      fi
      continue
    fi
    lines+=("$ln")
  done
  if [[ ${#lines[@]} -eq 0 ]]; then
    print_warning "No queries pasted — cancelled."
    return
  fi
  echo ""

  # Step 3: every line needs a target — ask for the ones that didn't paste
  # SQL<delim>TARGET inline, then normalize everything to the pipe the
  # downstream parser (config.QUERY_FILE_DELIMITER) expects.
  echo -e "${CYAN}Step 3: Resolve targets${NC}"
  local -a resolved=()
  for ln in "${lines[@]}"; do
    if [[ "$ln" == *"$qdelim"* ]]; then
      if [[ "$qdelim" != "|" ]]; then
        ln="${ln%%"$qdelim"*}|${ln#*"$qdelim"}"
      fi
      resolved+=("$ln")
    else
      echo -e "  ${DIM}${ln:0:64}$([[ ${#ln} -gt 64 ]] && echo …)${NC}"
      local tgt
      prompt_owner_table "    No '$qdelim' detected — target schema.table: " || {
        print_error "A target schema.table is required — cancelled."
        return
      }
      tgt="$PROMPT_RESULT"
      resolved+=("${ln}|${tgt}")
    fi
  done
  lines=("${resolved[@]}")
  echo ""

  # Step 4: classify each line + collect driving table for splittable ones
  local -a needs=() tbls=() aliases=()
  local n_split=0 n_pass=0 i=0
  echo -e "${CYAN}Step 4: Driving tables for splittable queries${NC}"
  for ln in "${lines[@]}"; do
    i=$(( i + 1 ))
    if [[ "$ln" == *"~SPLIT~"* ]]; then
      n_split=$(( n_split + 1 ))
      echo -e "  ${YELLOW}~SPLIT~${NC} q${i}: ${DIM}${ln:0:64}$([[ ${#ln} -gt 64 ]] && echo …)${NC}"
      local t a
      prompt_owner_table "    Driving OWNER.TABLE: " || {
        print_error "Splittable query needs a driving table — cancelled."
        return
      }
      t="$PROMPT_RESULT"
      prompt_opt_ident "    Alias (blank = auto-detect): " ; a="$PROMPT_RESULT"
      needs+=("1"); tbls+=("$t"); aliases+=("$a")
    else
      n_pass=$(( n_pass + 1 ))
      needs+=("0"); tbls+=(""); aliases+=("")
    fi
  done
  echo ""

  # Step 5: probe vs global strategy (only relevant if there are splittable lines)
  local probe_mode=1 depth="bakeoff" gstrategy="auto" gchunks="16"
  if (( n_split > 0 )); then
    echo -e "${CYAN}Step 5: Splitting mode${NC}"
    if confirm_yes "  Probe each splittable query to auto-pick strategy/chunks?"; then
      probe_mode=1
      echo -e "  ${DIM}lightweight = fastest · bakeoff = race ROWID vs column · adaptive = + tuning${NC}"
      prompt_enum "  Probe depth lightweight/bakeoff/adaptive [bakeoff]: " "bakeoff" "lightweight bakeoff adaptive"
      depth="$PROMPT_RESULT"
    else
      probe_mode=0
      prompt_enum "  Strategy auto/rowid/partition/column [auto]: " "auto" "auto rowid partition column"
      gstrategy="$PROMPT_RESULT"
      prompt_int "  Chunks [16]: " "16" ; gchunks="$PROMPT_RESULT"
    fi
    echo ""
  fi

  # Step 6: options
  local assort="n" explain="n"
  echo -e "${CYAN}Step 6: Options${NC}"
  confirm "  Also build queryfile-assorted.txt (--assort)?" && assort="y"
  if (( n_split > 0 )); then
    confirm "  Run EXPLAIN PLAN on slice #1 of each split?" && explain="y"
  fi
  echo ""

  # Step 7: summary + confirm
  local out="${ST_QUERYFILE:-queryfile.txt}"
  local mode_disp
  if (( n_split == 0 )); then
    mode_disp="${DIM}pass-through only${NC}"
  elif [[ "$probe_mode" == "1" ]]; then
    mode_disp="${GREEN}probe (${depth})${NC}"
  else
    mode_disp="${YELLOW}${gstrategy} x${gchunks}${NC}"
  fi
  echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
  echo -e "${CYAN}║${NC}  ${WHITE}${BOLD}Build Configuration${NC}"
  echo -e "${CYAN}╠══════════════════════════════════════════════════════════╣${NC}"
  echo -e "${CYAN}║${NC}  Queries:        ${YELLOW}${#lines[@]}${NC}  ${DIM}(${n_split} splittable, ${n_pass} pass-through)${NC}"
  echo -e "${CYAN}║${NC}  Split mode:     ${mode_disp}"
  echo -e "${CYAN}║${NC}  Source engine:  ${GREEN}${SE_ENGINE:-oracle}${NC}"
  echo -e "${CYAN}║${NC}  Output:         ${GREEN}${out}${NC}"
  echo -e "${CYAN}║${NC}  Assort:         $([[ "$assort" == "y" ]] && echo -e "${GREEN}yes${NC}" || echo -e "${DIM}no${NC}")"
  echo -e "${CYAN}║${NC}  Explain:        $([[ "$explain" == "y" ]] && echo -e "${GREEN}yes${NC}" || echo -e "${DIM}no${NC}")"
  echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
  echo ""
  if ! confirm "Build the queryfile with these settings?"; then
    print_warning "Build cancelled."
    return
  fi

  # Step 8: write the manifest (NUL-delimited -> python for safe JSON encoding)
  local raw_file manifest
  raw_file=$(mktemp "${TMPDIR:-/tmp}/il-batch-raw.XXXXXX") || { print_error "mktemp failed."; return; }
  manifest=$(mktemp "${TMPDIR:-/tmp}/il-batch-manifest.XXXXXX") || { print_error "mktemp failed."; rm -f "$raw_file"; return; }
  local idx=0
  for ln in "${lines[@]}"; do
    printf '%s\0%s\0%s\0%s\0' "$ln" "${needs[$idx]}" "${tbls[$idx]}" "${aliases[$idx]}" >> "$raw_file"
    idx=$(( idx + 1 ))
  done
  if ! RAW="$raw_file" OUT="$manifest" python - <<'PY'
import json, os
data = open(os.environ["RAW"], "rb").read().decode("utf-8", "replace").split("\0")
if data and data[-1] == "":
    data.pop()
entries = []
for i in range(0, len(data), 4):
    line, ns, tbl, al = data[i], data[i + 1], data[i + 2], data[i + 3]
    e = {"line": line, "needs_split": ns == "1"}
    if tbl:
        e["table"] = tbl
    if al:
        e["alias"] = al
    entries.append(e)
with open(os.environ["OUT"], "w") as f:
    json.dump(entries, f)
PY
  then
    print_error "Failed to build manifest."
    rm -f "$raw_file" "$manifest"
    return
  fi

  # Step 9: build + run split-batch
  local cmd=(python manage.py split-batch --manifest "$manifest" --output "$out")
  [[ "${SE_ENGINE:-oracle}" != "oracle" ]] && cmd+=(--source-engine "$SE_ENGINE")
  if [[ "$probe_mode" == "1" ]]; then
    cmd+=(--depth "$depth")
  else
    cmd+=(--no-probe --strategy "$gstrategy" --chunks "$gchunks")
  fi
  [[ "$assort" == "y" ]]  && cmd+=(--assort)
  [[ "$explain" == "y" ]] && cmd+=(--explain)

  echo -e "${DIM}\$ ${cmd[*]}${NC}"
  echo ""
  # Keep the manifest until the loop ends — a rc==3 retry re-runs this command.
  local rc
  while true; do
    "${cmd[@]}"
    rc=$?
    echo ""
    if [[ $rc -eq 0 ]]; then
      print_success "Queryfile built."
      if [[ -f "$out" ]]; then
        echo -e "${DIM}First lines of ${out}:${NC}"
        head -n 8 "$out"
      fi
      break
    fi
    # rc==3 → offer setup + retry; anything else → driver hint and stop.
    handle_engine_failure "$rc" "${SE_ENGINE:-oracle}" || break
    echo -e "${DIM}\$ ${cmd[*]}${NC}"
    echo ""
  done
  rm -f "$raw_file" "$manifest"
}

action_setup_backend() {
  print_header "Connect Your Databases"
  load_status
  echo -e "  Enter Striim + database connection details, save them to a gitignored ${WHITE}.env${NC},"
  echo -e "  and test the ${MAGENTA}${ST_BACKEND}${NC} backend — or just validate what's already set."
  echo -e "  ${DIM}(Set STAGE_DB_LOCATION in config.py: TinyDB / PG / BQ / ORACLE.)${NC}"
  echo ""
  echo -e "    ${WHITE}1)${NC} Enter / update credentials interactively (then test)"
  echo -e "    ${WHITE}2)${NC} Just validate what's already set"
  echo -e "    ${WHITE}b)${NC} Back"
  echo ""
  local c rc
  while true; do
    read -rp "  Choose [1]: " c; c=${c:-1}
    case "$c" in
      1)
        python manage.py setup --interactive
        rc=$?
        echo ""
        report_rc "$rc" "Credentials saved and backend tested." "Setup did not complete — see the messages above."
        return 0
        ;;
      2)
        python manage.py setup
        rc=$?
        echo ""
        report_rc "$rc" "Backend ready (connectivity OK)." "Backend not ready — set the env vars shown above and re-run."
        return 0
        ;;
      b|B) return 0 ;;
      *) print_error "Not an option: $c — enter 1, 2, or b." ;;
    esac
  done
}

action_live_board() {
  # --- one-shot config fetch (before the loop) ---
  local cfg_raw backend logpath stage_dir interval_default interval_bq interval
  cfg_raw=$(python - <<'PY' 2>/dev/null || true
import config
print(getattr(config, "STAGE_DB_LOCATION", "TinyDB"))
print(getattr(config, "LOG_OUTPUT_PATH", "logging/run.log"))
print(getattr(config, "TARGET_TQL_PATH", "stage"))
print(int(getattr(config, "BOARD_REFRESH_SECONDS", 5)))
print(int(getattr(config, "BOARD_REFRESH_SECONDS_BQ", 30)))
PY
)
  {
    IFS= read -r backend
    IFS= read -r logpath
    IFS= read -r stage_dir
    IFS= read -r interval_default
    IFS= read -r interval_bq
  } <<< "$cfg_raw"
  : "${backend:=TinyDB}" "${logpath:=logging/run.log}" "${stage_dir:=stage}"
  : "${interval_default:=5}" "${interval_bq:=30}"

  # BQ backend → longer interval to avoid billable query spam
  case "$backend" in
    BQ) interval="$interval_bq" ;;
    *)  interval="$interval_default" ;;
  esac

  local prev_stage=""
  local board_json cur_stage item p appeared vanished found_it

  while true; do
    clear
    print_header "Live Status Board — refresh ${interval}s"

    board_json=$(python manage.py board --json 2>/dev/null || true)

    # Data is passed via env var; the heredoc delivers the Python *program* only.
    BOARD_JSON="$board_json" python - <<'PY'
import json, os
d = json.loads(os.environ.get("BOARD_JSON", "") or "{}")
if not d:
    print("  (no board data — run not started, or backend unreadable)")
else:
    counts = d.get("counts", {}) or {}
    total = d.get("total", 0); done = d.get("done", 0); pct = d.get("pct_complete", 0.0)
    barlen = 30; filled = int(pct * barlen)
    bar = "#" * filled + "-" * (barlen - filled)
    cs = " ".join(f"{k}:{v}" for k, v in sorted(counts.items()))
    print(f"  Run {d.get('run_id','?')}  [{d.get('backend','?')}/{d.get('source_engine','?')}]  {d.get('state','?')}")
    print(f"  Progress [{bar}] {pct*100:5.1f}%  ({done}/{total})   {cs}")
    inflight = d.get("inflight", []) or []
    print(f"  In-flight ({len(inflight)}):")
    for r in inflight[:20]:
        rate = r.get("rate")
        rate_s = (f"{rate/1000:.1f}k/s" if isinstance(rate, (int, float)) else "—")
        tgt = (str(r.get("targettbl", "")) or "")[:24]
        ns = (str(r.get("namespace", "")) or "")[:18]
        print(f"    #{str(r.get('roworder','')):<5} {tgt:<24} {ns:<18} started {r.get('started','-')}  {rate_s}")
    if not inflight:
        print("    (none running)")
    recent = d.get("recent", []) or []
    if recent:
        print("  Recent:")
        for r in recent[-6:]:
            print(f"    #{str(r.get('roworder','')):<5} {(str(r.get('targettbl',''))or'')[:24]:<24} {r.get('status','')}")
    if not d.get("striim"):
        print("  (Striim metrics unavailable — rates show ‘—’; state still live)")
PY

    echo ""
    echo -e "${CYAN}Stage files (.tql being run):${NC}"
    cur_stage=$(ls "$stage_dir"/*.tql 2>/dev/null | xargs -n1 basename 2>/dev/null | sort | tr '\n' ' ')
    if [[ -z "$cur_stage" ]]; then
      echo -e "  ${DIM}(none)${NC}"
    else
      for item in $cur_stage; do
        echo -e "  ${BLUE}${item}${NC}"
      done
    fi

    # appeared: items in cur but not in prev (skip on first iteration when prev is empty)
    appeared=""
    if [[ -n "$prev_stage" ]]; then
      for item in $cur_stage; do
        found_it=0
        for p in $prev_stage; do
          case "$p" in
            "$item") found_it=1; break ;;
          esac
        done
        if [[ "$found_it" -eq 0 ]]; then
          appeared="${appeared}${item} "
        fi
      done
    fi

    # vanished: items in prev but not in cur
    vanished=""
    for item in $prev_stage; do
      found_it=0
      for p in $cur_stage; do
        case "$p" in
          "$item") found_it=1; break ;;
        esac
      done
      if [[ "$found_it" -eq 0 ]]; then
        vanished="${vanished}${item} "
      fi
    done

    [[ -n "$appeared" ]] && echo -e "  ${GREEN}+ appeared:${NC} ${appeared}"
    [[ -n "$vanished" ]] && echo -e "  ${RED}- finished:${NC} ${vanished}"
    prev_stage="$cur_stage"

    echo ""
    echo -e "${CYAN}Recent log:${NC}"
    if [[ -f "$logpath" ]]; then
      tail -n 6 "$logpath" 2>/dev/null
    else
      echo -e "  ${DIM}(log not found: ${logpath})${NC}"
    fi

    echo ""
    echo -e "${DIM}Auto-refresh in ${interval}s — press any key to return.${NC}"
    # read returns non-zero on timeout (keep looping); a keypress breaks out.
    if read -r -t "$interval" -n 1 _; then
      break
    fi
  done
}

# ---- Run the load (python main.py) ----
action_run_load() {
  print_header "Run the Load (python main.py)"
  load_status
  local qf="${ST_QUERYFILE:-queryfile.txt}"
  if [[ ! -s "$qf" ]]; then
    print_error "No queryfile at '${qf}' (or it is empty)."
    print_info "Generate one first: option 9 (split wizard) or option 10 (probe & optimize)."
    return
  fi
  local nslices
  nslices=$(grep -cve '^[[:space:]]*$' "$qf" 2>/dev/null || echo "?")
  echo -e "  Launches ${DIM}python main.py${NC} for run ${WHITE}${ST_RUN_ID}${NC} on the ${MAGENTA}${ST_BACKEND}${NC} backend."
  echo -e "  Queryfile ${BLUE}${qf}${NC} defines ${WHITE}${nslices}${NC} slice(s)."
  echo ""
  echo -e "  ${DIM}• Long-running and stays in the foreground — press Ctrl-C to stop it.${NC}"
  echo -e "  ${DIM}• A run-lock prevents a second loader on this run id from starting.${NC}"
  echo -e "  ${DIM}• It auto-decides RESUME vs FRESH from existing state + the queryfile.${NC}"
  echo ""
  local fresh_flag=""
  if [[ "${ST_TOTAL:-0}" -gt 0 ]]; then
    print_warning "Run ${ST_RUN_ID} already has ${ST_TOTAL} slice(s) in state — it RESUMEs by default."
    if confirm "Force a FRESH restart instead (re-read queryfile, re-queue every slice)?"; then
      fresh_flag="--force-fresh"
      print_warning "FRESH selected: this run's slices will be re-created from the queryfile."
    fi
  fi
  if ! confirm_yes "Start the load now?"; then
    print_warning "Load not started."
    return
  fi
  local cmd=(python main.py)
  [[ -n "$fresh_flag" ]] && cmd+=("$fresh_flag")
  echo ""
  echo -e "${DIM}\$ ${cmd[*]}${NC}"
  echo ""
  "${cmd[@]}"
  local rc=$?
  echo ""
  report_rc "$rc" "Loader exited cleanly." "Loader exited with an error."
  print_info "Watch progress: option 11 (live status board) or option 2 (watch live)."
}

# ---- Verify completeness (reconcile) ----
action_reconcile() {
  print_header "Verify Completeness (reconcile)"
  load_status
  echo -e "  Checks every slice reached ${GREEN}COMPLETED${NC} and — when a watermark SCN was"
  echo -e "  captured at run start — re-counts the ${WHITE}source${NC} rows at that frozen point and"
  echo -e "  compares them to the loaded targets, proving no rows were missed."
  echo -e "  ${DIM}Degrades to a state-only gate if there is no SCN sidecar. Never modifies data.${NC}"
  echo ""
  python manage.py reconcile
  report_rc "$?" "Reconcile complete — verdict shown above." "Reconcile reported incomplete (see verdict above)."
}

# ======================== GUIDED WALKTHROUGH ========================
# Plain-language explainer of the split strategies (the "3) explain" branch).
_wt_explain_split() {
  print_header "How splitting works"
  echo -e "  The loader runs your query as ${WHITE}N parallel slices${NC}. Each slice loads a"
  echo -e "  disjoint chunk of the driving table. How you carve those chunks matters:"
  echo ""
  echo -e "  ${CYAN}rowid${NC}     Split by physical ROWID ranges. Fast, needs no special column."
  echo -e "            ${DIM}Boundaries freeze at split time — rows inserted afterward can be${NC}"
  echo -e "            ${DIM}missed on an append-heavy source.${NC}"
  echo -e "  ${CYAN}partition${NC} One slice per table partition. Ideal when the table is partitioned."
  echo -e "  ${CYAN}column${NC}    Range-split on a numeric/date column. Insert-safe; needs a column."
  echo -e "  ${CYAN}auto${NC}      Partition if partitioned, else ROWID (the split wizard's default)."
  echo ""
  echo -e "  ${BOLD}Not sure which?${NC} Use ${WHITE}Probe & optimize${NC} — it races ROWID vs column-range on"
  echo -e "  a bounded sample and recommends the strategy, chunk count, and concurrency."
}

# Queryfile stage — the core "know how to split vs test a sample" intent fork.
_wt_step_queryfile() {
  echo -e "  ${BOLD}Step: create your queryfile${NC}"
  echo -e "  A queryfile lists the parallel slices the loader runs. You don't have one yet."
  echo -e "  ${DIM}First time? You can set up / verify the state backend before anything else.${NC}"
  echo ""
  echo -e "  ${CYAN}How do you want to split your data into parallel slices?${NC}"
  echo -e "    ${WHITE}1)${NC} I know how I want to split it"
  echo -e "       ${DIM}(a numeric/date column, or the table is partitioned) → split wizard${NC}"
  echo -e "    ${WHITE}2)${NC} I'm not sure — test a sample and recommend the best split"
  echo -e "       ${DIM}(races ROWID vs column-range on a bounded sample) → probe & optimize${NC}"
  echo -e "    ${WHITE}3)${NC} Explain what these mean first"
  echo -e "    ${WHITE}4)${NC} I have several queries — paste them and build in one pass"
  echo -e "       ${DIM}(splittable ones probed/split, plain ones appended) → build wizard${NC}"
  echo -e "    ${WHITE}s)${NC} Connect your databases (enter credentials + test)"
  echo -e "    ${WHITE}b)${NC} Back to the main menu"
  echo ""
  local c
  read -rp "  Choose [2]: " c; c=${c:-2}
  case "$c" in
    1) action_split_wizard; pause ;;
    2) action_probe_wizard; pause ;;
    4) action_build_wizard; pause ;;
    3) _wt_explain_split; pause ;;
    s|S) action_setup_backend; pause ;;
    b|B) WT_EXIT=1 ;;
    *) print_error "Not an option: $c"; pause ;;
  esac
}

# Run stage — queryfile exists, offer to launch the load.
_wt_step_run() {
  echo -e "  ${BOLD}Step: run the load${NC}"
  print_success "Queryfile ${ST_QUERYFILE} is ready."
  echo -e "  Launch the loader to start pumping slices into your targets."
  echo ""
  echo -e "    ${WHITE}1)${NC} Run the load now (python main.py)"
  echo -e "    ${WHITE}2)${NC} Regenerate the queryfile (split wizard)"
  echo -e "    ${WHITE}3)${NC} Re-test with probe & optimize"
  echo -e "    ${WHITE}b)${NC} Back to the main menu"
  echo ""
  local c
  read -rp "  Choose [1]: " c; c=${c:-1}
  case "$c" in
    1) action_run_load; pause ;;
    2) action_split_wizard; pause ;;
    3) action_probe_wizard; pause ;;
    b|B) WT_EXIT=1 ;;
    *) print_error "Not an option: $c"; pause ;;
  esac
}

# Monitor stage — a run is live.
_wt_step_monitor() {
  echo -e "  ${BOLD}Step: monitor the run${NC}"
  echo -e "  Your load is in progress. Watch it complete, or drill into failures."
  echo ""
  [[ "${ST_FAILED:-0}" -gt 0 ]] && \
    echo -e "    ${WHITE}r)${NC} Reset ${RED}${ST_FAILED}${NC} FAILED slice(s) → NEW, then re-run"
  echo -e "    ${WHITE}1)${NC} Live status board (rates, files, log)"
  echo -e "    ${WHITE}2)${NC} Watch live (auto-refresh counts)"
  echo -e "    ${WHITE}3)${NC} Run the load again (resume remaining slices)"
  echo -e "    ${WHITE}b)${NC} Back to the main menu"
  echo ""
  local c
  read -rp "  Choose [1]: " c; c=${c:-1}
  case "$c" in
    1) action_live_board ;;
    2) action_watch ;;
    3) action_run_load; pause ;;
    r|R) action_reset; pause ;;
    b|B) WT_EXIT=1 ;;
    *) print_error "Not an option: $c"; pause ;;
  esac
}

# Verify stage — run finished, confirm completeness.
_wt_step_verify() {
  echo -e "  ${BOLD}Step: verify completeness${NC}"
  print_success "Run ${ST_RUN_ID} is FINISHED — every slice reached COMPLETED."
  echo -e "  Confirm nothing was missed, then retire the run when you're satisfied."
  echo ""
  echo -e "    ${WHITE}1)${NC} Verify completeness (reconcile — source vs target counts)"
  echo -e "    ${WHITE}2)${NC} Clear the run (retire — keeps history)"
  echo -e "    ${WHITE}3)${NC} Status dashboard"
  echo -e "    ${WHITE}b)${NC} Back to the main menu"
  echo ""
  local c
  read -rp "  Choose [1]: " c; c=${c:-1}
  case "$c" in
    1) action_reconcile; pause ;;
    2) action_clear_retire; pause ;;
    3) action_dashboard; pause ;;
    b|B) WT_EXIT=1 ;;
    *) print_error "Not an option: $c"; pause ;;
  esac
}

# The walkthrough itself: re-detect the stage each pass, draw the lifecycle map,
# and present the step screen for wherever the run currently is.
action_walkthrough() {
  WT_EXIT=0
  while [[ "$WT_EXIT" -eq 0 ]]; do
    clear
    print_header "Guided Walkthrough — first time here?"
    load_status
    detect_stage
    render_lifecycle_map "$WT_STAGE"
    echo ""
    echo -e "  ${DIM}Run ${ST_RUN_ID} · backend ${ST_BACKEND} · state ${ST_STATE}${NC}"
    [[ "${ST_FAILED:-0}" -gt 0 ]] && print_warning "${ST_FAILED} slice(s) FAILED on this run."
    echo ""
    case "$WT_STAGE" in
      QUERYFILE) _wt_step_queryfile ;;
      RUN)       _wt_step_run ;;
      MONITOR)   _wt_step_monitor ;;
      VERIFY)    _wt_step_verify ;;
      *)         _wt_step_queryfile ;;
    esac
  done
}

# ======================== HOME SCREEN ========================
show_home() {
  clear
  print_header "Striim InitialLoad ParallelLoader"
  load_status
  detect_stage
  render_status_line
  echo ""
  echo -e "  ${DIM}Next step:${NC} ${WHITE}▶ ${WT_HINT}${NC}"
  if [[ "${WT_FIRST_RUN:-0}" -eq 1 ]]; then
    echo -e "  ${YELLOW}New here?${NC} Start with the guided walkthrough (press Enter)."
  fi
  echo ""

  echo -e "${BOLD}${WHITE}Getting Started:${NC}"
  echo -e "${DIM}    G)  Guided walkthrough — first time here?${NC}"
  echo ""

  echo -e "${BOLD}${CYAN}Run Status:${NC}"
  echo -e "${DIM}    1)  Status dashboard (counts + in-flight + failed)${NC}"
  echo -e "${DIM}    2)  Watch live (auto-refresh)${NC}"
  echo -e "${DIM}    3)  Show failed slice details${NC}"
  echo -e "${DIM}    11) Live status board (rich — rates, files, log)${NC}"
  echo ""
  echo -e "${BOLD}${GREEN}Run Control:${NC}"
  echo -e "${DIM}    4)  Reset failed slices → NEW (redo them)${NC}"
  echo -e "${DIM}    5)  Clear run (retire — keeps history)${NC}"
  echo -e "${DIM}    6)  Clear run (hard delete)${NC}"
  echo ""
  echo -e "${BOLD}${BLUE}Logs:${NC}"
  echo -e "${DIM}    7)  View recent log${NC}"
  echo -e "${DIM}    8)  Tail errors (live)${NC}"
  echo ""
  echo -e "${BOLD}${MAGENTA}Data Splitting:${NC}"
  echo -e "${DIM}    9)  Generate queryfile (guided split wizard)${NC}"
  echo -e "${DIM}    10) Probe & optimize (recommend best split)${NC}"
  echo -e "${DIM}    15) Build queryfile from pasted queries (paste → probe/split/append)${NC}"
  echo ""
  echo -e "${BOLD}${GREEN}Load & Verify:${NC}"
  echo -e "${DIM}    13) Run the load (python main.py)${NC}"
  echo -e "${DIM}    14) Verify completeness (reconcile)${NC}"
  echo ""
  echo -e "${BOLD}${WHITE}Backend Setup:${NC}"
  echo -e "${DIM}    12) Set up / verify state backend (connectivity + create table)${NC}"
  echo ""
  echo -e "${DIM}    0)  Exit${NC}"
  echo ""
  echo -e "${DIM}    Windows: run this TUI under WSL or Git Bash. The 'python manage.py …' subcommands work natively in cmd/PowerShell.${NC}"
  echo ""
  local choice default_choice=""
  [[ "${WT_FIRST_RUN:-0}" -eq 1 ]] && default_choice="G"
  read -rp "  Enter choice${default_choice:+ [$default_choice]}: " choice
  choice=${choice:-$default_choice}

  case "$choice" in
    G|g) action_walkthrough ;;
    1) action_dashboard; pause ;;
    2) action_watch ;;
    3) action_failed; pause ;;
    4) action_reset; pause ;;
    5) action_clear_retire; pause ;;
    6) action_clear_hard; pause ;;
    7) action_logs; pause ;;
    8) action_tail_errors; pause ;;
    9) action_split_wizard; pause ;;
    10) action_probe_wizard; pause ;;
    11) action_live_board ;;
    12) action_setup_backend; pause ;;
    13) action_run_load; pause ;;
    14) action_reconcile; pause ;;
    15) action_build_wizard; pause ;;
    0) echo ""; print_info "Goodbye."; exit 0 ;;
    "") ;;
    *) print_error "Invalid choice: $choice"; pause ;;
  esac
}

main() {
  while true; do
    show_home
  done
}

main
