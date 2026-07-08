import os


def _load_dotenv(path):
    """Minimal, no-dependency ``.env`` loader.

    Reads ``KEY=VALUE`` lines from ``path`` and sets ``os.environ[KEY]`` ONLY
    when the key is not already present, so precedence is:
    real env var > ``.env`` > hardcoded default. Strips surrounding single or
    double quotes from the value, ignores blank lines, ``#`` comment lines and
    lines without ``=``, and swallows a missing file (``OSError``). No external
    package (``python-dotenv``) is required.
    """
    try:
        with open(path) as f:
            lines = f.readlines()
    except OSError:
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(os.path.join(os.getcwd(), ".env"))


def _env_int(name, default, minimum=None):
    """Parse an integer env var with a clear message instead of a cryptic
    ``invalid literal for int()`` traceback at import.

    ``default`` is used when the var is unset/blank. When ``minimum`` is given, a
    value below it is rejected (e.g. a port or tunable must be >= 1)."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        raw = str(default)
    try:
        val = int(raw)
    except (TypeError, ValueError):
        raise ValueError(
            f"{name} must be an integer, got {raw!r}. "
            f"Fix it in your environment or .env file."
        )
    if minimum is not None and val < minimum:
        raise ValueError(
            f"{name} must be >= {minimum}, got {val}. "
            f"Fix it in your environment or .env file."
        )
    return val


# Base Paths and File Names
BASE_PATH = os.getcwd()  # This should be the base path of this project.
QUERY_FILE = "queryfile.txt"  # This should be the list of queries to run with the target table listed (source|target)
QUERY_FILE_DELIMITER = "|"  # In your source QUERY_FILE, this is what is used to delimit the data. PIPE by default.

# You must create your own SOURCE_TQL_FILE contents. The name is unimportant (it can stay) but the contents must be a created app.
# See read.me for details on creating this file.
SOURCE_TQL_FILE = "admin.SW.tql"
# The name of your Striim application (do not include the namespace)
ILA_APP_NAME_BASE = "OracleInitialLoadApp"

# Session details
UNIQUE_RUN_ID = 100  # Unique Run ID (per user/session. Keep static to use existing session. Creating a new one will NOT erase old session.)
CONCURRENT_APPS_MAX = 5  # This controls the maximum number of running, quiescing, or completed apps that can run at the same time (in parallel)
APP_MONITOR_INTERVAL_SECONDS = 15  # Controls how often we monitor app status. Should not be less than 15 seconds, and usually much greater (at least 60 seconds).
DEPLOY_WAIT_TIME_SECONDS = 20  # Controls minimum time on how long to wait between deploying new apps, so we do not overload Striim

# # Not yet implemented ******************: Uncomment if you want this to automatically stop, undeploy, and remove all existing apps in this run
# CLEANUP_RUN_ID = 100      # Not yet implemented ******************

# Logging
LOG_OUTPUT_NAME = os.path.join("logging", "striimautoloader.log")
LOG_OUTPUT_PATH = os.path.join(
    BASE_PATH, LOG_OUTPUT_NAME
)  # By default, create a lot in the same directory the app runs in

# Initial Load Automater (ILA) Settings - This generates a unique namespace per app, so that cleanup and app running concurrency is easy. Do not change this.
# NOTE: ILA_NS_BASE is computed once at import time from UNIQUE_RUN_ID.
# Changing UNIQUE_RUN_ID at runtime will not update this value.
ILA_NS_BASE = "ILA" + "_" + str(UNIQUE_RUN_ID) + "_"

# Do not change these
# Derived Paths (constructed using base paths)
QUERY_FILE_PATH = os.path.join(BASE_PATH, QUERY_FILE)
SOURCE_TQL_PATH = BASE_PATH
TARGET_TQL_PATH = os.path.join(BASE_PATH, "stage")

# Do not change these
DONE_STATUSES = frozenset(["COMPLETED", "FAILED", "COMPLETED-FAILEDDROP"])
RUNNING_STATUSES = frozenset(["RUNNING"])
NEW_EXCLUDES_STATUSES = frozenset(
    ["RUNNING", "COMPLETED", "FAILED", "COMPLETED-FAILEDDROP"]
)
APP_RUNNING_STATUSES = frozenset(["RUNNING", "QUIESCING", "COMPLETED"])

# Defines where to orchestrate. Supports BigQuery (BQ), PostgreSQL (PG), Oracle (ORACLE), or TinyDB (default: stores locally as a file):
STAGE_DB_LOCATION = "TinyDB"  # Options: BQ, PG, ORACLE, or TinyDB
TINYDB_PATH = os.path.join(BASE_PATH, "logging", "current_position.json")

DEPLOYMENT_GROUP_TARGET = "default"

# DEV and PROD Environments
ENV = os.environ.get("ENV", "DEV")  # Read ENV from environment

# Credentials and connection details are read from environment variables.
# Set them in your shell or in a .env file (loaded separately, e.g. with python-dotenv).
# Required vars: STRIIM_NODE, STRIIM_ADMIN_USER, STRIIM_ADMIN_PWD (or STRIIM_API_TOKEN).
# For BigQuery mode: BQ_KEYFILE_LOCATION, BQ_PROJECT_ID, BQ_DATASET_ID.
# For PostgreSQL mode: PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD.

if ENV == "DEV":

    # Striim Configuration
    STRIIM_URL_PREFIX = os.environ.get("STRIIM_URL_PREFIX", "http://")
    STRIIM_NODE = os.environ.get("STRIIM_NODE", "localhost:9080")

    # Striim Authentication - Provide one OR the other (not both).
    # Either USER / PWD - Any user that has admin level privilidges
    STRIIM_ADMIN_USER = os.environ.get("STRIIM_ADMIN_USER", "admin")
    STRIIM_ADMIN_PWD = os.environ.get("STRIIM_ADMIN_PWD", "")
    # Or Valid token from any user that has admin level privilidges
    STRIIM_API_TOKEN = os.environ.get("STRIIM_API_TOKEN", "")

    # Not yet implemented ******************
    # CREATE_BQ_TABLE_IF_NOT_EXISTS = False

    # BigQuery Configuration
    # Only needed if value of
    #  STAGE_DB_LOCATION='BQ'
    BQ_KEYFILE_LOCATION = os.environ.get("BQ_KEYFILE_LOCATION", "")
    BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "")
    BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "")
    BQ_TABLE_ID = os.environ.get("BQ_TABLE_ID", "striim_orchestration")

    # PostgreSQL Configuration
    # Only needed if STAGE_DB_LOCATION='PG'
    PG_HOST = os.environ.get("PG_HOST", "localhost")
    PG_PORT = _env_int("PG_PORT", 5432, minimum=1)
    PG_DATABASE = os.environ.get("PG_DATABASE", "")
    PG_USER = os.environ.get("PG_USER", "")
    PG_PASSWORD = os.environ.get("PG_PASSWORD", "")
    PG_TABLE_ID = os.environ.get("PG_TABLE_ID", "striim_orchestration")
    PG_SSLMODE = os.environ.get("PG_SSLMODE", "prefer")


elif ENV == "PROD":
    # PROD-specific settings
    STRIIM_URL_PREFIX = os.environ.get(
        "STRIIM_URL_PREFIX", "https://"
    )  # Always use https:// in production
    STRIIM_NODE = os.environ.get("STRIIM_NODE", "")
    STRIIM_ADMIN_USER = os.environ.get("STRIIM_ADMIN_USER", "")
    STRIIM_ADMIN_PWD = os.environ.get("STRIIM_ADMIN_PWD", "")
    STRIIM_API_TOKEN = os.environ.get("STRIIM_API_TOKEN", "")
    BQ_KEYFILE_LOCATION = os.environ.get("BQ_KEYFILE_LOCATION", "")
    BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "")
    BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "")
    BQ_TABLE_ID = os.environ.get("BQ_TABLE_ID", "striim_orchestration")

    # PostgreSQL Configuration
    # Only needed if STAGE_DB_LOCATION='PG'
    PG_HOST = os.environ.get("PG_HOST", "localhost")
    PG_PORT = _env_int("PG_PORT", 5432, minimum=1)
    PG_DATABASE = os.environ.get("PG_DATABASE", "")
    PG_USER = os.environ.get("PG_USER", "")
    PG_PASSWORD = os.environ.get("PG_PASSWORD", "")
    PG_TABLE_ID = os.environ.get("PG_TABLE_ID", "striim_orchestration")
    PG_SSLMODE = os.environ.get("PG_SSLMODE", "prefer")

    # These vars are inherited from the top-level config unless overridden here.
    # Override CONCURRENT_APPS_MAX or UNIQUE_RUN_ID here if needed for production.

    # For PROD, we should allow more time between deployment, since the workload may be greater.
    APP_MONITOR_INTERVAL_SECONDS = 60
    DEPLOY_WAIT_TIME_SECONDS = 120

else:
    raise ValueError(f"Unknown ENV value: '{ENV}'. Expected 'DEV' or 'PROD'.")


# Oracle connection for the splitter (manage.py split). Read-only account suffices.
# Only required when running `split`. Provide ORACLE_DSN, or ORACLE_HOST + ORACLE_SERVICE.
ORACLE_DSN = os.environ.get("ORACLE_DSN", "")
ORACLE_HOST = os.environ.get("ORACLE_HOST", "")
ORACLE_PORT = _env_int("ORACLE_PORT", 1521, minimum=1)
ORACLE_SERVICE = os.environ.get("ORACLE_SERVICE", "")
ORACLE_USER = os.environ.get("ORACLE_USER", "")
ORACLE_PASSWORD = os.environ.get("ORACLE_PASSWORD", "")

# Oracle as an ORCHESTRATION/state backend (STAGE_DB_LOCATION = "ORACLE").
# Distinct from the source ORACLE_* above (used by the splitter/probe): it may
# point at the same Oracle but is configured independently. Only required when
# STAGE_DB_LOCATION = "ORACLE". Provide ORCH_ORACLE_DSN, or
# ORCH_ORACLE_HOST + ORCH_ORACLE_SERVICE.
ORCH_ORACLE_DSN = os.environ.get("ORCH_ORACLE_DSN", "")
ORCH_ORACLE_HOST = os.environ.get("ORCH_ORACLE_HOST", "")
ORCH_ORACLE_PORT = _env_int("ORCH_ORACLE_PORT", 1521, minimum=1)
ORCH_ORACLE_SERVICE = os.environ.get("ORCH_ORACLE_SERVICE", "")
ORCH_ORACLE_USER = os.environ.get("ORCH_ORACLE_USER", "")
ORCH_ORACLE_PASSWORD = os.environ.get("ORCH_ORACLE_PASSWORD", "")
ORCH_ORACLE_TABLE_ID = os.environ.get("ORCH_ORACLE_TABLE_ID", "striim_orchestration")

# ---- Source database engine for the splitter / probe ----
# "oracle" (default), "postgres", "sqlserver", and "jdbc" are all implemented;
# the get_dialect() factory routes each to its concrete SourceDialect. Override
# per-run with --source-engine on probe/split.
SOURCE_DB_TYPE = os.environ.get("SOURCE_DB_TYPE", "oracle")

# ---- Source PostgreSQL (SOURCE_DB_TYPE = "postgres") — distinct from the PG_* STATE backend ----
SOURCE_PG_HOST = os.environ.get("SOURCE_PG_HOST", "")
SOURCE_PG_PORT = _env_int("SOURCE_PG_PORT", 5432, minimum=1)
SOURCE_PG_DATABASE = os.environ.get("SOURCE_PG_DATABASE", "")
SOURCE_PG_USER = os.environ.get("SOURCE_PG_USER", "")
SOURCE_PG_PASSWORD = os.environ.get("SOURCE_PG_PASSWORD", "")
SOURCE_PG_SSLMODE = os.environ.get("SOURCE_PG_SSLMODE", "prefer")

# ---- Source SQL Server (SOURCE_DB_TYPE = "sqlserver") ----
SQLSERVER_HOST = os.environ.get("SQLSERVER_HOST", "")
SQLSERVER_PORT = _env_int("SQLSERVER_PORT", 1433, minimum=1)
SQLSERVER_DATABASE = os.environ.get("SQLSERVER_DATABASE", "")
SQLSERVER_USER = os.environ.get("SQLSERVER_USER", "")
SQLSERVER_PASSWORD = os.environ.get("SQLSERVER_PASSWORD", "")
SQLSERVER_DRIVER = os.environ.get("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")

# ---- Generic JDBC source (SOURCE_DB_TYPE = "jdbc") ----
JDBC_DRIVER_CLASS = os.environ.get("JDBC_DRIVER_CLASS", "")
JDBC_URL = os.environ.get("JDBC_URL", "")
JDBC_JAR_PATH = os.environ.get("JDBC_JAR_PATH", "")
JDBC_USER = os.environ.get("JDBC_USER", "")
JDBC_PASSWORD = os.environ.get("JDBC_PASSWORD", "")
JDBC_ROW_LIMIT_SYNTAX = os.environ.get(
    "JDBC_ROW_LIMIT_SYNTAX", "rownum"
)  # rownum|limit|top|fetch
JDBC_WATERMARK_SQL = os.environ.get("JDBC_WATERMARK_SQL", "")

# ---- Target DB provider for the generated TQL (defaults to the source engine) ----
TARGET_DB_TYPE = os.environ.get("TARGET_DB_TYPE", SOURCE_DB_TYPE)

# ---- Probe tunables (manage.py probe) ----
# Bounded-sampling caps keep the probe safe to run against trillion-row tables.
PROBE_DEPTH_DEFAULT = os.environ.get(
    "PROBE_DEPTH_DEFAULT", "bakeoff"
)  # lightweight|bakeoff|adaptive
PROBE_TARGET_SLICE_SECONDS = _env_int("PROBE_TARGET_SLICE_SECONDS", 600, minimum=1)
PROBE_SAMPLE_ROWS = _env_int("PROBE_SAMPLE_ROWS", 100000, minimum=1)
PROBE_TIME_BUDGET_SECONDS = _env_int("PROBE_TIME_BUDGET_SECONDS", 20, minimum=1)
PROBE_MAX_CONCURRENCY = _env_int("PROBE_MAX_CONCURRENCY", 32, minimum=1)


def _env_int_list(name, default_list, minimum=1):
    """Parse a comma-separated list of positive integers (e.g. ``1,2,4,8``).

    Blank/unset uses ``default_list``. Whitespace around items is tolerated; each
    item must parse as an int >= ``minimum`` or a clear error is raised at import."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return list(default_list)
    out = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            val = int(part)
        except (TypeError, ValueError):
            raise ValueError(
                f"{name} must be a comma-separated list of integers, got {raw!r}."
            )
        if val < minimum:
            raise ValueError(f"{name} values must be >= {minimum}, got {val}.")
        out.append(val)
    return out or list(default_list)


# Optional Oracle PARALLEL-degree sweep in the probe (opt-in via --parallel-sweep).
# DEGREES are the PARALLEL(n) settings to race; RUNS is how many timings to average
# per degree (set 3 for a thorough, noise-resistant run — the first run is warm-up).
PROBE_PARALLEL_DEGREES = _env_int_list("PROBE_PARALLEL_DEGREES", [1, 2, 4, 8], minimum=1)
PROBE_PARALLEL_RUNS = _env_int("PROBE_PARALLEL_RUNS", 1, minimum=1)

# ---- Live status board (manage.sh "Live status board" / manage.py board) ----
BOARD_REFRESH_SECONDS = _env_int("BOARD_REFRESH_SECONDS", 5, minimum=1)  # local TinyDB / PG
BOARD_REFRESH_SECONDS_BQ = _env_int("BOARD_REFRESH_SECONDS_BQ", 30, minimum=1)  # avoid spamming billable BQ queries

# ---- Stall detection (main.py loop) ----
# A slice RUNNING longer than this (seconds) is force-FAILED so the end-of-run gate
# surfaces it. 0 = disabled (default: behaviour unchanged unless the operator opts in).
SLICE_MAX_RUNTIME_SECONDS = _env_int("SLICE_MAX_RUNTIME_SECONDS", 0, minimum=0)
