import os

# Base Paths and File Names
BASE_PATH = os.getcwd()                                                 # This should be the base path of this project.
QUERY_FILE = "queryfile.txt"                                            # This should be the list of queries to run with the target table listed (source|target)
QUERY_FILE_DELIMITER = "|"                                              # In your source QUERY_FILE, this is what is used to delimit the data. PIPE by default.

# You must create your own SOURCE_TQL_FILE contents. The name is unimportant (it can stay) but the contents must be a created app.
# See read.me for details on creating this file.
SOURCE_TQL_FILE = "admin.SW.tql"
# The name of your Striim application (do not include the namespace)
ILA_APP_NAME_BASE = "OracleInitialLoadApp"

# Session details
UNIQUE_RUN_ID = 100                 # Unique Run ID (per user/session. Keep static to use existing session. Creating a new one will NOT erase old session.)
CONCURRENT_APPS_MAX = 5             # This controls the maximum number of running, quiescing, or completed apps that can run at the same time (in parallel)
APP_MONITOR_INTERVAL_SECONDS = 15   # Controls how often we monitor app status. Should not be less than 15 seconds, and usually much greater (at least 60 seconds).
DEPLOY_WAIT_TIME_SECONDS = 20       # Controls minimum time on how long to wait between deploying new apps, so we do not overload Striim

# # Not yet implemented ******************: Uncomment if you want this to automatically stop, undeploy, and remove all existing apps in this run
# CLEANUP_RUN_ID = 100      # Not yet implemented ******************

# Logging
LOG_OUTPUT_NAME = os.path.join('logging','striimautoloader.log')
LOG_OUTPUT_PATH = os.path.join(BASE_PATH, LOG_OUTPUT_NAME)  # By default, create a lot in the same directory the app runs in

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
DONE_STATUSES = frozenset(['COMPLETED', 'FAILED'])
RUNNING_STATUSES = frozenset(['RUNNING'])
NEW_EXCLUDES_STATUSES = frozenset(['RUNNING', 'COMPLETED', 'FAILED'])
APP_RUNNING_STATUSES = frozenset(['RUNNING', 'QUIESCING', 'COMPLETED'])

# Defines where to orchestrate. Supports BigQuery (BQ), PostgreSQL (PG), or TinyDB (default: stores locally as a file):
STAGE_DB_LOCATION = 'TinyDB' #Options: BQ, PG, or TinyDB
TINYDB_PATH = os.path.join(BASE_PATH,'logging','current_position.json')

DEPLOYMENT_GROUP_TARGET = 'default'

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
    PG_PORT = int(os.environ.get("PG_PORT", "5432"))
    PG_DATABASE = os.environ.get("PG_DATABASE", "")
    PG_USER = os.environ.get("PG_USER", "")
    PG_PASSWORD = os.environ.get("PG_PASSWORD", "")
    PG_TABLE_ID = os.environ.get("PG_TABLE_ID", "striim_orchestration")
    PG_SSLMODE = os.environ.get("PG_SSLMODE", "prefer")


elif ENV == "PROD":
    # PROD-specific settings
    STRIIM_URL_PREFIX = os.environ.get("STRIIM_URL_PREFIX", "https://")  # Always use https:// in production
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
    PG_PORT = int(os.environ.get("PG_PORT", "5432"))
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
