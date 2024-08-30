import os

# Base Paths and File Names
BASE_PATH = "/Users/danielferrara/PycharmProjects/StriimQueryAutoLoad/" # This should be the base path of this project.
QUERY_FILE = "queryfile.txt"                                            # This should be the list of queries to run with the target table listed (source|target)
QUERY_FILE_DELIMITER = "|"                                              # In your source QUERY_FILE, this is what is used to delimit the data. PIPE by default.

# You must create your own SOURCE_TQL_FILE contents. The name is unimportant (it can stay) but the contents must be a created app.
# See read.me for details on creating this file.
SOURCE_TQL_FILE = "admin.SW.tql"
# The name of your Striim application (do not include the namespace)
ILA_APP_NAME_BASE = "OracleInitialLoadApp"

# Not yet implemented ****************** To-do:
TABLE_LIST = ["source.tbl", "source.tbl2", "source.tbl3"]

# Session details
UNIQUE_RUN_ID = 100                 # Unique Run ID (per user/session. Keep static to use existing session. Creating a new one will NOT erase old session.)
CONCURRENT_APPS_MAX = 5             # This controls the maximum number of running, quiescing, or completed apps that can run at the same time (in parallel)
MAX_MEMORY_USAGE = 80               # Not yet implemented ******************
APP_MONITOR_INTERVAL_SECONDS = 15   # Controls how often we monitor app status. Should not be less than 15 seconds, and usually much greater (at least 60 seconds).
DEPLOY_WAIT_TIME_SECONDS = 20       # Controls minimum time on how long to wait between deploying new apps, so we do not overload Striim

# # Not yet implemented ******************: Uncomment if you want this to automatically stop, undeploy, and remove all existing apps in this run
# CLEANUP_RUN_ID = 100      # Not yet implemented ******************

# Logging
LOG_OUTPUT_NAME = "striimautoloader.log"
LOG_OUTPUT_PATH = os.path.join(BASE_PATH, LOG_OUTPUT_NAME)  # By default, create a lot in the same directory the app runs in

# Initial Load Automater (ILA) Settings - This generates a unique namespace per app, so that cleanup and app running concurrency is easy. Do not change this.
ILA_NS_BASE = "ILA" + "_" + str(UNIQUE_RUN_ID) + "_"

# Do not change these
# Derived Paths (constructed using base paths)
QUERY_FILE_PATH = os.path.join(BASE_PATH, QUERY_FILE)
SOURCE_TQL_PATH = BASE_PATH
TARGET_TQL_PATH = os.path.join(BASE_PATH, "stage/")

# Do not change these
DONE_STATUSES = ['COMPLETED', 'FAILED']
RUNNING_STATUSES = ['RUNNING']
NEW_EXCLUDES_STATUSES = ['RUNNING', 'COMPLETED', 'FAILED']
APP_RUNNING_STATUSES = ['RUNNING', 'QUIESCING', 'COMPLETED']

# DEV and PROD Environments
ENV = "DEV"  # Set to "PROD" for production environment, provide PROD details below

if ENV == "DEV":

    # Striim Configuration
    STRIIM_URL_PREFIX = "http://"
    STRIIM_NODE = "localhost:9080"

    # Striim Authentication - Provide one OR the other (not both).
    # Either USER / PWD - Any user that has admin level privilidges
    STRIIM_ADMIN_USER = "admin"
    STRIIM_ADMIN_PWD = "admin"
    # Or Valid token from any user that has admin level privilidges
    STRIIM_API_TOKEN = ""

    # Not yet implemented ******************
    # CREATE_BQ_TABLE_IF_NOT_EXISTS = False

    # BigQuery Configuration
    BQ_KEYFILE_LOCATION = "/Users/danielferrara/Documents/Striim420/UploadedFiles/daniel-sa-striimfieldproject-a326623e58fe.json" # Update with your actual keyfile path
    PROJECT_ID = "striimfieldproject"
    DATASET_ID = "Daniel"
    TABLE_ID = "striim_orchestration"


elif ENV == "PROD":
    # PROD-specific settings
    # For example:
    # STRIIM_NODE = "prod-striim-server:9080"
    # BQ_DATASET_ID = "prod_dataset"

    # For PROD, we should allow more time between deployment, since the workload may be greater.
    APP_MONITOR_INTERVAL_SECONDS = 60
    DEPLOY_WAIT_TIME_SECONDS = 120
