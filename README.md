# Striim API Orchestration Python Program

This Python program utilizes the Striim API to orchestrate the creation, deployment, starting, reviewing status, undeploying, and dropping of Striim Applications. It helps parallelize and automate the load by splitting data into pieces, such as utilizing reading huge Oracle tables, computing read ranges for parallel reading, or splitting based on primary key values or date ranges. The resulting output is a set of queries.

This app currently utilizes either a BQ table or local TinyDB (current_position.json file) as both a historical record and a place to orchestrate progress.

## Key Features

*   **Automation:**  Handles the entire lifecycle of Striim applications, including creation, deployment, starting, monitoring, stopping, undeploying, and dropping.
*   **Parallelization:**  Divides the data load into smaller units to run multiple Striim applications concurrently, optimizing throughput.
*   **Query-Based Processing:**  Reads queries from a file (`queryfile.txt`) where each line contains a query and its target table.
*   **TQL-Driven Deployment:**  Uses a TQL template file (`admin.SW.tql`) to generate and deploy Striim applications for each query.
*   **State Management:**  Utilizes a database (BigQuery or TinyDB) to track the progress and status of each query execution.
*   **Monitoring and Logging:**  Continuously monitors the status of applications and logs events for debugging and analysis.

## Requirements

*   **Python 3.6 or higher:**  The program is written in Python and requires a compatible version.
*   **Striim Environment:**  Access to a Striim cluster with API connectivity.
*   **Required Python Libraries:**  `requests`, `google-cloud-bigquery` (if using BigQuery), `tinydb`.
*   **Configuration File:**  A `config.py` file to store Striim credentials, database settings, and other parameters.
*   **Input Files:**
    *   `queryfile.txt`: Contains the queries to be executed, one per line, with the target table separated by a delimiter (configurable in `config.py`).
    *   `admin.SW.tql`: A TQL file that serves as a template for creating Striim applications. This file should include placeholders for the query and the target table.

## Configuration

The program's behavior can be customized through the `config.py` file. Here are some of the key configuration options:

*   **Striim Connection Details:** `STRIIM_URL_PREFIX`, `STRIIM_NODE`, `STRIIM_ADMIN_USER`, `STRIIM_ADMIN_PWD`, `STRIIM_API_TOKEN`.
*   **Database Selection:** `STAGE_DB_LOCATION` (choose between `BQ` for BigQuery or `TinyDB` for a local file-based database).
*   **BigQuery Settings:** `BQ_KEYFILE_LOCATION`, `PROJECT_ID`, `DATASET_ID`, `TABLE_ID` (if using BigQuery).
*   **Concurrency Control:** `CONCURRENT_APPS_MAX` (maximum number of concurrent Striim applications).
*   **Monitoring Interval:** `APP_MONITOR_INTERVAL_SECONDS` (how often to check the status of applications).
*   **Deployment Delay:** `DEPLOY_WAIT_TIME_SECONDS` (minimum time to wait between deploying new applications).
*   **Logging:** `LOG_OUTPUT_NAME`, `LOG_OUTPUT_PATH`.

## queryfile.txt

This file is simply a two-column file, containing the query to run and the target table name. The delimeter is definied in config.py and is pipe (|) by default. Here is an example:
```sql
SELECT * FROM QATEST.WF_PENDING_ACTIVITY WHERE ID < 1000               |QATEST2.WF_PENDING_ACTIVITY
SELECT * FROM QATEST.WF_PENDING_ACTIVITY WHERE ID BETWEEN 1000 AND 2000|QATEST2.WF_PENDING_ACTIVITY
SELECT * FROM QATEST.WF_PENDING_ACTIVITY WHERE ID BETWEEN 2001 AND 3000|QATEST2.WF_PENDING_ACTIVITY
SELECT * FROM QATEST.WF_PENDING_ACTIVITY WHERE ID > 3000               |QATEST2.WF_PENDING_ACTIVITY
SELECT * FROM QATEST.BIGTABLE WHERE ID < 1000               |QATEST2.BIGTABLE
SELECT * FROM QATEST.BIGTABLE WHERE ID BETWEEN 1000 AND 2000|QATEST2.BIGTABLE
SELECT * FROM QATEST.BIGTABLE WHERE ID BETWEEN 2001 AND 3000|QATEST2.BIGTABLE
SELECT * FROM QATEST.BIGTABLE WHERE ID > 3000               |QATEST2.BIGTABLE
```

## TQL Template File

The TQL template file should utilize Property Variables (for connection string, username, and password), and the following placeholder variables:
- **```Query: "~QUERYTEXT~"```**: This portion must exist in your TQL Sample App, on your **Source Reader**.
- **```Query: "Tables: 'QUERY,~TARGETTABLE~'"```**: This portion must exist in your TQL Sample App, on your **Target Writer**.

## Running the Program

1.  **Install Dependencies:** Ensure that all required Python libraries are installed.
2.  **Configure:** Update the `config.py` file with your Striim credentials, database settings, and other desired parameters.
3.  **Prepare Input Files:** Create the `queryfile.txt` and `admin.SW.tql` files as described in the Requirements section.
4.  **Execute:** Run the `main.py` script to start the orchestration process.

The program will then read the queries from `queryfile.txt`, generate TQL files for each query using the `admin.SW.tql` template, create and deploy Striim applications, monitor their execution, and finally undeploy and drop them.

## Additional Notes

*   The program includes basic error handling and retries to ensure robust operation.
*   The database (BigQuery or TinyDB) is used to persist the state of the orchestration process, allowing for resuming from interruptions.
*   The program provides logging capabilities for monitoring and debugging purposes.

This README provides a comprehensive overview of the Striim API Orchestration Python program, its features, requirements, and usage instructions.

## BigQuery Table

The output is stored in the following BigQuery table:

```sql
CREATE TABLE `striimfieldproject.Daniel.striim_orchestration` (
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

### Table Fields

- **id**: INTEGER, NOT NULL. Unique identifier for each record.
- **roworder**: INTEGER. Order of the row in the sequence.
- **uniquerunid**: INTEGER. Unique identifier for each run.
- **query**: STRING. The actual query text.
- **appname**: STRING. Keeps track of the full app name created.
- **targettbl**: STRING. The target table (full schema.tablename) that the query results will write to. Include ColumnMap or KeyColumns if needed.
- **status**: STRING. Keeps track of the status.
- **namespace**: STRING. Keeps track of the Namespace used in deployment.
- **started_datetime**: TIMESTAMP. Tracks when the app was confirmed started (may not be exact).
- **finished_datetime**: TIMESTAMP. Tracks when the app was confirmed completed (may not be exact).
- **notes**: STRING. Any additional notes related to the output.
- **iscurrentrow**: BOOL. Indicates if this is the current row.

### Possible statuses from 'status' above:

- `<blank>` → Not yet started to process yet
- `RUNNING` → Has been created, deployed, and started
- `COMPLETED` → Has been detected as completed successfully.
- `FAILED` → Has been detected as failed, and added any failure messages to notes.
