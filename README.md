# Striim API Orchestration Python Program

This Python program utilizes the Striim API to orchestrate the creation, deployment, starting, reviewing status, undeploying, and dropping of Striim Applications. It helps parallelize and automate the load by splitting data into pieces, such as utilizing reading huge Oracle tables, computing read ranges for parallel reading, or splitting based on primary key values or date ranges. The resulting output is a set of queries.

This app currently utilizes a BQ table as both a historical record and a place to orchestrate progress.

## Required Files

- `<template>.tql`
- `config.py`
- `main.py`
- `queryfile.txt`

## queryfile.txt

This file is simply a two-column file, containing the query to run and the target table name. The delimeter is definied in config.py and is pipe (|) by default. Here is an example:
```sql
SELECT * FROM QATEST.WF_PENDING_ACTIVITY WHERE ID < 1000               |QATEST2.WF_PENDING_ACTIVITY
SELECT * FROM QATEST.WF_PENDING_ACTIVITY WHERE ID BETWEEN 1000 AND 2000|QATEST2.WF_PENDING_ACTIVITY
SELECT * FROM QATEST.WF_PENDING_ACTIVITY WHERE ID BETWEEN 2001 AND 3000|QATEST2.WF_PENDING_ACTIVITY
SELECT * FROM QATEST.WF_PENDING_ACTIVITY WHERE ID > 3000 AND 2000      |QATEST2.WF_PENDING_ACTIVITY
```

## TQL Template File

The TQL template file should utilize Property Variables (for connection string, username, and password), and the following placeholder variables:
- **```Query: "~QUERYTEXT~"```**: This portion must exist in your TQL Sample App, on your **Source Reader**.
- **```Query: "Tables: 'QUERY,~TARGETTABLE~'"```**: This portion must exist in your TQL Sample App, on your **Target Writer**.

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
