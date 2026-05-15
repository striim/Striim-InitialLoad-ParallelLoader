CREATE TABLE IF NOT EXISTS striim_orchestration (
    id                INTEGER PRIMARY KEY,
    roworder          INTEGER,
    uniquerunid       INTEGER,
    query             TEXT,
    appname           TEXT,
    targettbl         TEXT,
    status            TEXT,
    namespace         TEXT,
    started_datetime  TIMESTAMP,
    finished_datetime TIMESTAMP,
    notes             TEXT,
    iscurrentrow      BOOLEAN
);
