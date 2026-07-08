-- Oracle orchestration state table.
-- The backend auto-creates this on first use (ORA-00955 tolerated), so this
-- script is provided for manual creation or documentation purposes only.
CREATE TABLE striim_orchestration (
    id                NUMBER PRIMARY KEY,
    roworder          NUMBER,
    uniquerunid       NUMBER,
    query             CLOB,
    appname           VARCHAR2(4000),
    targettbl         VARCHAR2(4000),
    status            VARCHAR2(64),
    namespace         VARCHAR2(256),
    started_datetime  TIMESTAMP,
    finished_datetime TIMESTAMP,
    notes             VARCHAR2(4000),
    iscurrentrow      NUMBER(1)
);
