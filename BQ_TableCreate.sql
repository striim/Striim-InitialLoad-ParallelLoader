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
