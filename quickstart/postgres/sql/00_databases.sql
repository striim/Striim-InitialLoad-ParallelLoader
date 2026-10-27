-- Drop and recreate the quick-start databases. Run against the "postgres" database.
-- WITH (FORCE) disconnects any session still attached (Striim, psql, the loader).
DROP DATABASE IF EXISTS split_demo WITH (FORCE);
DROP DATABASE IF EXISTS split_demo_tgt WITH (FORCE);
CREATE DATABASE split_demo;
CREATE DATABASE split_demo_tgt;
