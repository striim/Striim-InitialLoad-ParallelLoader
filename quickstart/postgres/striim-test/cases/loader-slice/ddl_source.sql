CREATE TABLE ${TID}src (
  id INTEGER PRIMARY KEY,
  amount NUMERIC(12, 2) NOT NULL,
  created_at TIMESTAMP NOT NULL
);
-- A copy of exactly the rows the slice app reads; lifecycle readiness compares the
-- target with it. (A view is not accepted: witnesses must be tables the test owns.)
CREATE TABLE ${TID}slice (
  id INTEGER PRIMARY KEY,
  amount NUMERIC(12, 2) NOT NULL,
  created_at TIMESTAMP NOT NULL
);
