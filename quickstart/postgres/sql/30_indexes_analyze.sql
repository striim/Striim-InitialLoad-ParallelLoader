-- Keys, indexes and fresh statistics. Run against split_demo after 20_generate.sql.
-- The probe ranks split columns from pg_stats and reads plan estimates, so ANALYZE
-- is required.
ALTER TABLE demo.orders      ADD PRIMARY KEY (order_id);
ALTER TABLE demo.order_lines ADD PRIMARY KEY (line_id);
ALTER TABLE demo.events      ADD PRIMARY KEY (event_id, event_ts);

CREATE INDEX orders_customer_idx     ON demo.orders (customer_id);
-- order_ts is random relative to disk order: a range on it reads scattered pages.
CREATE INDEX orders_ts_idx           ON demo.orders (order_ts);
-- Scenario 2 filters one region. This index reads only that region's rows in an
-- order_id range, while a ctid slice reads every page and discards 90% of the rows.
CREATE INDEX orders_region_order_idx ON demo.orders (region_id, order_id);

ANALYZE demo.orders;
ANALYZE demo.order_lines;
ANALYZE demo.events;
