-- Source tables. Run against split_demo. Keys and indexes are added after the load
-- (30_indexes_analyze.sql) so the bulk insert stays fast.
CREATE SCHEMA demo;

-- Wide table; rows are inserted in order_id order. order_ts is random, so it is not
-- correlated with the physical (ctid) order.
CREATE TABLE demo.orders (
    order_id    bigint NOT NULL,
    customer_id bigint NOT NULL,
    region_id   int    NOT NULL,
    status      text   NOT NULL,
    order_ts    timestamp NOT NULL,
    amount      numeric(12,2) NOT NULL,
    notes       text
);

-- Three lines per order. No index on order_id: scenario 3 adds one during the demo.
CREATE TABLE demo.order_lines (
    line_id  bigint NOT NULL,
    order_id bigint NOT NULL,
    sku      text   NOT NULL,
    qty      int    NOT NULL,
    price    numeric(10,2) NOT NULL
);

-- Range-partitioned by month over 2024-2025 (24 partitions).
CREATE TABLE demo.events (
    event_id bigint    NOT NULL,
    event_ts timestamp NOT NULL,
    kind     text      NOT NULL,
    payload  text
) PARTITION BY RANGE (event_ts);

DO $$
DECLARE
    m date := date '2024-01-01';
BEGIN
    WHILE m < date '2026-01-01' LOOP
        EXECUTE format(
            'CREATE TABLE demo.%I PARTITION OF demo.events FOR VALUES FROM (%L) TO (%L)',
            'events_' || to_char(m, 'YYYY_MM'), m, (m + interval '1 month')::date);
        m := (m + interval '1 month')::date;
    END LOOP;
END $$;
