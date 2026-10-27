-- Target table for the parallel load. Run against split_demo_tgt.
-- The primary key is required: on a re-run the loader relies on DUPLICATE_ROW_EXISTS
-- being ignored, which only happens when the target has a key.
CREATE SCHEMA demo;

CREATE TABLE demo.orders (
    order_id    bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    region_id   int    NOT NULL,
    status      text   NOT NULL,
    order_ts    timestamp NOT NULL,
    amount      numeric(12,2) NOT NULL,
    notes       text
);
