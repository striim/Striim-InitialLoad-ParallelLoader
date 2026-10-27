-- Generate the data server-side. Run against split_demo with -v scale=<N>.
-- scale=1 gives 5M orders, 15M order_lines, 10M events (about 5 GB).
-- setseed makes every build produce the same data.
SELECT setseed(0.42);

-- 40% of orders belong to customer 1 (the skew scenario).
-- Orders arrive in regional batches: 1000 consecutive orders per region, cycling
-- through 10 regions, so each region's rows sit together on disk (scenario 2).
INSERT INTO demo.orders
SELECT g,
       CASE WHEN random() < 0.40 THEN 1 ELSE 2 + floor(random() * 500000)::bigint END,
       1 + ((g - 1) / 1000) % 10,
       (ARRAY['NEW','PAID','SHIPPED','DELIVERED','RETURNED'])[1 + floor(random() * 5)::int],
       timestamp '2023-01-01' + random() * interval '1095 days',
       round((random() * 1000)::numeric, 2),
       md5(g::text) || repeat('x', 168)
FROM generate_series(1, (5000000 * :scale)::bigint) AS g;

INSERT INTO demo.order_lines
SELECT (o - 1) * 3 + k,
       o,
       'SKU-' || lpad((1 + floor(random() * 20000))::int::text, 5, '0'),
       1 + floor(random() * 5)::int,
       round((random() * 200)::numeric, 2)
FROM generate_series(1, (5000000 * :scale)::bigint) AS o,
     generate_series(1, 3) AS k;

INSERT INTO demo.events
SELECT g,
       timestamp '2024-01-01' + random() * interval '730 days',
       (ARRAY['click','view','cart','purchase','refund'])[1 + floor(random() * 5)::int],
       md5(g::text) || repeat('e', 68)
FROM generate_series(1, (10000000 * :scale)::bigint) AS g;
