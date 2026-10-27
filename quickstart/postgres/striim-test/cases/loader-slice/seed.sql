INSERT INTO ${TID}src (id, amount, created_at)
SELECT g, g * 1.25, TIMESTAMP '2026-01-01 00:00:00' + g * INTERVAL '1 hour'
FROM generate_series(1, 50) AS g;
INSERT INTO ${TID}slice SELECT * FROM ${TID}src WHERE id >= 1 AND id < 26;
