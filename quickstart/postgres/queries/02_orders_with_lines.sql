SELECT o.order_id, o.order_ts, l.sku, l.qty FROM demo.orders o JOIN demo.order_lines l ON l.order_id = o.order_id WHERE ~SPLIT~
