INSERT INTO record(group_id, body) VALUES (1, repeat('x', 512));
INSERT INTO plain_events(group_id, body) VALUES (1, repeat('x', 512));
INSERT INTO record_items(record_id, body)
SELECT id, repeat('i', 128) FROM record ORDER BY id DESC LIMIT 1;
INSERT INTO record_meta(record_id, body)
SELECT id, repeat('m', 128) FROM record ORDER BY id DESC LIMIT 1;
