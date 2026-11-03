CREATE TABLE IF NOT EXISTS hotel_inventory (
  `key` STRING NOT NULL,
  available_rooms INT,
  nightly_rate DECIMAL(10, 2),
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'key.format' = 'raw', 'value.format' = 'avro-registry');
