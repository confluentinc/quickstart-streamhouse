-- as_of is when the hotel reported the rooms. Lightning Tables keep every version of an upsert
-- row, so readers order by as_of and take the newest row per hotel.
CREATE TABLE IF NOT EXISTS hotel_inventory (
  `key` STRING NOT NULL,
  available_rooms INT,
  nightly_rate DECIMAL(10, 2),
  as_of TIMESTAMP(3),
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'key.format' = 'raw', 'value.format' = 'avro-registry');
