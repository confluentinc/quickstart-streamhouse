CREATE TABLE IF NOT EXISTS flight_status (
  `key` STRING NOT NULL,
  origin STRING,
  destination STRING,
  scheduled_time TIMESTAMP(3),
  estimated_time TIMESTAMP(3),
  status STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'key.format' = 'raw', 'value.format' = 'avro-registry');
