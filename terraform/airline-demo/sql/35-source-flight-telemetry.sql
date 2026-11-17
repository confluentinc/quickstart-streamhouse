CREATE TABLE IF NOT EXISTS flight_telemetry (
  `key` STRING NOT NULL,
  as_of TIMESTAMP(3),
  stage STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  altitude_ft INT,
  ground_speed_kt DOUBLE,
  vertical_speed_fpm INT,
  heading_deg DOUBLE,
  distance_to_go_nm DOUBLE,
  hold_ends_at TIMESTAMP(3),
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'key.format' = 'raw', 'value.format' = 'avro-registry');
