-- connecting_flight_id and connecting_seat are null for a passenger whose trip ends at the
-- hub. Seats look like 14C.
CREATE TABLE IF NOT EXISTS passenger_itineraries (
  `key` STRING NOT NULL,
  inbound_flight_id STRING,
  inbound_seat STRING,
  connecting_flight_id STRING,
  connecting_seat STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'key.format' = 'raw', 'value.format' = 'avro-registry');
