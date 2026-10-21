-- Where the hotel partner's webhook lands. The Webhooks source connector writes one record per
-- update to this topic: it cannot set the record key, so the key is a random id and the hotel
-- name travels in the value. Flink keys it into hotel_inventory (sql/34).
-- One bucket keeps the updates in arrival order. Number and string types match what the
-- connector infers from JSON (whole numbers become BIGINT), so it can share this schema.
CREATE TABLE IF NOT EXISTS hotel_availability_updates (
  hotel_name STRING,
  available_rooms BIGINT,
  nightly_rate STRING
) DISTRIBUTED INTO 1 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');
