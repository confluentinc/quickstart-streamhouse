-- impacted_at is when the passenger's connection became HIGH risk; recommended_at is when
-- the offer was written. Their difference is the recovery time, excluding the customer's
-- decision time. hotel_name and hotel_cost are null for a same-day rebooking.
CREATE TABLE IF NOT EXISTS passenger_recommendations (
  `key` STRING NOT NULL,
  passenger_id STRING,
  recommended_flight_id STRING,
  hotel_name STRING,
  status STRING,
  hotel_cost DECIMAL(10, 2),
  impacted_at TIMESTAMP(3),
  recommended_at TIMESTAMP(3),
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'raw',
  'kafka.cleanup-policy' = 'compact',
  'value.format' = 'avro-registry'
);
