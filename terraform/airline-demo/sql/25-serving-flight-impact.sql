-- One row per flight: its latest status, delay, and how many passengers it puts at HIGH risk.
-- Tableflow syncs this table to Iceberg for the historical questions. passenger_state is read
-- uncommitted so a new HIGH count doesn't wait for that table's once-a-minute Flink commit.
CREATE OR ALTER MATERIALIZED TABLE flight_impact (
  `key` STRING NOT NULL,
  origin STRING,
  destination STRING,
  scheduled_time TIMESTAMP(3),
  status STRING,
  delay_minutes INT,
  affected_passengers INT,
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'raw',
  'kafka.cleanup-policy' = 'compact',
  'value.format' = 'avro-registry'
)
START_MODE = FROM_BEGINNING
AS SELECT
  f.`key`,
  f.origin,
  f.destination,
  f.scheduled_time,
  f.status,
  TIMESTAMPDIFF(MINUTE, f.scheduled_time, f.estimated_time) AS delay_minutes,
  COALESCE(impact.affected_passengers, 0) AS affected_passengers
FROM flight_status f
LEFT JOIN (
  SELECT inbound_flight_id, CAST(COUNT(*) FILTER (WHERE risk = 'HIGH') AS INT) AS affected_passengers
  FROM passenger_state /*+ OPTIONS('kafka.consumer.isolation-level' = 'read-uncommitted') */
  GROUP BY inbound_flight_id
) impact ON f.`key` = impact.inbound_flight_id;
