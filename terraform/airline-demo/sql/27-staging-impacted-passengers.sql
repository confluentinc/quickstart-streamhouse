-- One append-only row per RA417 passenger, the first time their connection turns HIGH risk,
-- for the recovery agent. first_high groups passenger_state_changes by passenger, and
-- TO_CHANGELOG forwards only each group's first row (INSERT), so another delay notice for an
-- already-HIGH passenger adds no row. impacted_at is when that first HIGH change landed.
-- The agent handles only today's RA417, the flight the demo follows, so its offers arrive
-- within minutes; the generator writes every other flight's offers. This is its
-- own table because a Flink statement can hold only one TO_CHANGELOG, and
-- sql/31-insert-passenger-recommendations.sql uses its one to group passengers.
CREATE OR ALTER MATERIALIZED TABLE impacted_passengers (
  `key` STRING NOT NULL,
  inbound_flight_id STRING,
  connecting_flight_id STRING,
  final_destination STRING,
  impacted_at TIMESTAMP(3)
) DISTRIBUTED BY (`key`) INTO 4 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
)
START_MODE = FROM_BEGINNING
AS WITH first_high AS (
  SELECT
    `key`,
    inbound_flight_id,
    connecting_flight_id,
    final_destination,
    CAST(MIN($rowtime) AS TIMESTAMP(3)) AS impacted_at
  FROM passenger_state_changes
  WHERE risk = 'HIGH'
    AND inbound_flight_id = CONCAT('RA417-', DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd'))
  GROUP BY `key`, inbound_flight_id, connecting_flight_id, final_destination
)
SELECT `key`, inbound_flight_id, connecting_flight_id, final_destination, impacted_at
FROM TO_CHANGELOG(
  input      => TABLE first_high PARTITION BY `key`,
  op_mapping => MAP['INSERT', 'FIRST_HIGH']
);
