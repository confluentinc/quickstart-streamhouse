-- One append-only row per passenger on the agent's flights, the first time their connection
-- turns HIGH risk, for the recovery agent. first_high groups passenger_state_changes by
-- passenger, and TO_CHANGELOG forwards only each group's first row (INSERT), so another delay
-- notice for an already-HIGH passenger adds no row. impacted_at is when that first HIGH change
-- landed. The agent handles three flights: RA417, the flight the demo follows, and RA183 and
-- RA185, whose delays build after it, so its offers arrive within minutes; the generator writes
-- every other flight's offers. It rebooks a passenger only while their inbound flight is still
-- due to land, which keeps out history days and earlier streams, whose flights landed long ago,
-- whatever the UTC date. The session must be in UTC, like the generator's timestamps. This is its own table because a Flink statement can hold only
-- one TO_CHANGELOG, and sql/31-insert-passenger-recommendations.sql uses its one to group passengers.
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
    AND SPLIT_INDEX(inbound_flight_id, '-', 0) IN ('RA417', 'RA183', 'RA185')
    AND CAST($rowtime AS TIMESTAMP(3)) < inbound_estimated_time
  GROUP BY `key`, inbound_flight_id, connecting_flight_id, final_destination
)
SELECT `key`, inbound_flight_id, connecting_flight_id, final_destination, impacted_at
FROM TO_CHANGELOG(
  input      => TABLE first_high PARTITION BY `key`,
  op_mapping => MAP['INSERT', 'FIRST_HIGH']
);
