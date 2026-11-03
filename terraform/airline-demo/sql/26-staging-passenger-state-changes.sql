-- Append-only copy of every passenger_state change, for the recovery agent. AI_RUN_AGENT
-- can't read an upsert table, so TO_CHANGELOG turns each insert or update into a new row;
-- the row's $rowtime is when that change landed. Deletes are dropped.
-- sql/27-staging-impacted-passengers.sql reads it. Four buckets, like impacted_passengers,
-- spread the agent's work over four tasks.
CREATE OR ALTER MATERIALIZED TABLE passenger_state_changes (
  `key` STRING NOT NULL,
  inbound_flight_id STRING,
  connecting_flight_id STRING,
  final_destination STRING,
  risk STRING
) DISTRIBUTED BY (`key`) INTO 4 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'raw',
  'value.format' = 'avro-registry'
)
START_MODE = FROM_BEGINNING
AS SELECT
  `key`,
  inbound_flight_id,
  connecting_flight_id,
  final_destination,
  risk
FROM TO_CHANGELOG(
  input      => TABLE passenger_state PARTITION BY `key`,
  op_mapping => MAP['INSERT, UPDATE_AFTER', 'UPSERT']
);
