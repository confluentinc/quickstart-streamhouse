-- One row per passenger. A passenger whose trip ends at the hub has no connection to miss.
CREATE OR ALTER MATERIALIZED TABLE passenger_state (
  `key` STRING NOT NULL,
  inbound_flight_id STRING,
  inbound_seat STRING,
  connecting_flight_id STRING,
  connecting_seat STRING,
  final_destination STRING,
  connection_minutes INT,
  risk STRING,
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
  p.`key`,
  p.inbound_flight_id,
  p.inbound_seat,
  p.connecting_flight_id,
  p.connecting_seat,
  COALESCE(onward.destination, inbound.destination) AS final_destination,
  TIMESTAMPDIFF(MINUTE, inbound.estimated_time, onward.estimated_time) AS connection_minutes,
  CASE
    WHEN p.connecting_flight_id IS NULL THEN 'NO_CONNECTION'
    WHEN TIMESTAMPDIFF(MINUTE, inbound.estimated_time, onward.estimated_time) < 45 THEN 'HIGH'
    ELSE 'OK'
  END AS risk
FROM passenger_itineraries p
JOIN flight_status inbound ON p.inbound_flight_id = inbound.`key`
LEFT JOIN flight_status onward ON p.connecting_flight_id = onward.`key`;
