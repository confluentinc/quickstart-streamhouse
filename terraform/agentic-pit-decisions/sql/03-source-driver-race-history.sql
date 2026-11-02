-- Past results per driver, seeded by `uv run deploy` with a bounded INSERT of 198 rows.
CREATE TABLE IF NOT EXISTS `driver_race_history` (
  `race_id` STRING COMMENT 'Race identifier (e.g. bahrain_2026)',
  `gp_name` STRING COMMENT 'Grand Prix name',
  `race_date` DATE COMMENT 'Date the race was held',
  `car_number` INT COMMENT 'Car number identifier',
  `driver` STRING COMMENT 'Driver full name',
  `team` STRING COMMENT 'Constructor team name',
  `starting_grid` INT COMMENT 'Starting grid position',
  `finishing_pos` INT COMMENT 'Finishing position',
  `positions_gained` INT COMMENT 'Positions gained (start - finish)',
  `pit_stops` INT COMMENT 'Number of pit stops',
  `stint_1_tire` STRING COMMENT 'Tire compound for stint 1',
  `stint_2_tire` STRING COMMENT 'Tire compound for stint 2',
  `stint_3_tire` STRING COMMENT 'Tire compound for stint 3 (or n/a)'
)
DISTRIBUTED INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.consumer.isolation-level' = 'read-uncommitted',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
);
