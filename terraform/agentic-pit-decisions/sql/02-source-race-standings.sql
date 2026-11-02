-- The race timing feed: the latest standing per car, the versioned side of Step 1's temporal join.
CREATE TABLE IF NOT EXISTS `race_standings` (
  `car_number` INT COMMENT 'Car number identifier',
  `driver` STRING COMMENT 'Driver full name',
  `team` STRING COMMENT 'Constructor team name',
  `lap` INT COMMENT 'Current lap number (1-60)',
  `position` INT COMMENT 'Current race position (1-22)',
  `gap_to_leader_sec` DOUBLE COMMENT 'Time gap to race leader in seconds',
  `gap_to_ahead_sec` DOUBLE COMMENT 'Time gap to car directly ahead in seconds',
  `last_lap_time_sec` DOUBLE COMMENT 'Last completed lap time in seconds',
  `pit_stops` INT COMMENT 'Number of pit stops completed',
  `tire_compound` STRING COMMENT 'Current tire compound (SOFT, MEDIUM, HARD)',
  `tire_age_laps` INT COMMENT 'Number of laps on current set of tires',
  `in_pit_lane` BOOLEAN COMMENT 'Whether car is currently in the pit lane',
  `event_time` TIMESTAMP(3) COMMENT 'FIA timing feed timestamp',
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND,
  PRIMARY KEY (`car_number`) NOT ENFORCED
) DISTRIBUTED BY (`car_number`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'connector' = 'confluent',
  'kafka.consumer.isolation-level' = 'read-uncommitted',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
);
