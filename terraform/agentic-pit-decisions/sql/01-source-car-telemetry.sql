-- The live telemetry feed `uv run f1-race` produces: one row per car per reading.
CREATE TABLE IF NOT EXISTS `car_telemetry` (
  `car_number` INT COMMENT 'Car number identifier',
  `lap` INT COMMENT 'Current lap number (1-60)',
  `tire_temp_fl_c` DOUBLE COMMENT 'Front-left tire temperature in Celsius',
  `tire_temp_fr_c` DOUBLE COMMENT 'Front-right tire temperature in Celsius',
  `tire_temp_rl_c` DOUBLE COMMENT 'Rear-left tire temperature in Celsius',
  `tire_temp_rr_c` DOUBLE COMMENT 'Rear-right tire temperature in Celsius',
  `tire_pressure_fl_psi` DOUBLE COMMENT 'Front-left tire pressure in PSI',
  `tire_pressure_fr_psi` DOUBLE COMMENT 'Front-right tire pressure in PSI',
  `tire_pressure_rl_psi` DOUBLE COMMENT 'Rear-left tire pressure in PSI',
  `tire_pressure_rr_psi` DOUBLE COMMENT 'Rear-right tire pressure in PSI',
  `engine_temp_c` DOUBLE COMMENT 'Engine temperature in Celsius',
  `brake_temp_fl_c` DOUBLE COMMENT 'Front-left brake temperature in Celsius',
  `brake_temp_fr_c` DOUBLE COMMENT 'Front-right brake temperature in Celsius',
  `battery_charge_pct` DOUBLE COMMENT 'Hybrid battery charge percentage (0-100)',
  `fuel_remaining_kg` DOUBLE COMMENT 'Remaining fuel in kilograms',
  `drs_active` BOOLEAN COMMENT 'Drag Reduction System active flag',
  `speed_kph` DOUBLE COMMENT 'Current speed in km/h',
  `throttle_pct` DOUBLE COMMENT 'Throttle pedal position percentage (0-100)',
  `brake_pct` DOUBLE COMMENT 'Brake pedal position percentage (0-100)',
  `event_time` TIMESTAMP(3) COMMENT 'Sensor reading timestamp',
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
)
DISTRIBUTED INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.compaction.time' = '0 ms',
  'kafka.consumer.isolation-level' = 'read-uncommitted',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.message-timestamp-type' = 'create-time',
  'kafka.retention.size' = '0 bytes',
  'kafka.retention.time' = '0 ms',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
);
