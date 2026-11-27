-- Tableflow keeps one row per key of the upsert topics: both counts should match.
SELECT 'flight_impact' AS table_name, count(*) AS row_count, count(DISTINCT key) AS key_count FROM flight_impact
UNION ALL
SELECT 'passenger_recommendations', count(*), count(DISTINCT key) FROM passenger_recommendations;
