-- Scene 3, question 2: average time from impacted to offer ready, in seconds.
SELECT avg(to_unixtime(recommended_at) - to_unixtime(impacted_at)) AS avg_recovery_seconds
FROM passenger_recommendations
WHERE status = 'BOOKED' AND recommended_at >= current_date - INTERVAL '30' DAY;
