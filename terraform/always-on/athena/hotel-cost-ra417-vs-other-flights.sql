-- Booked hotel cost in the last 30 days: RA417's passengers against everyone else's.
SELECT split_part(key, '-', 3) = '417' AS ra417, count(*) AS bookings, sum(hotel_cost) AS hotel_cost
FROM passenger_recommendations
WHERE status = 'BOOKED' AND recommended_at >= current_date - INTERVAL '30' DAY
GROUP BY 1;
