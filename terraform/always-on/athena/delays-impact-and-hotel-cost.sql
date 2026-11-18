-- Scene 3, question 1: delayed flights, impacted passengers, and hotel cost in the last 30 days.
SELECT
  (SELECT count_if(delay_minutes >= 15) FROM flight_impact
    WHERE scheduled_departure >= current_date - INTERVAL '30' DAY) AS delayed_flights,
  (SELECT sum(affected_passengers) FROM flight_impact
    WHERE scheduled_departure >= current_date - INTERVAL '30' DAY) AS impacted_passengers,
  (SELECT sum(hotel_cost) FROM passenger_recommendations
    WHERE status = 'BOOKED' AND recommended_at >= current_date - INTERVAL '30' DAY) AS hotel_cost;
