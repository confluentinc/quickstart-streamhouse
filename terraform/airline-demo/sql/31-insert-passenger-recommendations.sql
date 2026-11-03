-- Runs the recovery agent for the passengers in impacted_passengers and writes two offers per
-- passenger to passenger_recommendations.
--
-- Passengers on the same inbound flight to the same final destination get the same offers,
-- so first_group groups them and TO_CHANGELOG forwards only each group's first row (INSERT):
-- the agent runs once per inbound flight and destination. The join hands its reply to every
-- passenger in that group, including ones who turn HIGH later.
-- AI_RUN_AGENT can't run inside a materialized table, hence CREATE TABLE plus this INSERT.
INSERT INTO passenger_recommendations
WITH first_group AS (
  SELECT
    CONCAT(inbound_flight_id, '>', final_destination) AS group_key,
    inbound_flight_id,
    final_destination,
    MIN(impacted_at) AS impacted_at
  FROM impacted_passengers
  GROUP BY inbound_flight_id, final_destination
),
new_groups AS (
  SELECT *
  FROM TO_CHANGELOG(
    input      => TABLE first_group PARTITION BY group_key,
    op_mapping => MAP['INSERT', 'FIRST_GROUP']
  )
),
plans AS (
  SELECT g.inbound_flight_id, g.final_destination, r.response
  FROM new_groups g,
  LATERAL TABLE(AI_RUN_AGENT(
    'passenger_recovery_agent',
    CONCAT(
      'inbound_flight_id=', g.inbound_flight_id,
      '; final_destination=', g.final_destination,
      '; inbound_query=SELECT * FROM `flight_status` WHERE "KEY" = ''', g.inbound_flight_id, ''' LIMIT 1',
      '; departures_query=SELECT * FROM `flight_status` WHERE origin = ''SFO'' AND destination = ''',
      g.final_destination, ''' AND scheduled_time >= TIMESTAMP ''',
      DATE_FORMAT(g.impacted_at - INTERVAL '1' HOUR, 'yyyy-MM-dd HH:mm:ss'),
      ''' ORDER BY scheduled_time ASC LIMIT 12',
      '; hotel_query=SELECT * FROM `hotel_inventory` LIMIT 10'
    ),
    g.group_key
  )) AS r(agent_status, response)
),
offers AS (
  SELECT
    h.`key` AS passenger_id,
    h.impacted_at,
    o.n,
    NULLIF(REGEXP_EXTRACT(p.response, CONCAT('O', o.n, '_FLIGHT=([^;\s]+)'), 1), 'NONE') AS recommended_flight_id,
    NULLIF(TRIM(REGEXP_EXTRACT(p.response, CONCAT('O', o.n, '_HOTEL=([^;\n]+)'), 1)), 'NONE') AS hotel_name,
    TRY_CAST(REGEXP_EXTRACT(p.response, CONCAT('O', o.n, '_COST=([0-9.]+)'), 1) AS DECIMAL(10, 2)) AS hotel_cost
  FROM impacted_passengers h
  JOIN plans p
    ON h.inbound_flight_id = p.inbound_flight_id AND h.final_destination = p.final_destination
  CROSS JOIN UNNEST(ARRAY['1', '2']) AS o(n)
)
SELECT
  CONCAT(passenger_id, '-O', n) AS `key`,
  passenger_id,
  recommended_flight_id,
  hotel_name,
  'OFFERED' AS status,
  hotel_cost,
  impacted_at,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS recommended_at
FROM offers
WHERE recommended_flight_id IS NOT NULL;
