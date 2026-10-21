-- Keeps hotel_inventory current from the webhook feed: the latest update per hotel wins.
INSERT INTO hotel_inventory
SELECT
  hotel_name AS `key`,
  CAST(available_rooms AS INT) AS available_rooms,
  CAST(nightly_rate AS DECIMAL(10, 2)) AS nightly_rate
FROM hotel_availability_updates;
