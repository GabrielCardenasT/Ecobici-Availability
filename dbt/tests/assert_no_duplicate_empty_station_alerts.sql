/*
  tests/assert_no_duplicate_empty_station_alerts.sql
  Regression test: an empty station must raise BIKE_SHORTAGE only.
  IMMINENT_EMPTY ("will run out soon") is meaningless once a station has
  0 bikes; before the fix every empty station produced both alerts.
  Passing test = zero rows.
*/
SELECT alert_id, station_id, num_bikes_available
FROM {{ ref('mart_rebalancing_alerts') }}
WHERE alert_type = 'IMMINENT_EMPTY'
  AND num_bikes_available = 0
