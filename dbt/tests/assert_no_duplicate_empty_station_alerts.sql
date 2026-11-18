/*
  tests/assert_no_duplicate_empty_station_alerts.sql
  Regression test: an empty station must raise BIKE_SHORTAGE only.
  IMMINENT_EMPTY ("will run out soon") and CRITICAL_DRAIN ("losing bikes
  fast") are meaningless once a station has 0 bikes; before the fix every
  empty station could produce two or three alerts for the same problem.
  Passing test = zero rows.
*/
SELECT alert_id, station_id, alert_type, num_bikes_available
FROM {{ ref('mart_rebalancing_alerts') }}
WHERE alert_type IN ('IMMINENT_EMPTY', 'CRITICAL_DRAIN')
  AND num_bikes_available = 0
