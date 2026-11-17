/*
  tests/assert_empty_stations_get_max_priority.sql
  ─────────────────────────────────────────────────
  Regression test for the "priority score = 0" bug.

  Business rule:
    A station with zero bikes (BIKE_SHORTAGE) or zero free docks
    (DOCK_OVERFLOW) is already failing riders, so it must sit at the top
    of the dispatch queue with score 1.0 — whatever its velocity.

  The old formula multiplied by COALESCE(ABS(velocity)/0.5, 0.5); a station
  that stayed empty across polls had velocity = 0.0 (not NULL), so the
  score collapsed to 0 and empty stations sank to the BOTTOM of the queue.

  Returns failing rows. Passing test = zero rows.
*/

SELECT
    alert_id,
    station_id,
    alert_type,
    num_bikes_available,
    num_docks_available,
    dispatch_priority_score
FROM {{ ref('mart_rebalancing_alerts') }}
WHERE dispatch_priority_score < 1.0
  AND (
        (alert_type = 'BIKE_SHORTAGE' AND num_bikes_available = 0)
     OR (alert_type = 'DOCK_OVERFLOW' AND num_docks_available = 0)
  )
