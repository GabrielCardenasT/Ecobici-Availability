/*
  tests/assert_alerts_have_velocity_source.sql
  ─────────────────────────────────────────────
  Custom singular test.

  Business rule:
    Every alert row in mart_rebalancing_alerts must trace back to a row
    in int_station_velocity. An orphaned alert (no matching velocity row)
    indicates a join failure or a data pipeline gap.

  This test validates referential integrity across the model graph.
  Returns rows that FAIL (orphaned alerts). Passing test = zero rows.
*/

SELECT
    a.alert_id,
    a.station_id,
    a.ingested_at_utc,
    a.alert_type,
    'no_matching_velocity_row' AS failure_reason

FROM {{ ref('mart_rebalancing_alerts') }} a

LEFT JOIN {{ ref('int_station_velocity') }} v
    ON  a.station_id      = v.station_id
    AND a.ingested_at_utc = v.ingested_at_utc

WHERE v.station_id IS NULL
