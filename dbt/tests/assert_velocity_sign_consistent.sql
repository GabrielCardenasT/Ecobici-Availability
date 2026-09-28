/*
  tests/assert_velocity_sign_consistent.sql
  ──────────────────────────────────────────
  Custom singular test.

  Business rule:
    If velocity is strictly negative (station is draining) AND the prior
    snapshot had MORE bikes than the current snapshot, the sign is consistent.
    If velocity is strictly positive (station is refilling) AND the prior
    snapshot had FEWER bikes than the current snapshot, the sign is consistent.

  Failure condition:
    Any row where velocity is negative but bikes INCREASED (or vice versa).
    This would indicate a bug in the LAG window or the velocity macro.

  Returns rows that FAIL the assertion.
  A passing test returns zero rows.
*/

SELECT
    station_id,
    ingested_at_utc,
    prior_ingested_at_utc,
    num_bikes_available,
    prior_bikes_available,
    depletion_velocity_per_min,
    'velocity_sign_inconsistent_with_bike_delta' AS failure_reason

FROM {{ ref('int_station_velocity') }}

WHERE
    -- Only check rows where we have a valid prior snapshot and valid velocity
    prior_bikes_available     IS NOT NULL
    AND depletion_velocity_per_min IS NOT NULL
    AND minutes_since_prior_snapshot IS NOT NULL
    AND minutes_since_prior_snapshot BETWEEN 1 AND 30

    -- Inconsistency: velocity negative but bikes went UP
    AND (
        (depletion_velocity_per_min < -0.001
         AND num_bikes_available > prior_bikes_available)
        OR
        -- Inconsistency: velocity positive but bikes went DOWN
        (depletion_velocity_per_min > 0.001
         AND num_bikes_available < prior_bikes_available)
    )
