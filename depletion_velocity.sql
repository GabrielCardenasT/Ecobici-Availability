{#
  macros/depletion_velocity.sql
  ─────────────────────────────
  Calculates the rate of change of bike availability between consecutive
  snapshots for a given station. Returns bikes lost per minute (negative
  means bikes are being drained; positive means bikes are returning).

  Formula:
    velocity = (current_bikes - prior_bikes) / minutes_elapsed

  Arguments:
    bikes_col       : column name holding current num_bikes_available
    prior_bikes_col : column name holding prior snapshot's bike count (from LAG)
    ts_col          : column name holding current ingested_at_utc
    prior_ts_col    : column name holding prior snapshot's timestamp (from LAG)

  Usage in a SELECT:
    {{ depletion_velocity(
         bikes_col       = 'num_bikes_available',
         prior_bikes_col = 'prior_bikes_available',
         ts_col          = 'ingested_at_utc',
         prior_ts_col    = 'prior_ingested_at_utc'
    ) }} AS depletion_velocity_per_min

  Returns NULL when:
    - prior_bikes_col IS NULL (first snapshot for this station)
    - minutes_elapsed = 0 (duplicate timestamps — defensive guard)
    - minutes_elapsed > 30 (gap too large — stale data, not a real trend)

  Returns a FLOAT64. Negative = net drain. Positive = net refill.
  Typical range during rush hour: -0.8 to -0.2 bikes/min per station.
  Interpretation: -0.5 means the station is losing ~1 bike every 2 minutes.
#}

{% macro depletion_velocity(
    bikes_col       = 'num_bikes_available',
    prior_bikes_col = 'prior_bikes_available',
    ts_col          = 'ingested_at_utc',
    prior_ts_col    = 'prior_ingested_at_utc'
) %}

CASE
    -- No prior snapshot: first observation for this station in the window
    WHEN {{ prior_bikes_col }} IS NULL
        THEN NULL

    -- Gap too large: more than 30 minutes between polls suggests an outage
    -- or a cold-start. Velocity over a 30-min gap is not operationally useful.
    WHEN TIMESTAMP_DIFF({{ ts_col }}, {{ prior_ts_col }}, MINUTE) > 30
        THEN NULL

    -- Guard against division by zero (duplicate or out-of-order timestamps)
    WHEN TIMESTAMP_DIFF({{ ts_col }}, {{ prior_ts_col }}, MINUTE) = 0
        THEN NULL

    -- Core formula: delta bikes / delta minutes
    ELSE
        SAFE_DIVIDE(
            CAST({{ bikes_col }} - {{ prior_bikes_col }} AS FLOAT64),
            CAST(TIMESTAMP_DIFF({{ ts_col }}, {{ prior_ts_col }}, MINUTE) AS FLOAT64)
        )
END

{% endmacro %}


{#
  estimated_minutes_to_empty
  ──────────────────────────
  Given a current bike count and a depletion velocity, estimates how many
  minutes until the station runs out of bikes.

  Returns NULL if velocity is zero or positive (station is refilling).
  Returns NULL if current_bikes is already 0.
  Caps at 999 to avoid absurdly large numbers when drain rate is nearly zero.

  Usage:
    {{ estimated_minutes_to_empty(
         bikes_col  = 'num_bikes_available',
         velocity_col = 'depletion_velocity_per_min'
    ) }} AS est_minutes_to_empty
#}

{% macro estimated_minutes_to_empty(
    bikes_col    = 'num_bikes_available',
    velocity_col = 'depletion_velocity_per_min'
) %}

CASE
    WHEN {{ velocity_col }} IS NULL       THEN NULL
    WHEN {{ velocity_col }} >= 0          THEN NULL   -- refilling, not draining
    WHEN {{ bikes_col }}    <= 0          THEN 0      -- already empty
    ELSE LEAST(
        CAST(999 AS FLOAT64),
        SAFE_DIVIDE(
            CAST(-{{ bikes_col }} AS FLOAT64),
            {{ velocity_col }}               -- velocity is negative, so -v is positive
        )
    )
END

{% endmacro %}
