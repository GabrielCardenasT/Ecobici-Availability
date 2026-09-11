{{
    config(
        materialized  = 'table',
        partition_by  = {
            'field': 'ingested_at_utc',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by    = ['station_id', 'time_period'],
        description   = 'Per-station, per-snapshot depletion velocity computed via LAG window function. Core intermediate model — all alert and health mart models derive from this.'
    )
}}

/*
  int_station_velocity.sql
  ─────────────────────────
  Computes the "depletion velocity" for every station at every snapshot:
    - How fast is this station losing bikes right now? (bikes per minute)
    - How many minutes until it runs out at this rate?

  Window function design:
    LAG() OVER (PARTITION BY station_id ORDER BY ingested_at_utc)

    Partitioning by station_id is critical: it ensures we compare a station
    only to its own prior snapshot, not to a different station's reading.
    Ordering by ingested_at_utc gives us the chronologically previous poll.

  The depletion_velocity() and estimated_minutes_to_empty() macros handle
  the NULL guards and edge cases — see macros/depletion_velocity.sql.

  Output cardinality: same as stg_station_snapshots (one row per station
  per poll), plus the velocity and estimated empty-time columns.
*/

WITH

snapshots AS (

    SELECT
        station_id,
        short_name,
        name,
        lat,
        lon,
        num_bikes_available,
        num_docks_available,
        total_operational_capacity,
        availability_pct,
        dock_pct,
        is_critically_low_bikes,
        is_critically_low_docks,
        hour_of_day,
        day_of_week,
        time_period,
        snapshot_date,
        ingested_at_utc,
        ingested_at_cdmx,
        is_stale_reading
    FROM {{ ref('stg_station_snapshots') }}

),

with_lag AS (

    SELECT
        *,

        -- ── Prior-snapshot values via LAG window ──────────────────────────────
        -- These are the inputs to the depletion_velocity macro.
        LAG(num_bikes_available) OVER (
            PARTITION BY station_id
            ORDER BY     ingested_at_utc
        ) AS prior_bikes_available,

        LAG(num_docks_available) OVER (
            PARTITION BY station_id
            ORDER BY     ingested_at_utc
        ) AS prior_docks_available,

        LAG(ingested_at_utc) OVER (
            PARTITION BY station_id
            ORDER BY     ingested_at_utc
        ) AS prior_ingested_at_utc,

        -- Minutes since last snapshot (for gap detection in the macro)
        TIMESTAMP_DIFF(
            ingested_at_utc,
            LAG(ingested_at_utc) OVER (
                PARTITION BY station_id
                ORDER BY     ingested_at_utc
            ),
            MINUTE
        ) AS minutes_since_prior_snapshot

    FROM snapshots

),

with_velocity AS (

    SELECT
        *,

        -- ── Bike depletion velocity (macro call) ──────────────────────────────
        -- Result: bikes lost per minute. Negative = draining. Positive = refilling.
        -- NULL = first snapshot or gap > 30 min (see macro for full logic).
        {{ depletion_velocity(
             bikes_col       = 'num_bikes_available',
             prior_bikes_col = 'prior_bikes_available',
             ts_col          = 'ingested_at_utc',
             prior_ts_col    = 'prior_ingested_at_utc'
        ) }} AS depletion_velocity_per_min,

        -- ── Dock fill velocity (inverse: how fast docks are disappearing) ─────
        -- Positive = docks are filling up (bikes arriving). Negative = docks freeing.
        {{ depletion_velocity(
             bikes_col       = 'num_docks_available',
             prior_bikes_col = 'prior_docks_available',
             ts_col          = 'ingested_at_utc',
             prior_ts_col    = 'prior_ingested_at_utc'
        ) }} AS dock_fill_velocity_per_min

    FROM with_lag

),

final AS (

    SELECT
        -- ── Core identity + geography ─────────────────────────────────────────
        station_id,
        short_name,
        name,
        lat,
        lon,

        -- ── Snapshot state ────────────────────────────────────────────────────
        num_bikes_available,
        num_docks_available,
        total_operational_capacity,
        availability_pct,
        dock_pct,
        is_critically_low_bikes,
        is_critically_low_docks,
        is_stale_reading,

        -- ── Velocity metrics (the new columns this model contributes) ─────────
        prior_bikes_available,
        prior_ingested_at_utc,
        minutes_since_prior_snapshot,
        depletion_velocity_per_min,
        dock_fill_velocity_per_min,

        -- ── Estimated minutes to empty (macro call) ───────────────────────────
        {{ estimated_minutes_to_empty(
             bikes_col    = 'num_bikes_available',
             velocity_col = 'depletion_velocity_per_min'
        ) }} AS est_minutes_to_empty,

        -- ── Velocity severity classification ──────────────────────────────────
        -- Operationalizes the velocity into discrete alert levels.
        -- Thresholds derived from: at ~500 stations avg capacity ~20 bikes,
        -- -0.5 bikes/min means empty in 10 min — that's critical.
        CASE
            WHEN depletion_velocity_per_min IS NULL         THEN 'unknown'
            WHEN depletion_velocity_per_min <= -0.5         THEN 'critical_drain'
            WHEN depletion_velocity_per_min <= -0.2         THEN 'moderate_drain'
            WHEN depletion_velocity_per_min <= -0.05        THEN 'slow_drain'
            WHEN depletion_velocity_per_min <   0.05
             AND depletion_velocity_per_min >  -0.05        THEN 'stable'
            WHEN depletion_velocity_per_min >=  0.05        THEN 'refilling'
            ELSE 'unknown'
        END AS velocity_status,

        -- ── Temporal context ──────────────────────────────────────────────────
        hour_of_day,
        day_of_week,
        time_period,
        snapshot_date,
        ingested_at_utc,
        ingested_at_cdmx

    FROM with_velocity

)

SELECT * FROM final
