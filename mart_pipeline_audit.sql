{{
    config(
        materialized     = 'incremental',
        unique_key       = ['audit_date', 'audit_hour'],
        partition_by     = {
            'field': 'audit_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        on_schema_change = 'append_new_columns',
        description      = 'Hourly pipeline health audit. Tracks ingestion gaps, station coverage, and data freshness. Used for SLA monitoring and alerting on pipeline failures.'
    )
}}

/*
  mart_pipeline_audit.sql
  ────────────────────────
  Answers: "Is the pipeline working correctly?"

  Every 5-minute poll should produce ~480–520 rows (one per active station).
  If a poll is missing entirely, this model surfaces the gap.
  If coverage drops below 90% of expected stations, it raises a flag.

  This is the model you point a monitoring alert at:
    SELECT * FROM mart_pipeline_audit
    WHERE audit_date = CURRENT_DATE('America/Mexico_City')
      AND (pipeline_gap_detected = TRUE OR coverage_pct < 0.90)

  One row per hour per day — deliberately coarser grain than the other marts
  to keep this table small and cheap to query for SLA reporting.
*/

WITH

snapshots AS (

    SELECT
        snapshot_date,
        EXTRACT(HOUR FROM ingested_at_cdmx)                         AS snapshot_hour,
        ingested_at_utc,
        station_id,
        is_stale_reading
    FROM {{ ref('stg_station_snapshots') }}

    {% if is_incremental() %}
    WHERE ingested_at_utc >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
    {% endif %}

),

-- Count distinct polls per hour (should be ~12 per hour at 5-min cadence)
polls_per_hour AS (

    SELECT
        snapshot_date,
        snapshot_hour,
        COUNT(DISTINCT ingested_at_utc)                             AS actual_polls,
        12                                                           AS expected_polls,
        -- 12 polls/hr = every 5 minutes
        COUNT(DISTINCT station_id)                                  AS stations_observed,
        COUNTIF(is_stale_reading) / NULLIF(COUNT(*), 0)             AS stale_reading_rate

    FROM snapshots
    GROUP BY snapshot_date, snapshot_hour

),

-- Expected station count: mode of stations_observed across recent hours.
-- We use MAX as a conservative proxy — the most stations ever seen in one poll.
expected_station_count AS (

    SELECT MAX(stations_observed) AS expected_stations
    FROM polls_per_hour

),

audit AS (

    SELECT
        p.snapshot_date                                              AS audit_date,
        p.snapshot_hour                                              AS audit_hour,
        p.actual_polls,
        p.expected_polls,
        p.stations_observed,
        e.expected_stations,

        ROUND(SAFE_DIVIDE(p.actual_polls, p.expected_polls), 4)     AS poll_completion_rate,

        ROUND(
            SAFE_DIVIDE(p.stations_observed, e.expected_stations),
            4
        )                                                            AS coverage_pct,

        ROUND(p.stale_reading_rate, 4)                              AS stale_reading_rate,

        -- Gap flag: if we got fewer than 8 of 12 expected polls this hour
        -- (allows for 4 missed cycles = 20 min grace window)
        p.actual_polls < 8                                           AS pipeline_gap_detected,

        -- Coverage flag: fewer than 90% of expected stations observed
        SAFE_DIVIDE(p.stations_observed, e.expected_stations) < 0.90 AS low_coverage_detected,

        -- Stale flag: more than 20% of readings are stale this hour
        p.stale_reading_rate > 0.20                                  AS high_staleness_detected,

        -- Overall health: TRUE if no flags
        (p.actual_polls >= 8
         AND SAFE_DIVIDE(p.stations_observed, e.expected_stations) >= 0.90
         AND p.stale_reading_rate <= 0.20)                           AS hour_is_healthy,

        CURRENT_TIMESTAMP()                                          AS dbt_run_at

    FROM polls_per_hour p
    CROSS JOIN expected_station_count e

)

SELECT * FROM audit
