{{
    config(
        materialized     = 'table',
        partition_by     = {
            'field': 'audit_date',
            'data_type': 'date',
        },
        on_schema_change = 'append_new_columns',
        description      = 'Hourly pipeline health audit. Tracks ingestion gaps, station coverage, and data freshness. Used for SLA monitoring and alerting on pipeline failures.'
    )
}}

/*
  mart_pipeline_audit.sql
  Answers: "Is the pipeline working correctly?"

  Grain: one row per CDMX date per CDMX hour.

  Why this model reads the RAW source instead of stg_station_snapshots:
    Staging keeps only stations that are installed AND renting, inside the
    service window. That is right for analytics but wrong for auditing
    ingestion: between 00:30 and 05:00 ECOBICI is closed, almost no station
    is renting, and coverage measured on staging collapsed to ~0.6% — the
    audit flagged a healthy pipeline as broken every night.
    Pipeline health = "did we poll, and did the feed return every station?"
    The raw table answers that; renting status is reported separately.

  Monitoring query:
    SELECT * FROM mart_pipeline_audit
    WHERE audit_date = CURRENT_DATE('America/Mexico_City')
      AND audit_status IN ('gap', 'low_coverage')

  Note on poll_completion_rate > 1.0:
    cron-job.org fires every 15 min (4/h) and the GitHub `schedule` fallback
    adds one more run per hour, so 5 polls/hour → 1.25 is normal.
*/

WITH

raw_polls AS (

    SELECT
        station_id,
        is_renting,
        ingested_at_utc,
        station_last_reported_utc,
        DATETIME(ingested_at_utc, 'America/Mexico_City')            AS ingested_at_cdmx
    FROM {{ source('ecobici_raw', 'station_snapshots') }}
    -- Satisfies require_partition_filter on the raw table
    WHERE ingested_at_utc >= TIMESTAMP_SUB(
        CURRENT_TIMESTAMP(),
        INTERVAL {{ var('lookback_days') }} DAY
    )

),

polls_per_hour AS (

    SELECT
        DATE(ingested_at_cdmx)                                      AS audit_date,
        EXTRACT(HOUR FROM ingested_at_cdmx)                         AS audit_hour,
        COUNT(DISTINCT ingested_at_utc)                             AS actual_polls,
        4                                                           AS expected_polls,
        COUNT(DISTINCT station_id)                                  AS stations_observed,
        COUNT(DISTINCT IF(is_renting, station_id, NULL))            AS stations_renting,
        SAFE_DIVIDE(
            COUNTIF(TIMESTAMP_DIFF(ingested_at_utc, station_last_reported_utc, MINUTE) > 15),
            COUNT(*)
        )                                                           AS stale_reading_rate,

        -- Hour is fully inside the closed window (01:00–04:59 with defaults).
        -- The 00:00 hour is partly open (until 00:30), so it is audited normally.
        LOGICAL_AND(
            TIME(ingested_at_cdmx) >= TIME '{{ var("service_close_time") }}'
            AND TIME(ingested_at_cdmx) < TIME '{{ var("service_open_time") }}'
        )                                                           AS is_system_closed,

        -- The current CDMX hour is still collecting polls. Judging it now
        -- flagged a false 'gap' every time dbt ran early in the hour.
        LOGICAL_OR(
            DATE(ingested_at_cdmx) = CURRENT_DATE('America/Mexico_City')
            AND EXTRACT(HOUR FROM ingested_at_cdmx)
                = EXTRACT(HOUR FROM CURRENT_DATETIME('America/Mexico_City'))
        )                                                           AS is_in_progress

    FROM raw_polls
    GROUP BY audit_date, audit_hour

),

expected_station_count AS (

    SELECT MAX(stations_observed) AS expected_stations
    FROM polls_per_hour

),

audit AS (

    SELECT
        p.audit_date,
        p.audit_hour,
        p.actual_polls,
        p.expected_polls,
        p.stations_observed,
        p.stations_renting,
        e.expected_stations,

        ROUND(SAFE_DIVIDE(p.actual_polls, p.expected_polls), 4)     AS poll_completion_rate,
        ROUND(SAFE_DIVIDE(p.stations_observed, e.expected_stations), 4) AS coverage_pct,
        ROUND(p.stale_reading_rate, 4)                              AS stale_reading_rate,
        p.is_system_closed,
        p.is_in_progress,

        -- Gap: fewer than 2 of 4 expected polls this hour.
        -- Never raised for the hour still in progress (it has not finished).
        (NOT p.is_in_progress AND p.actual_polls < 2)               AS pipeline_gap_detected,

        -- Coverage: the feed returned fewer than 90% of known stations
        SAFE_DIVIDE(p.stations_observed, e.expected_stations) < 0.90 AS low_coverage_detected,

        FALSE                                                       AS high_staleness_detected,

        -- Healthy = we polled AND the feed was complete. Applies at night too:
        -- the pipeline should keep polling while the system is closed.
        -- NULL for the hour in progress: it cannot be judged yet.
        IF(p.is_in_progress, NULL,
           p.actual_polls >= 2
           AND SAFE_DIVIDE(p.stations_observed, e.expected_stations) >= 0.90) AS hour_is_healthy,

        -- Single label for dashboards (drives the row colour in Looker Studio)
        CASE
            WHEN p.is_in_progress                                          THEN 'in_progress'
            WHEN p.actual_polls < 2                                        THEN 'gap'
            WHEN SAFE_DIVIDE(p.stations_observed, e.expected_stations) < 0.90 THEN 'low_coverage'
            WHEN p.is_system_closed                                        THEN 'system_closed'
            ELSE 'healthy'
        END                                                         AS audit_status,

        CURRENT_TIMESTAMP()                                         AS dbt_run_at

    FROM polls_per_hour p
    CROSS JOIN expected_station_count e

)

SELECT * FROM audit
