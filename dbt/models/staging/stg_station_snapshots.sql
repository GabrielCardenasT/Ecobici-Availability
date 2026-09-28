{{
    config(
        materialized = 'view',
        description  = 'Cleaned and lightly enriched view over the raw GBFS snapshot table. One row per station per 5-min poll. This is the only model that references the raw source directly.'
    )
}}

/*
  stg_station_snapshots.sql
  ──────────────────────────
  Staging contract:
    - All column names are exactly as defined in schema.py BIGQUERY_SCHEMA
    - No business logic — only cleaning, casting, and temporal grain derivation
    - Filters to only INSTALLED + RENTING stations (offline stations carry no
      operational meaning for rebalancing decisions)
    - Adds hour_of_day and day_of_week for downstream window partitioning
      without repeating EXTRACT() in every intermediate model

  Partition filter: ingested_at_utc >= lookback window
    The raw table has require_partition_filter = true (Phase 2 Terraform).
    This WHERE clause satisfies that requirement and keeps query costs low.
    var('lookback_days') defaults to 2 — enough for velocity windows + 1 day buffer.
*/

WITH

source AS (

    SELECT
        -- Identity
        station_id,
        short_name,
        name,

        -- Geography (passed through unchanged — used in mart_cluster_health)
        lat,
        lon,

        -- Capacity (raw counts — staging doesn't recompute derived fields)
        capacity,
        num_bikes_available,
        num_bikes_disabled,
        num_docks_available,
        num_docks_disabled,
        total_operational_capacity,

        -- Availability ratios (already computed in Python — staging trusts them)
        availability_pct,
        dock_pct,

        -- Alert flags (Python-computed — staging validates, doesn't recompute)
        is_critically_low_bikes,
        is_critically_low_docks,

        -- Operational status
        is_installed,
        is_renting,
        is_returning,

        -- Timestamps
        station_last_reported_utc,
        feed_last_updated_utc,
        ingested_at_utc

    FROM {{ source('ecobici_raw', 'station_snapshots') }}

    WHERE
        -- Satisfies require_partition_filter on the raw table.
        -- CURRENT_TIMESTAMP() is evaluated at query time by BigQuery.
        ingested_at_utc >= TIMESTAMP_SUB(
            CURRENT_TIMESTAMP(),
            INTERVAL {{ var('lookback_days') }} DAY
        )

        -- Exclude stations that are physically offline or not renting.
        -- These carry no operational signal for the rebalancing model.
        AND is_installed = TRUE
        AND is_renting   = TRUE

),

enriched AS (

    SELECT
        *,

        -- ── Temporal grain derivation ─────────────────────────────────────────
        -- These avoid repeated EXTRACT() calls in every downstream model.
        -- All times in CDMX local time (UTC-6, no DST since 2023 timezone reform).

        DATETIME(ingested_at_utc, 'America/Mexico_City')        AS ingested_at_cdmx,

        EXTRACT(HOUR FROM
            DATETIME(ingested_at_utc, 'America/Mexico_City'))    AS hour_of_day,

        EXTRACT(DAYOFWEEK FROM
            DATETIME(ingested_at_utc, 'America/Mexico_City'))    AS day_of_week,
        -- BQ convention: 1=Sunday, 2=Monday, ..., 7=Saturday

        FORMAT_TIMESTAMP(
            '%Y-%m-%d',
            ingested_at_utc,
            'America/Mexico_City')                               AS snapshot_date,

        -- Rush hour classification (CDMX empirical rush windows)
        CASE
            WHEN EXTRACT(HOUR FROM
                DATETIME(ingested_at_utc, 'America/Mexico_City'))
                    BETWEEN 7  AND 9  THEN 'morning_rush'
            WHEN EXTRACT(HOUR FROM
                DATETIME(ingested_at_utc, 'America/Mexico_City'))
                    BETWEEN 18 AND 20 THEN 'evening_rush'
            ELSE 'off_peak'
        END                                                      AS time_period,

        -- Staleness flag: station hasn't phoned home in > 15 minutes.
        -- Stale readings distort the velocity calculation downstream.
        TIMESTAMP_DIFF(
            ingested_at_utc,
            station_last_reported_utc,
            MINUTE
        ) > 15                                                   AS is_stale_reading

    FROM source

)

SELECT * FROM enriched
