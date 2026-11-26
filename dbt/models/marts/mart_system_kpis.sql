{{
    config(
        materialized = 'table',
        description  = 'System-wide KPIs, one row per snapshot, with the value 24h earlier for trend tiles.'
    )
}}

/*
  mart_system_kpis.sql
  Answers: "How is the whole system doing right now, and compared with
  this time yesterday?"

  Grain: one row per snapshot (ingested_at_utc).

  The *_24h_ago columns take the snapshot closest to 24h earlier
  (±15 min window, polls arrive every 15 min). They are NULL when that
  snapshot does not exist (e.g. the closed window 00:30–05:00 or the
  first day of the lookback), so the dashboard shows "no comparison"
  instead of an invented one.
*/

WITH

per_snapshot AS (

    SELECT
        ingested_at_utc,
        DATETIME(ingested_at_utc, 'America/Mexico_City')            AS ingested_at_cdmx,
        COUNT(DISTINCT station_id)                                  AS stations_reporting,
        SUM(num_bikes_available)                                    AS total_bikes_available,
        SUM(num_docks_available)                                    AS total_docks_available,
        SUM(total_operational_capacity)                             AS total_operational_capacity,
        COUNTIF(num_bikes_available = 0)                            AS empty_stations,
        COUNTIF(num_docks_available = 0)                            AS full_stations,
        COUNTIF(is_critically_low_bikes OR is_critically_low_docks) AS stations_to_rebalance
    FROM {{ ref('int_neighborhood_clusters') }}
    GROUP BY ingested_at_utc

),

with_rates AS (

    SELECT
        *,
        ROUND(SAFE_DIVIDE(total_bikes_available, total_operational_capacity), 4) AS system_availability_pct
    FROM per_snapshot

),

-- Window over epoch seconds: the snapshot that landed 23h45m–24h15m earlier.
with_yesterday AS (

    SELECT
        *,
        LAST_VALUE(system_availability_pct) OVER day_ago AS system_availability_pct_24h_ago,
        LAST_VALUE(empty_stations)          OVER day_ago AS empty_stations_24h_ago,
        LAST_VALUE(full_stations)           OVER day_ago AS full_stations_24h_ago,
        LAST_VALUE(stations_to_rebalance)   OVER day_ago AS stations_to_rebalance_24h_ago
    FROM with_rates
    WINDOW day_ago AS (
        ORDER BY UNIX_SECONDS(ingested_at_utc)
        RANGE BETWEEN 87300 PRECEDING AND 85500 PRECEDING
    )

),

-- Share of finished hours in the last 24h where ingestion was healthy.
-- Closed-system hours count as healthy: the pipeline kept polling.
pipeline_uptime AS (

    SELECT
        ROUND(SAFE_DIVIDE(
            COUNTIF(audit_status IN ('healthy', 'system_closed')),
            COUNTIF(audit_status != 'in_progress')
        ), 4) AS pipeline_uptime_24h
    FROM {{ ref('mart_pipeline_audit') }}
    WHERE audit_hour_start >= DATETIME_SUB(CURRENT_DATETIME('America/Mexico_City'), INTERVAL 24 HOUR)

)

SELECT
    y.*,
    -- Deltas for trend tiles. Availability in percentage points.
    ROUND(y.system_availability_pct - y.system_availability_pct_24h_ago, 4) AS system_availability_delta_24h,
    y.empty_stations        - y.empty_stations_24h_ago                      AS empty_stations_delta_24h,
    y.full_stations         - y.full_stations_24h_ago                       AS full_stations_delta_24h,
    y.stations_to_rebalance - y.stations_to_rebalance_24h_ago               AS stations_to_rebalance_delta_24h,
    p.pipeline_uptime_24h,
    y.ingested_at_utc = MAX(y.ingested_at_utc) OVER ()                      AS is_latest_snapshot
FROM with_yesterday y
CROSS JOIN pipeline_uptime p
