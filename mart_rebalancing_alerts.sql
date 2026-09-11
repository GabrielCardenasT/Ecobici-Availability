{{
    config(
        materialized       = 'incremental',
        unique_key         = ['station_id', 'ingested_at_utc', 'alert_type'],
        partition_by       = {
            'field': 'ingested_at_utc',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by         = ['neighborhood_cluster', 'alert_severity'],
        on_schema_change   = 'append_new_columns',
        description        = 'One row per station per snapshot where an alert condition is active. The primary output for the operations dashboard and rebalancing crew dispatch.'
    )
}}

/*
  mart_rebalancing_alerts.sql
  ────────────────────────────
  The operational core of the entire pipeline.

  An alert row is generated whenever ANY of these conditions are true:
    1. BIKE_SHORTAGE  : availability_pct < 10% (station running out of bikes)
    2. DOCK_OVERFLOW  : dock_pct < 10% (station running out of docks)
    3. CRITICAL_DRAIN : depletion_velocity <= -0.5 (losing ≥1 bike / 2 min)
    4. IMMINENT_EMPTY : est_minutes_to_empty <= 15 (< 15 min at current rate)

  A single snapshot can generate multiple alert rows (e.g. a station that
  is both at < 10% bikes AND draining at -0.6 bikes/min gets two rows).
  This design makes aggregation simpler: COUNT(*) per alert_type gives
  the exact count of each alert class with no CASE gymnastics.

  Incremental strategy: insert_overwrite on day partition.
    On each dbt run, the current day's partition is fully replaced.
    This is idempotent — re-running never creates duplicate alert rows.

  Operations team consumption:
    SELECT * FROM mart_rebalancing_alerts
    WHERE ingested_at_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
      AND alert_severity IN ('critical', 'high')
    ORDER BY est_minutes_to_empty ASC NULLS LAST
*/

WITH

clustered AS (

    SELECT * FROM {{ ref('int_neighborhood_clusters') }}

    {% if is_incremental() %}
    -- Incremental runs only process today's partition.
    -- Full refresh (`dbt run --full-refresh`) processes all history.
    WHERE ingested_at_utc >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
    {% endif %}

),

-- ── Unpivot alert conditions into individual alert rows ────────────────────
-- Each UNION branch represents one alert type.
-- Using UNION ALL over CROSS JOIN UNNEST for BigQuery cost efficiency
-- (UNNEST on a small array is fine, but explicit UNION is more readable here).

bike_shortage_alerts AS (

    SELECT
        station_id,
        short_name,
        name,
        lat,
        lon,
        neighborhood_cluster,
        num_bikes_available,
        num_docks_available,
        total_operational_capacity,
        availability_pct,
        dock_pct,
        depletion_velocity_per_min,
        dock_fill_velocity_per_min,
        est_minutes_to_empty,
        velocity_status,
        station_role,
        time_period,
        hour_of_day,
        snapshot_date,
        ingested_at_utc,
        ingested_at_cdmx,

        -- Alert metadata
        'BIKE_SHORTAGE'                                              AS alert_type,
        'Station has fewer than 10% bikes available'                AS alert_description,

        CASE
            WHEN availability_pct = 0                               THEN 'critical'
            WHEN availability_pct < 0.05                            THEN 'high'
            ELSE 'medium'
        END                                                          AS alert_severity,

        -- Dispatch priority score (lower = dispatch sooner)
        -- Combines urgency (velocity) with current state (availability)
        -- Formula: stations draining fast + nearly empty get score near 1.0
        ROUND(
            COALESCE(1.0 - availability_pct, 1.0)
            * COALESCE(
                LEAST(1.0, ABS(depletion_velocity_per_min) / 0.5),
                0.5   -- unknown velocity gets half weight
            ),
            4
        )                                                            AS dispatch_priority_score

    FROM clustered
    WHERE is_critically_low_bikes = TRUE

),

dock_overflow_alerts AS (

    SELECT
        station_id,
        short_name,
        name,
        lat,
        lon,
        neighborhood_cluster,
        num_bikes_available,
        num_docks_available,
        total_operational_capacity,
        availability_pct,
        dock_pct,
        depletion_velocity_per_min,
        dock_fill_velocity_per_min,
        est_minutes_to_empty,
        velocity_status,
        station_role,
        time_period,
        hour_of_day,
        snapshot_date,
        ingested_at_utc,
        ingested_at_cdmx,

        'DOCK_OVERFLOW'                                              AS alert_type,
        'Station has fewer than 10% docks available'                AS alert_description,

        CASE
            WHEN dock_pct = 0                                        THEN 'critical'
            WHEN dock_pct < 0.05                                     THEN 'high'
            ELSE 'medium'
        END                                                          AS alert_severity,

        ROUND(
            COALESCE(1.0 - dock_pct, 1.0)
            * COALESCE(
                LEAST(1.0, ABS(dock_fill_velocity_per_min) / 0.5),
                0.5
            ),
            4
        )                                                            AS dispatch_priority_score

    FROM clustered
    WHERE is_critically_low_docks = TRUE

),

critical_drain_alerts AS (

    SELECT
        station_id,
        short_name,
        name,
        lat,
        lon,
        neighborhood_cluster,
        num_bikes_available,
        num_docks_available,
        total_operational_capacity,
        availability_pct,
        dock_pct,
        depletion_velocity_per_min,
        dock_fill_velocity_per_min,
        est_minutes_to_empty,
        velocity_status,
        station_role,
        time_period,
        hour_of_day,
        snapshot_date,
        ingested_at_utc,
        ingested_at_cdmx,

        'CRITICAL_DRAIN'                                             AS alert_type,
        'Station is losing bikes faster than 0.5 bikes/minute'      AS alert_description,

        CASE
            WHEN depletion_velocity_per_min <= -1.0                  THEN 'critical'
            WHEN depletion_velocity_per_min <= -0.7                  THEN 'high'
            ELSE 'medium'
        END                                                          AS alert_severity,

        ROUND(
            LEAST(1.0, ABS(depletion_velocity_per_min) / 1.0)
            * COALESCE(1.0 - availability_pct, 0.5),
            4
        )                                                            AS dispatch_priority_score

    FROM clustered
    WHERE velocity_status = 'critical_drain'

),

imminent_empty_alerts AS (

    SELECT
        station_id,
        short_name,
        name,
        lat,
        lon,
        neighborhood_cluster,
        num_bikes_available,
        num_docks_available,
        total_operational_capacity,
        availability_pct,
        dock_pct,
        depletion_velocity_per_min,
        dock_fill_velocity_per_min,
        est_minutes_to_empty,
        velocity_status,
        station_role,
        time_period,
        hour_of_day,
        snapshot_date,
        ingested_at_utc,
        ingested_at_cdmx,

        'IMMINENT_EMPTY'                                             AS alert_type,
        'Station will run out of bikes within 15 minutes at current rate' AS alert_description,

        CASE
            WHEN est_minutes_to_empty <= 5                           THEN 'critical'
            WHEN est_minutes_to_empty <= 10                          THEN 'high'
            ELSE 'medium'
        END                                                          AS alert_severity,

        -- Priority: stations emptying soonest get dispatched first
        ROUND(
            SAFE_DIVIDE(15.0 - est_minutes_to_empty, 15.0),
            4
        )                                                            AS dispatch_priority_score

    FROM clustered
    WHERE est_minutes_to_empty IS NOT NULL
      AND est_minutes_to_empty <= 15

),

unioned AS (

    SELECT * FROM bike_shortage_alerts
    UNION ALL
    SELECT * FROM dock_overflow_alerts
    UNION ALL
    SELECT * FROM critical_drain_alerts
    UNION ALL
    SELECT * FROM imminent_empty_alerts

),

final AS (

    SELECT
        -- Composite surrogate key for incremental deduplication
        FARM_FINGERPRINT(
            CONCAT(station_id, '|', CAST(ingested_at_utc AS STRING), '|', alert_type)
        )                       AS alert_id,
        *
    FROM unioned

)

SELECT * FROM final
