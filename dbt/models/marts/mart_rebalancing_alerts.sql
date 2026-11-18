{{
    config(
        materialized       = 'table',
        partition_by       = {
            'field': 'ingested_at_utc',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by         = ['neighborhood_cluster', 'alert_severity'],
        on_schema_change   = 'append_new_columns',
        description        = 'One row per station per alert type for the latest snapshot where an alert condition is active. The primary output for the operations dashboard and rebalancing crew dispatch.'
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
                        and at least 1 bike left (empty = BIKE_SHORTAGE)
    4. IMMINENT_EMPTY : est_minutes_to_empty <= 15 (< 15 min at current rate)
                        and at least 1 bike left (empty = BIKE_SHORTAGE)

  A single snapshot can generate multiple alert rows (e.g. a station that
  is both at < 10% bikes AND draining at -0.6 bikes/min gets two rows).
  This design makes aggregation simpler: COUNT(*) per alert_type gives
  the exact count of each alert class with no CASE gymnastics.

  The model processes ONLY the latest available snapshot from
  int_neighborhood_clusters.

  Operations team consumption:
    SELECT * FROM mart_rebalancing_alerts
    WHERE ingested_at_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
      AND alert_severity IN ('critical', 'high')
    ORDER BY est_minutes_to_empty ASC NULLS LAST
*/

WITH

latest_snapshot AS (
    SELECT MAX(ingested_at_utc) AS max_ts
    FROM {{ ref('int_neighborhood_clusters') }}
),

clustered AS (
    SELECT c.*
    FROM {{ ref('int_neighborhood_clusters') }} c
    CROSS JOIN latest_snapshot l
    WHERE c.ingested_at_utc = l.max_ts
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
        'BIKE_SHORTAGE' AS alert_type,
        'Station has fewer than 10% bikes available' AS alert_description,

        CASE
            WHEN availability_pct = 0 THEN 'critical'
            WHEN availability_pct < 0.05 THEN 'high'
            ELSE 'medium'
        END AS alert_severity,

        -- Dispatch priority score [0, 1] — HIGHER = dispatch sooner.
        --   * An empty station is already failing riders → always 1.0.
        --   * Otherwise: state severity (1 - availability) scaled by drain
        --     pressure. A stable station keeps 50% of its severity; one
        --     draining at >= 0.5 bikes/min keeps 100%.
        --   * Only DRAINING counts (GREATEST(0, -v)). The old ABS() gave
        --     refilling stations high urgency, and a stable-empty station
        --     (velocity = 0.0, not NULL) got COALESCE(0.0, 0.5) = 0 → score 0.
        CASE
            WHEN num_bikes_available = 0 THEN 1.0
            ELSE ROUND(
                COALESCE(1.0 - availability_pct, 1.0)
                * (0.5 + 0.5 * COALESCE(
                    LEAST(1.0, GREATEST(0.0, -depletion_velocity_per_min) / 0.5),
                    0.0
                )),
                4
            )
        END AS dispatch_priority_score

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

        'DOCK_OVERFLOW' AS alert_type,
        'Station has fewer than 10% docks available' AS alert_description,

        CASE
            WHEN dock_pct = 0 THEN 'critical'
            WHEN dock_pct < 0.05 THEN 'high'
            ELSE 'medium'
        END AS alert_severity,

        -- Same shape as BIKE_SHORTAGE, mirrored for docks.
        -- dock_fill_velocity_per_min < 0 means free docks are DISAPPEARING
        -- (bikes arriving), so urgency comes from GREATEST(0, -v).
        CASE
            WHEN num_docks_available = 0 THEN 1.0
            ELSE ROUND(
                COALESCE(1.0 - dock_pct, 1.0)
                * (0.5 + 0.5 * COALESCE(
                    LEAST(1.0, GREATEST(0.0, -dock_fill_velocity_per_min) / 0.5),
                    0.0
                )),
                4
            )
        END AS dispatch_priority_score

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

        'CRITICAL_DRAIN' AS alert_type,
        'Station is losing bikes faster than 0.5 bikes/minute' AS alert_description,

        CASE
            WHEN depletion_velocity_per_min <= -1.0 THEN 'critical'
            WHEN depletion_velocity_per_min <= -0.7 THEN 'high'
            ELSE 'medium'
        END AS alert_severity,

        ROUND(
            LEAST(1.0, ABS(depletion_velocity_per_min) / 1.0)
            * COALESCE(1.0 - availability_pct, 0.5),
            4
        ) AS dispatch_priority_score

    FROM clustered
    WHERE velocity_status = 'critical_drain'
      -- A station that has already hit 0 cannot keep draining; it is covered
      -- by BIKE_SHORTAGE. Without this guard the bikes lost on the way to 0
      -- produced a second, duplicate alert for the same empty station.
      AND num_bikes_available > 0

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

        'IMMINENT_EMPTY' AS alert_type,
        'Station will run out of bikes within 15 minutes at current rate'
            AS alert_description,

        CASE
            WHEN est_minutes_to_empty <= 5 THEN 'critical'
            WHEN est_minutes_to_empty <= 10 THEN 'high'
            ELSE 'medium'
        END AS alert_severity,

        -- Priority: stations emptying soonest get dispatched first
        ROUND(
            SAFE_DIVIDE(15.0 - est_minutes_to_empty, 15.0),
            4
        ) AS dispatch_priority_score

    FROM clustered
    WHERE est_minutes_to_empty IS NOT NULL
      AND est_minutes_to_empty <= 15
      -- "About to empty" only makes sense while bikes remain. An already-empty
      -- station is covered by BIKE_SHORTAGE; without this guard it raised a
      -- second, duplicate alert (est_minutes_to_empty = 0).
      AND num_bikes_available > 0

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
        -- Composite surrogate key
        FARM_FINGERPRINT(
            CONCAT(
                station_id,
                '|',
                CAST(ingested_at_utc AS STRING),
                '|',
                alert_type
            )
        ) AS alert_id,

        -- Human-readable label for dashboards. alert_type stays the stable
        -- machine key (tests, filters); BI tools display alert_label.
        CASE alert_type
            WHEN 'BIKE_SHORTAGE'  THEN 'Bike shortage'
            WHEN 'DOCK_OVERFLOW'  THEN 'Dock overflow'
            WHEN 'CRITICAL_DRAIN' THEN 'Critical drain'
            WHEN 'IMMINENT_EMPTY' THEN 'Imminent empty'
        END AS alert_label,

        *

    FROM unioned

)

SELECT * FROM final
