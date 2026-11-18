{{
    config(
        materialized     = 'table',
        partition_by     = {
            'field': 'ingested_at_utc',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by       = ['neighborhood_cluster'],
        on_schema_change = 'append_new_columns',
        description      = 'One row per neighborhood cluster per 5-min snapshot. Aggregates station-level metrics into cluster health scores and flow direction. Primary input for Looker Studio / operations dashboard tiles.'
    )
}}


WITH

clustered AS (

    SELECT * FROM {{ ref('int_neighborhood_clusters') }}

),

aggregated AS (

    SELECT
        neighborhood_cluster,
        ingested_at_utc,
        ingested_at_cdmx,
        snapshot_date,
        time_period,
        hour_of_day,

        -- ── Station counts ────────────────────────────────────────────────────
        COUNT(DISTINCT station_id)                                   AS total_stations,

        COUNTIF(is_critically_low_bikes)                             AS stations_low_bikes,
        COUNTIF(is_critically_low_docks)                             AS stations_low_docks,

        COUNTIF(station_role = 'source')                             AS source_stations,
        COUNTIF(station_role = 'sink')                               AS sink_stations,
        COUNTIF(station_role = 'neutral')                            AS neutral_stations,

        COUNTIF(velocity_status = 'critical_drain')                  AS critical_drain_stations,
        COUNTIF(velocity_status = 'moderate_drain')                  AS moderate_drain_stations,

        -- ── Availability aggregates ───────────────────────────────────────────
        SUM(num_bikes_available)                                     AS total_bikes_available,
        SUM(num_docks_available)                                     AS total_docks_available,
        SUM(total_operational_capacity)                              AS total_operational_capacity,

        ROUND(SAFE_DIVIDE(
            SUM(num_bikes_available),
            SUM(total_operational_capacity)
        ), 4)                                                        AS cluster_availability_pct,

        ROUND(AVG(availability_pct), 4)                             AS avg_station_availability_pct,
        ROUND(MIN(availability_pct), 4)                             AS min_station_availability_pct,

        -- ── Velocity aggregates ───────────────────────────────────────────────
        ROUND(AVG(depletion_velocity_per_min), 4)                   AS avg_depletion_velocity,
        ROUND(SUM(depletion_velocity_per_min), 4)                   AS net_cluster_flow,
        -- net_cluster_flow > 0: zone is net receiving bikes (sink zone)
        -- net_cluster_flow < 0: zone is net losing bikes (source zone)

        ROUND(MIN(est_minutes_to_empty), 1)                         AS min_est_minutes_to_empty,
        -- The most urgent station in the cluster — drives dispatch priority

        -- ── Cluster flow direction ────────────────────────────────────────────
        CASE
            WHEN SUM(depletion_velocity_per_min) <= -1.0 THEN 'strong_outflow'
            WHEN SUM(depletion_velocity_per_min) <= -0.3 THEN 'mild_outflow'
            WHEN SUM(depletion_velocity_per_min) <   0.3
             AND SUM(depletion_velocity_per_min) >  -0.3 THEN 'balanced'
            -- strong_inflow must be checked BEFORE mild_inflow; otherwise every
            -- value >= 1.0 is caught by the >= 0.3 branch and strong_inflow
            -- can never appear.
            WHEN SUM(depletion_velocity_per_min) >=  1.0 THEN 'strong_inflow'
            WHEN SUM(depletion_velocity_per_min) >=  0.3 THEN 'mild_inflow'
            ELSE 'unknown'
        END                                                          AS cluster_flow_direction

    FROM clustered
    GROUP BY
        neighborhood_cluster,
        ingested_at_utc,
        ingested_at_cdmx,
        snapshot_date,
        time_period,
        hour_of_day

),

with_health_score AS (

    SELECT
        *,

        -- ── Cluster health score [0.0 → 1.0] ──────────────────────────────────
        -- Composite score for dashboard traffic-light coloring:
        --   0.0 – 0.3 : red   — cluster needs immediate dispatch
        --   0.3 – 0.6 : amber — cluster degrading, monitor closely
        --   0.6 – 1.0 : green — cluster healthy
        --
        -- Weights (tunable via dbt vars in production):
        --   50% : cluster availability (are there bikes?)
        --   30% : fraction of critically low stations
        --   20% : velocity pressure (how fast is it getting worse?)
        ROUND(
            (0.50 * COALESCE(cluster_availability_pct, 0.5))
            + (0.30 * (1.0 - SAFE_DIVIDE(stations_low_bikes, NULLIF(total_stations, 0))))
            -- Velocity term clamped to [0, 1]: a refilling cluster
            -- (avg velocity > 0) must not push the score above 1.0.
            + (0.20 * LEAST(1.0, GREATEST(0.0, 1.0 + COALESCE(
                SAFE_DIVIDE(avg_depletion_velocity, 1.0),
                0.0
              )))),
            4
        )                                                            AS cluster_health_score,

        -- ── Recommended truck dispatch count ─────────────────────────────────
        -- Rule-of-thumb heuristic: 1 truck per 3 critically low stations.
        -- Operations teams override this based on actual truck availability.
        CAST(
            CEIL(SAFE_DIVIDE(stations_low_bikes + stations_low_docks, 3.0))
        AS INT64)                                                    AS recommended_truck_dispatches,

        -- ── Latest-snapshot flag ─────────────────────────────────────────────
        -- This table holds every snapshot in the lookback window, so a Looker
        -- scorecard with no filter averages ~2 days of history. Filter on
        -- is_latest_snapshot = TRUE to show the state of the system right now.
        ingested_at_utc = MAX(ingested_at_utc) OVER ()               AS is_latest_snapshot,

        -- ── System-wide availability (capacity-weighted) ─────────────────────
        -- Same value on every cluster row of a snapshot. Averaging
        -- cluster_availability_pct across clusters weights a 20-station cluster
        -- the same as a 400-station one; this is total bikes / total capacity.
        ROUND(SAFE_DIVIDE(
            SUM(total_bikes_available)       OVER (PARTITION BY ingested_at_utc),
            SUM(total_operational_capacity)  OVER (PARTITION BY ingested_at_utc)
        ), 4)                                                        AS system_availability_pct

    FROM aggregated

)

SELECT * FROM with_health_score
