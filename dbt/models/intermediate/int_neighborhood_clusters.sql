{{
    config(
        materialized = 'table',
        description  = 'Assigns each station to a CDMX neighborhood cluster using coordinate-based bounding boxes. Joins to int_station_velocity to produce cluster-level aggregations.'
    )
}}

/*
  int_neighborhood_clusters.sql
  ──────────────────────────────
  Maps every station to one of five CDMX operational zones using lat/lon
  bounding boxes derived from the actual ECOBICI service area.

  Why bounding boxes instead of a seed CSV?
    The station → neighborhood mapping could be a seeds/neighborhoods.csv file,
    but that would require re-seeding whenever ECOBICI opens a new station.
    Coordinate-based assignment is self-updating — any new station automatically
    falls into the right cluster based on its lat/lon.

  Why not PostGIS / ST_Within?
    BigQuery supports GEOGRAPHY functions (ST_WITHIN, ST_CONTAINS) but they
    require polygon definitions. For five rectangular zones, BETWEEN clauses
    are simpler, faster, and don't require geography type casting.

  Bounding box reference (WGS84):
    Determined from the real station_information feed data:
    - Roma/Condesa:    lat [19.40–19.43], lon [-99.19–99.16]
    - Reforma/Polanco: lat [19.42–19.44], lon [-99.22–99.16]
    - Centro Histórico: lat [19.42–19.44], lon [-99.14–99.12]
    - Doctores/Narvarte: lat [19.39–19.42], lon [-99.17–99.14]
    - Tlatelolco/Tepito: lat [19.44–19.46], lon [-99.14–99.12]
    Stations outside all five boxes → "Otra Zona"

  Output: one row per station per snapshot (same grain as int_station_velocity).
*/

WITH

velocity AS (

    SELECT * FROM {{ ref('int_station_velocity') }}

),

with_cluster AS (

    SELECT
        *,

        -- ── Neighborhood cluster assignment ───────────────────────────────────
        CASE
            -- Roma Norte / Condesa (residential drain source during morning rush)
            WHEN lat BETWEEN 19.400 AND 19.430
             AND lon BETWEEN -99.190 AND -99.155
                THEN 'Roma / Condesa'

            -- Reforma / Polanco (corporate arrival zone)
            WHEN lat BETWEEN 19.420 AND 19.445
             AND lon BETWEEN -99.220 AND -99.160
                THEN 'Reforma / Polanco'

            -- Centro Histórico (mixed use — tourist + government)
            WHEN lat BETWEEN 19.425 AND 19.445
             AND lon BETWEEN -99.145 AND -99.120
                THEN 'Centro Histórico'

            -- Doctores / Narvarte (residential south, secondary drain)
            WHEN lat BETWEEN 19.390 AND 19.420
             AND lon BETWEEN -99.170 AND -99.140
                THEN 'Doctores / Narvarte'

            -- Tlatelolco / Tepito (north residential)
            WHEN lat BETWEEN 19.440 AND 19.465
             AND lon BETWEEN -99.145 AND -99.120
                THEN 'Tlatelolco / Tepito'

            -- Stations outside defined clusters (e.g. Satélite expansions)
            ELSE 'Otra Zona'
        END AS neighborhood_cluster,

        -- ── Station role classification ────────────────────────────────────────
        -- Is this station currently acting as a SOURCE (draining) or SINK (filling)?
        -- Used in mart_cluster_health to summarize directional flow per zone.
        CASE
            WHEN depletion_velocity_per_min <= -0.1 THEN 'source'   -- actively losing bikes
            WHEN depletion_velocity_per_min >=  0.1 THEN 'sink'     -- actively gaining bikes
            ELSE 'neutral'
        END AS station_role

    FROM velocity

)

SELECT * FROM with_cluster
