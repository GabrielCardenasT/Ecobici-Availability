{{
    config(
        materialized = 'table',
        description  = 'Latest snapshot with one row per station, worst alert status only.'
    )
}}

WITH

latest_ts AS (
    SELECT MAX(ingested_at_utc) AS max_ts
    FROM {{ ref('int_neighborhood_clusters') }}
),

latest_data AS (
    SELECT c.*
    FROM {{ ref('int_neighborhood_clusters') }} c
    INNER JOIN latest_ts l
        ON c.ingested_at_utc = l.max_ts
),

with_alert_status AS (

    SELECT
        *,
        CASE
            WHEN availability_pct = 0
                THEN '🔴 Vacía'
            WHEN availability_pct < 0.10
                THEN '🟠 Crítica'
            WHEN dock_pct < 0.10
                THEN '🟡 Sin espacio'
            ELSE '🟢 Normal'
        END AS estado_estacion,

        CASE
            WHEN availability_pct = 0      THEN 1
            WHEN availability_pct < 0.10   THEN 2
            WHEN dock_pct < 0.10           THEN 3
            ELSE 4
        END AS alert_rank

    FROM latest_data

)

SELECT * FROM with_alert_status