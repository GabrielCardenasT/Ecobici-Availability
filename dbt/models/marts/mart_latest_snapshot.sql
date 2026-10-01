{{
    config(
        materialized = 'table',
        description  = 'Always contains only the single most recent ingestion snapshot. Used by Looker Studio to show current live state.'
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
)

SELECT * FROM latest_data