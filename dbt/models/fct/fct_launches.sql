{{ config(materialized='table') }}

WITH launches AS (
    SELECT * FROM {{ ref('stg_launches') }}
),
rockets AS (
    SELECT * FROM {{ ref('dim_rockets') }}
)

SELECT
    l.launch_id,
    l.launch_name,
    l.date_utc,
    l.is_success,
    r.rocket_id,
    r.cost_per_launch,
    CASE 
        WHEN l.is_success = TRUE THEN r.cost_per_launch 
        ELSE 0 
    END AS cost_of_success
FROM launches l
LEFT JOIN rockets r
    ON l.rocket_id = r.rocket_id