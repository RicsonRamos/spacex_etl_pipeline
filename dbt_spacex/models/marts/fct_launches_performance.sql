{{ config(materialized='table') }}

WITH launches AS (
    SELECT * FROM {{ ref('stg_launches') }}
)

SELECT
    mission_id,
    mission_name,
    launch_date,
    is_success,
    -- Como não temos o stg_rockets ainda, removemos o JOIN
    rocket_id
FROM launches