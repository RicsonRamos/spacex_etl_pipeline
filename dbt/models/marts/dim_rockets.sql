{{ config(materialized='table') }}

SELECT
    rocket_id,
    name AS rocket_name,
    active AS is_active,
    cost_per_launch,
    success_rate_pct,
    NOW() AS gold_processed_at
FROM {{ source('spacex_raw', 'silver_rockets') }}
