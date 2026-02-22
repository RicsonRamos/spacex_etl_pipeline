{{ config(materialized='view') }}

SELECT
    rocket_id::text AS rocket_id,
    name AS rocket_name,
    active,
    cost_per_launch,
    success_rate_pct / 100.0 AS success_rate_decimal
FROM {{ source('spacex_raw', 'silver_rockets') }}