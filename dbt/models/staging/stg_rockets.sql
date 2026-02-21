WITH source AS (
    SELECT * FROM {{ source('spacex_raw', 'silver_rockets') }}
)

SELECT
    rocket_id,
    name AS rocket_name,
    active AS is_active,
    CAST(cost_per_launch AS BIGINT) AS cost_per_launch,
    success_rate_pct / 100 AS success_rate_decimal, -- Converte 98 para 0.98
    NOW() AS stg_updated_at
FROM source
