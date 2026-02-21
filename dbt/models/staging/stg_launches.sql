WITH source AS (
    SELECT * FROM {{ source('spacex_raw', 'silver_launches') }}
)

SELECT
    launch_id,
    name AS launch_name,
    date_utc,
    COALESCE(success, FALSE) AS is_success, -- Trata nulos como falha por padrão de negócio
    rocket AS rocket_id,
    NOW() AS stg_updated_at
FROM source
