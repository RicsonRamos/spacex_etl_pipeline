{{ config(materialized='table') }}

WITH launches AS (
    SELECT * FROM {{ source('spacex_raw', 'silver_launches') }}
),
rockets AS (
    SELECT * FROM {{ ref('dim_rockets') }} -- Referência interna do dbt
)

SELECT
    l.launch_id,
    l.name AS launch_name,
    l.date_utc,
    l.success,
    r.rocket_name,
    r.cost_per_launch,
    -- Exemplo de lógica de negócio: custo por sucesso
    CASE 
        WHEN l.success = TRUE THEN r.cost_per_launch 
        ELSE 0 
    END AS cost_of_success
FROM launches l
LEFT JOIN rockets r ON l.rocket = r.rocket_id
