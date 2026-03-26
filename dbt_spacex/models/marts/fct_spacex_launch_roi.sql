--dbt_spacex\models\marts\fct_spacex_launch_roi.sql
{{ config(
    materialized='incremental',
    unique_key='launch_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    schema='gold',
    tags=['incremental', 'daily', 'roi']
) }}

WITH expanded_launches AS (
    SELECT 
        id AS launch_id,
        rocket AS rocket_id,
        jsonb_array_elements_text(payloads::jsonb) AS payload_id,
        date_utc::timestamp AS launch_at_utc
    FROM {{ source('spacex_raw', 'spacex_launches') }}
    
    {% if is_incremental() %}
    WHERE date_utc::timestamp > (SELECT MAX(launch_at_utc) FROM {{ this }})
    {% endif %}
),

launch_metrics AS (
    SELECT 
        el.launch_id,
        el.rocket_id,
        el.launch_at_utc,
        SUM(p.mass_kg) as total_payload_mass_kg
    FROM expanded_launches el
    JOIN {{ ref('stg_spacex__payloads') }} p ON el.payload_id = p.payload_id
    GROUP BY 1, 2, 3
)

SELECT
    m.launch_id,
    r.rocket_name,
    m.total_payload_mass_kg,
    r.cost_per_launch_usd,
    CASE 
        WHEN m.total_payload_mass_kg > 0 THEN r.cost_per_launch_usd / m.total_payload_mass_kg 
        ELSE NULL 
    END as usd_per_kg,
    m.launch_at_utc,
    CURRENT_TIMESTAMP AS processed_at
FROM launch_metrics m
JOIN {{ ref('stg_spacex__rockets') }} r ON m.rocket_id = r.rocket_id