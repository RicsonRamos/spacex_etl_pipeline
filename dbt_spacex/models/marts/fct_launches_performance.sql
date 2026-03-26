-- dbt_spacex\models\marts\fct_launches_performance.sql
{{ config(
    materialized='incremental',
    unique_key='launch_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    schema='gold',
    tags=['incremental', 'daily', 'performance']
) }}

WITH stg_launches AS (
    SELECT * 
    FROM {{ ref('stg_spacex__launches') }}
    WHERE is_success = TRUE
    
    {% if is_incremental() %}
    AND launch_at_utc > (SELECT MAX(launch_at_utc) FROM {{ this }})
    {% endif %}
),

stg_rockets AS (
    SELECT * FROM {{ ref('stg_spacex__rockets') }}
),

stg_payloads AS (
    SELECT * FROM {{ ref('stg_spacex__payloads') }}
),

payload_aggregation AS (
    SELECT 
        l.launch_id,
        SUM(p.mass_kg) AS total_payload_mass_kg
    FROM stg_launches l
    CROSS JOIN LATERAL jsonb_array_elements_text(l.payload_ids::jsonb) AS p_id
    LEFT JOIN stg_payloads p ON p.payload_id = p_id
    GROUP BY 1
),

solar_risk AS (
    SELECT 
        l.launch_id,
        COUNT(s.activityID) AS solar_events_count,
        MAX(s.speed_km_s) AS max_solar_speed_km_s
    FROM stg_launches l
    LEFT JOIN {{ ref('stg_nasa__solar_events') }} s 
        ON s.event_at_utc BETWEEN (l.launch_at_utc - INTERVAL '24 hours') AND l.launch_at_utc
    GROUP BY 1
),

final_metrics AS (
    SELECT
        l.launch_id,
        l.launch_name,
        l.launch_at_utc,
        r.rocket_name,
        r.cost_per_launch_usd,
        pa.total_payload_mass_kg,
        
        CASE 
            WHEN pa.total_payload_mass_kg > 0 
            THEN (r.cost_per_launch_usd / pa.total_payload_mass_kg)
            ELSE 0 
        END AS usd_per_kg,

        COALESCE(sr.solar_events_count, 0) AS count_cme_events,
        COALESCE(sr.max_solar_speed_km_s, 0) AS peak_cme_speed,

        CASE 
            WHEN sr.max_solar_speed_km_s > 1000 THEN 'HIGH RISK'
            WHEN sr.max_solar_speed_km_s > 500 THEN 'MEDIUM RISK'
            ELSE 'LOW RISK'
        END AS mission_risk_profile,

        CURRENT_TIMESTAMP AS processed_at

    FROM stg_launches l
    JOIN stg_rockets r ON l.rocket_id = r.rocket_id
    LEFT JOIN payload_aggregation pa ON l.launch_id = pa.launch_id
    LEFT JOIN solar_risk sr ON l.launch_id = sr.launch_id
)

SELECT * FROM final_metrics