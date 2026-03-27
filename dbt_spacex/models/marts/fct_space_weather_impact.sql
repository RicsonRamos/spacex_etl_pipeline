-- dbt_spacex\models\marts\fct_space_weather_impact.sql
{{ config(
    materialized='incremental',
    unique_key='surrogate_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    schema='gold',
    tags=['incremental', 'daily', 'space_weather']
) }}

WITH launches AS (
    SELECT 
        launch_id,
        launch_name,
        launch_at_utc,
        is_success
    FROM {{ ref('stg_spacex__launches') }}
    
    {% if is_incremental() %}
    WHERE launch_at_utc > (SELECT MAX(launch_at_utc) FROM {{ this }})
    {% endif %}
),

solar_events AS (
    SELECT 
        activityID,
        event_at_utc, 
        event_description as nasa_note 
    FROM {{ ref('stg_nasa__solar_events') }}
),

impact_analysis AS (
    SELECT 
        l.launch_id || '-' || COALESCE(s.activityID, 'NO_EVENT') AS surrogate_key,
        l.launch_name,
        l.launch_at_utc,
        l.is_success,
        s.activityID as solar_event_id,
        s.event_at_utc as solar_event_at,
        ABS(EXTRACT(EPOCH FROM (l.launch_at_utc - s.event_at_utc))/3600) as hours_diff,
        s.nasa_note,
        CURRENT_TIMESTAMP AS processed_at
    FROM launches l
    LEFT JOIN solar_events s 
        ON s.event_at_utc BETWEEN l.launch_at_utc - INTERVAL '7 days' 
                             AND l.launch_at_utc + INTERVAL '7 days'
)

SELECT * FROM impact_analysis