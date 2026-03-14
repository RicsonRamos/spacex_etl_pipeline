{{ config(materialized='table', schema='gold') }}

WITH launches AS (
    SELECT 
        launch_id,
        launch_name,
        launch_at_utc,
        is_success
    FROM {{ ref('stg_spacex__launches') }}
),

solar_events AS (
    SELECT 
        activityID,
        event_at_utc, 
        event_description as nasa_note 
    FROM {{ ref('stg_nasa__solar_events') }}
)

SELECT 
    l.launch_name,
    l.launch_at_utc,
    l.is_success,
    s.activityID as solar_event_id,
    s.event_at_utc as solar_event_at,
    -- Cálculo de proximidade em horas
    ABS(EXTRACT(EPOCH FROM (l.launch_at_utc - s.event_at_utc))/3600) as hours_diff
FROM launches l
LEFT JOIN solar_events s 
    ON s.event_at_utc BETWEEN l.launch_at_utc - INTERVAL '7 days' 
                         AND l.launch_at_utc + INTERVAL '7 days'