-- dbt_spacex\models\staging\nasa\stg_nasa__solar_events.sql

{{ config(materialized='view') }}

WITH raw_nasa AS (
    SELECT * FROM {{ source('nasa_raw', 'nasa_solar_events') }}
), -- <--- Esta vírgula é obrigatória para separar as CTEs

flattened AS (
    SELECT
        "activityID" AS activityID,
        catalog AS catalog_source,
        "startTime"::timestamp AS event_at_utc,
        
        ("cmeAnalyses"::jsonb->0->>'speed')::numeric AS speed_km_s,
        ("cmeAnalyses"::jsonb->0->>'type')::varchar AS cme_type,
        ("cmeAnalyses"::jsonb->0->>'halfAngle')::numeric AS half_angle,
        ("cmeAnalyses"::jsonb->0->>'isMostAccurate')::boolean AS is_most_accurate,
        
        "sourceLocation" AS source_location,
        note AS event_description,
        ingestion_timestamp AS ingested_at
    FROM raw_nasa
) 

SELECT * FROM flattened
WHERE speed_km_s IS NOT NULL