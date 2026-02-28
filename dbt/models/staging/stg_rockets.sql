{{ config(materialized='view') }}

SELECT
    rocket_id::text AS rocket_id,
    name AS rocket_name,
    active,
    type,
    stages,
    boosters,
    cost_per_launch,
    success_rate_pct,
    first_flight,
    country,
    company,
    description,
    wikipedia,
    height,
    diameter,
    mass,
    first_stage,
    second_stage,
    engines,
    landing_legs,
    payload_weights,
    flickr_images
FROM {{ source('spacex_raw', 'silver_rockets') }}