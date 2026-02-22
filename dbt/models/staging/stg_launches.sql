{{ config(materialized='view') }}

SELECT
    launch_id::text AS launch_id,
    name AS launch_name,
    date_utc,
    success AS is_success,
    rocket::text AS rocket_id
FROM {{ source('spacex_raw', 'silver_launches') }}