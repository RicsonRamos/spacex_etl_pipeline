--dbt_spacex\models\staging\spacex\stg_spacex__cores.sql

{{ config(materialized='view') }}

SELECT
    id AS core_id,
    serial AS core_serial,
    status AS core_status,
    reuse_count::integer AS reuse_count,
    rtls_landings::integer AS land_landings,
    asds_landings::integer AS sea_landings,
    ingestion_timestamp AS ingested_at
FROM {{ source('spacex_raw', 'spacex_cores') }}