
-- dbt_spacex\models\staging\spacex\stg_spacex__payloads.sql
{{ config(materialized='view') }}

SELECT
    id AS payload_id,
    name AS payload_name,
    type AS payload_type,
    reused::boolean AS is_payload_reused,
    mass_kg::numeric AS mass_kg,
    orbit AS orbit_code,
    
    (customers::jsonb->>0)::varchar AS primary_customer,
    ingestion_timestamp AS ingested_at
FROM {{ source('spacex_raw', 'spacex_payloads') }}