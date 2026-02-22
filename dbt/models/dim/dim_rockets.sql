{{ config(materialized='table') }}

SELECT DISTINCT
    rocket_id,
    rocket_name,
    cost_per_launch
FROM {{ ref('stg_rockets') }}