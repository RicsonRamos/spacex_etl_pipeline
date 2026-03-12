

WITH raw_data AS (
    SELECT * FROM "spacex_db"."raw"."spacex_launches"
)

SELECT
    id AS launch_id,
    flight_number,
    name AS launch_name,
    date_utc::timestamp AS launch_at_utc,
    rocket AS rocket_id,
    success::boolean AS is_success,
    details,
    -- Mantendo os arrays para o unnest na camada Gold
    payloads AS payload_ids,
    cores AS core_details,
    ingestion_timestamp AS ingested_at
FROM raw_data