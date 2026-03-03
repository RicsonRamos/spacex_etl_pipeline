

WITH raw_data AS (
    -- O nome dentro do source() deve bater com o 'name' do arquivo .yml acima
    SELECT 
        id as mission_id,
        name as mission_name,
        CAST(date_utc AS TIMESTAMP) as launch_date,
        success as is_success,
        rocket as rocket_id
    FROM "spacex_db"."raw"."Spacex_launches"
)

SELECT * FROM raw_data