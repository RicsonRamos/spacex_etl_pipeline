
  create view "spacex_db"."public"."stg_launches__dbt_tmp"
    
    
  as (
    

WITH raw_data AS (
    -- O nome dentro do source() deve bater com o 'name' do arquivo .yml acima
    SELECT 
        id AS mission_id,
        name AS mission_name,
        CAST(date_utc AS TIMESTAMP) AS launch_date,
        success AS is_success,
        rocket AS rocket_id,
        loaded_at, -- Metadado de timestamp para rtreabilidade
        now() AS last_updated
    FROM "spacex_db"."raw"."Spacex_launches"
)

SELECT * FROM raw_data
  );