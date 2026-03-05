
  create view "spacex_db"."public"."stg_rockets__dbt_tmp"
    
    
  as (
    

SELECT 
    id AS rocket_id,
    name AS rocket_name,
    type AS rocket_type,
    active AS is_active,
    stages,
    cost_per_launch,
    success_rate_pct,
    first_flight,
    country,
    company,
    loaded_at, -- Metadado de timestamp para rtreabilidade
    now() AS last_updated

FROM "spacex_db"."raw"."Spacex_rockets"
  );