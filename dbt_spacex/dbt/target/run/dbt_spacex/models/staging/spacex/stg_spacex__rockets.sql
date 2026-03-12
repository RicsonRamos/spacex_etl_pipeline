
  create view "spacex_db"."public"."stg_spacex__rockets__dbt_tmp"
    
    
  as (
    

SELECT 
    id AS rocket_id,
    name AS rocket_name,
    type AS rocket_type,
    active::boolean AS is_active,
    cost_per_launch::numeric AS cost_per_launch_usd,
    success_rate_pct::integer AS success_rate_pct,
    
    (payload_weights::jsonb->0->>'kg')::numeric AS max_payload_kg_leo,
    ingestion_timestamp AS ingested_at
FROM "spacex_db"."raw"."spacex_rockets"
  );