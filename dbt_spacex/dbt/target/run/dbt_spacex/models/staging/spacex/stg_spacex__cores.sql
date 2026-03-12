
  create view "spacex_db"."public"."stg_spacex__cores__dbt_tmp"
    
    
  as (
    

SELECT
    id AS core_id,
    serial AS core_serial,
    status AS core_status,
    reuse_count::integer AS reuse_count,
    rtls_landings::integer AS land_landings,
    asds_landings::integer AS sea_landings,
    ingestion_timestamp AS ingested_at
FROM "spacex_db"."raw"."spacex_cores"
  );