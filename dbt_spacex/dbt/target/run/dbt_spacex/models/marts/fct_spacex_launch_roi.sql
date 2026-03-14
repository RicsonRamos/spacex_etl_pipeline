
  
    

  create  table "spacex_db"."public_gold"."fct_spacex_launch_roi__dbt_tmp"
  
  
    as
  
  (
    

WITH expanded_launches AS (
    -- Explodindo a lista de payloads para conseguir fazer o JOIN
    SELECT 
        id AS launch_id,
        rocket AS rocket_id,
        jsonb_array_elements_text(payloads::jsonb) AS payload_id
    FROM "spacex_db"."raw"."spacex_launches"
),

launch_metrics AS (
    SELECT 
        el.launch_id,
        el.rocket_id,
        SUM(p.mass_kg) as total_payload_mass_kg
    FROM expanded_launches el
    JOIN "spacex_db"."public"."stg_spacex__payloads" p ON el.payload_id = p.payload_id
    GROUP BY 1, 2
)

SELECT
    m.launch_id,
    r.rocket_name,
    m.total_payload_mass_kg,
    r.cost_per_launch_usd,
    -- Cálculo Ouro: USD por KG
    CASE 
        WHEN m.total_payload_mass_kg > 0 THEN r.cost_per_launch_usd / m.total_payload_mass_kg 
        ELSE NULL 
    END as usd_per_kg
FROM launch_metrics m
JOIN "spacex_db"."public"."stg_spacex__rockets" r ON m.rocket_id = r.rocket_id
  );
  