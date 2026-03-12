
  
    

  create  table "spacex_db"."public_gold"."fct_launches_performance__dbt_tmp"
  
  
    as
  
  (
    

WITH stg_launches AS (
    SELECT * FROM "spacex_db"."public"."stg_spacex__launches"
    WHERE is_success = TRUE -- Focamos em missões concluídas para análise de ROI
),

stg_rockets AS (
    SELECT * FROM "spacex_db"."public"."stg_spacex__rockets"
),

stg_payloads AS (
    SELECT * FROM "spacex_db"."public"."stg_spacex__payloads"
),

-- Agregação de massa por lançamento (Explodindo o array de payloads)
payload_aggregation AS (
    SELECT 
        l.launch_id,
        SUM(p.mass_kg) AS total_payload_mass_kg
    FROM stg_launches l
    CROSS JOIN LATERAL jsonb_array_elements_text(l.payload_ids::jsonb) AS p_id
    LEFT JOIN stg_payloads p ON p.payload_id = p_id
    GROUP BY 1
),

-- Identificação de risco solar (CMEs no dia do lançamento ou 24h antes)
solar_risk AS (
    SELECT 
        l.launch_id,
        COUNT(s.activityID) AS solar_events_count,
        MAX(s.speed_km_s) AS max_solar_speed_km_s
    FROM stg_launches l
    LEFT JOIN "spacex_db"."public"."stg_nasa__solar_events" s 
        ON s.event_at_utc BETWEEN (l.launch_at_utc - INTERVAL '24 hours') AND l.launch_at_utc
    GROUP BY 1
),

final_metrics AS (
    SELECT
        l.launch_id,
        l.launch_name,
        l.launch_at_utc,
        r.rocket_name,
        r.cost_per_launch_usd,
        pa.total_payload_mass_kg,
        
        -- KPI 1: Eficiência Financeira (Custo por Kg)
        CASE 
            WHEN pa.total_payload_mass_kg > 0 
            THEN (r.cost_per_launch_usd / pa.total_payload_mass_kg)
            ELSE 0 
        END AS usd_per_kg,

        -- KPI 2: Score de Risco (NASA)
        COALESCE(sr.solar_events_count, 0) AS count_cme_events,
        COALESCE(sr.max_solar_speed_km_s, 0) AS peak_cme_speed,

        -- KPI 3: Margem de Segurança de Ativo
        CASE 
            WHEN sr.max_solar_speed_km_s > 1000 THEN 'HIGH RISK'
            WHEN sr.max_solar_speed_km_s > 500 THEN 'MEDIUM RISK'
            ELSE 'LOW RISK'
        END AS mission_risk_profile

    FROM stg_launches l
    JOIN stg_rockets r ON l.rocket_id = r.rocket_id
    LEFT JOIN payload_aggregation pa ON l.launch_id = pa.launch_id
    LEFT JOIN solar_risk sr ON l.launch_id = sr.launch_id
)

SELECT * FROM final_metrics
  );
  