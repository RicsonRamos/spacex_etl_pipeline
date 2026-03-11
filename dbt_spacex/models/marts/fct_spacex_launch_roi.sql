{{ config(materialized='table', schema='gold') }}

WITH base_launches AS (
    SELECT 
        l.launch_id,
        l.rocket_id,
        r.cost_per_launch,
        -- Somamos a massa de todos os payloads daquela missão
        SUM(p.mass_kg) as total_payload_mass_kg,
        -- Contamos quantos boosters foram reutilizados
        COUNT(CASE WHEN c.reused = True THEN 1 END) as reused_cores_count
    FROM {{ ref('stg_spacex__launches') }} l
    JOIN {{ ref('stg_spacex__rockets') }} r ON l.rocket_id = r.rocket_id
    LEFT JOIN {{ ref('stg_spacex__payloads') }} p ON l.launch_id = p.launch_id
    LEFT JOIN {{ ref('stg_spacex__cores') }} c ON l.launch_id = c.launch_id
    GROUP BY 1, 2, 3
)

SELECT
    launch_id,
    rocket_id,
    total_payload_mass_kg,
    cost_per_launch,
    -- Métrica Ouro: Custo por KG (Cuidado com divisão por zero)
    CASE 
        WHEN total_payload_mass_kg > 0 THEN cost_per_launch / total_payload_mass_kg 
        ELSE NULL 
    END as usd_per_kg,
    -- Avaliação de eficiência de reuso
    CASE 
        WHEN reused_cores_count > 0 THEN 'High Efficiency'
        ELSE 'Standard'
    END as reuse_tier
FROM base_launches