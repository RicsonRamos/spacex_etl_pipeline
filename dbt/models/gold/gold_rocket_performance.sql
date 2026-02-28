{{ config(materialized='view') }}

WITH launches AS (
    SELECT * FROM {{ ref('fct_launches') }}
),
rockets AS (
    SELECT * FROM {{ ref('dim_rockets') }}
)

SELECT
    r.rocket_id,
    r.rocket_name,
    COUNT(l.launches_id) AS total_launches,
    COUNT(CASE WHEN l.is_success = TRUE THEN 1 END) AS successful_launches,
    SUM(l.cost_per_launch) AS total_cost,
    SUM(l.cost_of_success) AS total_cost_success,
    ROUND(
        CASE WHEN COUNT(l.launches_id) > 0 
            THEN COUNT(CASE WHEN l.is_success = TRUE THEN 1 END)::numeric / COUNT(l.launches_id)
            ELSE 0
        END, 2
    ) AS success_rate
FROM rockets r
LEFT JOIN launches l
    ON r.rocket_id = l.rocket_id
GROUP BY r.rocket_id, r.rocket_name
ORDER BY total_launches DESC