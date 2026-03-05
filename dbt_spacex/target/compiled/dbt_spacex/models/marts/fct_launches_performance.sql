

WITH launches AS (
    SELECT * FROM "spacex_db"."public"."stg_launches"
),

rockets AS (
    SELECT * FROM "spacex_db"."public"."stg_rockets"
)

SELECT
    l.mission_id,
    l.mission_name,
    l.launch_date,
    l.is_success,
    r.rocket_name,
    r.rocket_type,
    r.cost_per_launch,
    l.loaded_at AS launch_loaded_at,
    r.loaded_at AS rocket_loaded_at,
    CASE 
        WHEN l.is_success THEN r.cost_per_launch
        ELSE 0
    END AS estimated_loss,
    -- Metadados para analises futuras
    EXTRACT(YEAR FROM l.launch_date) AS launch_year,
    EXTRACT(MONTH FROM l.launch_date) AS launch_month,
    EXTRACT(DAY FROM l.launch_date) AS launch_day,
    EXTRACT(HOUR FROM l.launch_date) AS launch_hour
FROM launches l
left JOIN rockets r ON l.rocket_id = r.rocket_id