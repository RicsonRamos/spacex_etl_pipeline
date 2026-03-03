

WITH launches AS (
    SELECT * FROM "spacex_db"."public"."stg_launches"
)

SELECT
    mission_id,
    mission_name,
    launch_date,
    is_success,
    -- Como não temos o stg_rockets ainda, removemos o JOIN
    rocket_id
FROM launches