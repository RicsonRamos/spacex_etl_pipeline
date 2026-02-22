-- models/gold/gold_rocket_performance.sql
WITH silver AS (
    SELECT * FROM silver_launches l
    JOIN silver_rockets r USING (rocket_id)
)
SELECT
    r.name AS rocket_name,
    COUNT(l.launch_id) AS total_launches,
    AVG(CAST(l.success AS INT)) * 100 AS success_rate
FROM silver
GROUP BY r.name