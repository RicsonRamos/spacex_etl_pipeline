-- Teste Singular: Garante que não existam lançamentos registrados 
-- ANTES de eventos solares relacionados no mesmo período.
SELECT
    l.launch_id,
    l.launch_at_utc,
    n.event_at_utc
FROM {{ ref('fct_launches_performance') }} l
JOIN {{ ref('stg_nasa__solar_events') }} n 
    ON l.launch_at_utc::date = n.event_at_utc::date
WHERE l.launch_at_utc < n.event_at_utc