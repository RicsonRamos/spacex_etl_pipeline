-- Um lançamento só pode ter um match se a data for coerente
-- Este teste retorna linhas que FALHAM na lógica
SELECT *
FROM {{ ref('fct_launches_performance') }}
WHERE launch_date_utc < event_at_utc