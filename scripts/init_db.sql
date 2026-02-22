-- 1. DROPS
DROP MATERIALIZED VIEW IF EXISTS gold_rocket_performance;
DROP TABLE IF EXISTS silver_launches;
DROP TABLE IF EXISTS silver_rockets;
DROP TABLE IF EXISTS bronze_launches;
DROP TABLE IF EXISTS bronze_rockets;

SET TIMEZONE = 'UTC';

-- 2. BRONZE
CREATE TABLE bronze_rockets (
    source TEXT NOT NULL,
    raw_data JSONB NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE bronze_launches (
    source TEXT NOT NULL,
    raw_data JSONB NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. SILVER
CREATE TABLE silver_rockets (
    rocket_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    active BOOLEAN,
    cost_per_launch BIGINT,
    success_rate_pct DOUBLE PRECISION,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE silver_launches (
    launch_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    date_utc TIMESTAMPTZ NOT NULL,
    rocket TEXT REFERENCES silver_rockets(rocket_id),
    success BOOLEAN,
    flight_number INTEGER,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. INDEXES
CREATE INDEX idx_silver_launches_date ON silver_launches(date_utc);
CREATE INDEX idx_silver_launches_rocket ON silver_launches(rocket);

-- 5. GOLD
CREATE MATERIALIZED VIEW gold_rocket_performance AS
SELECT 
    r.name AS rocket_name,
    COUNT(l.launch_id) AS total_launches,
    ROUND(
        AVG(CASE WHEN l.success = TRUE THEN 1.0 ELSE 0.0 END)::numeric * 100,
        2
    ) AS success_rate_pct
FROM silver_rockets r
LEFT JOIN silver_launches l ON r.rocket_id = l.rocket
GROUP BY r.name;