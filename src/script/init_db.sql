-- 1. EXTENSÕES E LIMPEZA
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 2. CAMADA BRONZE (RAW DATA)
-- Usamos JSONB para performance e uma coluna de auditoria
CREATE TABLE IF NOT EXISTS bronze_launches (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    raw_data JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_rockets (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    raw_data JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. CAMADA SILVER (TRUSTED DATA)
-- Tabelas estruturadas conforme o SCHEMA_REGISTRY
CREATE TABLE IF NOT EXISTS silver_launches (
    launch_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    date_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    success BOOLEAN,
    rocket TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver_rockets (
    rocket_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    active BOOLEAN,
    cost_per_launch BIGINT,
    success_rate_pct FLOAT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. ÍNDICES PARA PERFORMANCE INCREMENTAL
-- Crucial para que o método get_last_ingested do Loader não fique lento com o tempo
CREATE INDEX IF NOT EXISTS idx_launches_date_utc ON silver_launches(date_utc);
