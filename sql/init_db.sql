
-- Table fx_raw_ingestion sert à stocker les données brutes reçues de l'API de change, 
-- y compris les devises demandées, la date de l'API et le payload JSON complet. 
-- Cela permet de conserver une trace complète des données d'ingestion pour référence future ou pour des audits.
CREATE TABLE IF NOT EXISTS fx_raw_ingestion (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(250) NOT NULL,
    base_currency VARCHAR(10) NOT NULL,
    symbols_requested TEXT NOT NULL,
    api_date DATE NOT NULL,
    payload_json JSONB NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table fx_rates stocke les taux de change validés et transformés, avec des contraintes d'unicité pour éviter les doublons.
-- Elle inclut également des index sur la date pour améliorer les performances des requêtes basées sur la date.
CREATE TABLE IF NOT EXISTS fx_rates (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(250) NOT NULL,
    rate_date DATE NOT NULL,
    base_currency VARCHAR(10) NOT NULL,
    quote_currency VARCHAR(10) NOT NULL,
    currency_pair VARCHAR(30) NOT NULL,
    exchange_rate NUMERIC(18, 8) NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fx_rates_unique_pair_date
        UNIQUE (rate_date, base_currency, quote_currency)
);

-- Index pour accélérer les requêtes basées sur la date de taux de change
CREATE INDEX IF NOT EXISTS idx_fx_rates_rate_date
    ON fx_rates (rate_date);

-- Table fx_rejections enregistre les données rejetées lors de la validation, avec des raisons de rejet détaillées pour chaque enregistrement.
CREATE TABLE IF NOT EXISTS fx_rejections (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(250) NOT NULL,
    rate_date DATE,
    base_currency VARCHAR(10),
    quote_currency VARCHAR(10),
    currency_pair VARCHAR(30),
    raw_rate TEXT,
    rejection_reason TEXT NOT NULL,
    rejected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table fx_alerts stocke les alertes générées lorsque les taux de change dépassent les seuils définis, 
-- avec des détails sur les taux précédents et actuels, ainsi que les deltas de taux.
CREATE TABLE IF NOT EXISTS fx_alerts (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(250) NOT NULL,
    rate_date DATE NOT NULL,
    currency_pair VARCHAR(30) NOT NULL,
    previous_rate NUMERIC(18, 8) NOT NULL,
    current_rate NUMERIC(18, 8) NOT NULL,
    rate_delta NUMERIC(18, 8) NOT NULL,
    threshold_used NUMERIC(18, 8) NOT NULL,
    alert_created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table fx_run_logs enregistre les logs d'exécution de chaque pipeline, y compris les statuts, les compteurs de lignes traitées, 
-- et les messages d'erreur ou d'information.
CREATE TABLE IF NOT EXISTS fx_run_logs (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(250) NOT NULL,
    pipeline_name VARCHAR(150) NOT NULL,
    status VARCHAR(50) NOT NULL,
    rows_received INTEGER NOT NULL DEFAULT 0,
    rows_valid INTEGER NOT NULL DEFAULT 0,
    rows_rejected INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    alert_count INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP VIEW IF EXISTS vw_fx_latest_rates CASCADE;
CREATE VIEW vw_fx_latest_rates AS
SELECT DISTINCT ON (currency_pair)
    run_id,
    rate_date,
    base_currency,
    quote_currency,
    currency_pair,
    exchange_rate,
    ingested_at
FROM fx_rates
ORDER BY currency_pair, rate_date DESC, ingested_at DESC;

DROP VIEW IF EXISTS vw_fx_alerts_summary CASCADE;
CREATE VIEW vw_fx_alerts_summary AS
SELECT
    rate_date,
    currency_pair,
    ROUND(MAX(threshold_used) * 100, 4) AS threshold_used_pct,
    COUNT(*) AS alert_count,
    ROUND(MAX(rate_delta) * 100, 4) AS max_rate_delta_pct
FROM fx_alerts
GROUP BY rate_date, currency_pair
ORDER BY rate_date DESC, currency_pair;

DROP VIEW IF EXISTS vw_fx_run_quality_summary CASCADE;
CREATE VIEW vw_fx_run_quality_summary AS
SELECT
    created_at,
    run_id,
    pipeline_name,
    status,
    rows_received,
    rows_valid,
    rows_rejected,
    rows_inserted,
    alert_count,
    message
FROM fx_run_logs
ORDER BY created_at DESC;

DROP VIEW IF EXISTS vw_fx_rate_variation_pct CASCADE;
CREATE VIEW vw_fx_rate_variation_pct AS
WITH ranked_rates AS (
    SELECT
        rate_date,
        currency_pair,
        exchange_rate,
        LAG(exchange_rate) OVER (
            PARTITION BY currency_pair
            ORDER BY rate_date
        ) AS previous_rate
    FROM fx_rates
)
SELECT
    rate_date,
    currency_pair,
    exchange_rate,
    previous_rate,
    CASE
        WHEN previous_rate IS NULL OR previous_rate = 0 THEN NULL
        ELSE ROUND(((exchange_rate - previous_rate) / previous_rate) * 100, 4)
    END AS variation_pct
FROM ranked_rates
ORDER BY rate_date, currency_pair;

DROP VIEW IF EXISTS vw_fx_alert_count_by_currency CASCADE;
CREATE VIEW vw_fx_alert_count_by_currency AS
SELECT
    SPLIT_PART(currency_pair, '/', 2) AS quote_currency,
    COUNT(*) AS alert_count
FROM fx_alerts
GROUP BY SPLIT_PART(currency_pair, '/', 2)
ORDER BY alert_count DESC, quote_currency;

DROP VIEW IF EXISTS vw_fx_alert_avg_intensity CASCADE;
CREATE VIEW vw_fx_alert_avg_intensity AS
SELECT
    ROUND(AVG(rate_delta) * 100, 4) AS avg_alert_intensity_pct
FROM fx_alerts;

DROP VIEW IF EXISTS vw_fx_latest_alerts CASCADE;
CREATE VIEW vw_fx_latest_alerts AS
SELECT
    id,
    alert_created_at,
    run_id,
    rate_date,
    currency_pair,
    SPLIT_PART(currency_pair, '/', 1) AS base_currency,
    SPLIT_PART(currency_pair, '/', 2) AS quote_currency,
    previous_rate,
    current_rate,
    ROUND(rate_delta * 100, 4) AS rate_delta_pct,
    ROUND(threshold_used * 100, 4) AS threshold_used_pct
FROM fx_alerts
ORDER BY alert_created_at DESC;

DROP VIEW IF EXISTS vw_fx_rejections_by_reason CASCADE;
CREATE VIEW vw_fx_rejections_by_reason AS
SELECT
    rejection_reason,
    COUNT(*) AS rejection_count
FROM fx_rejections
GROUP BY rejection_reason
ORDER BY rejection_count DESC, rejection_reason;

DROP VIEW IF EXISTS vw_fx_latest_rejections CASCADE;
CREATE VIEW vw_fx_latest_rejections AS
SELECT
    rejected_at,
    run_id,
    rate_date,
    base_currency,
    quote_currency,
    currency_pair,
    raw_rate,
    rejection_reason
FROM fx_rejections
ORDER BY rejected_at DESC;

DROP VIEW IF EXISTS vw_fx_run_status_counts CASCADE;
CREATE VIEW vw_fx_run_status_counts AS
SELECT
    status,
    COUNT(*) AS run_count
FROM fx_run_logs
GROUP BY status
ORDER BY run_count DESC, status;

DROP VIEW IF EXISTS vw_fx_run_totals CASCADE;
CREATE VIEW vw_fx_run_totals AS
SELECT
    COUNT(*) AS total_runs,
    COUNT(*) FILTER (WHERE status = 'success') AS success_runs,
    COUNT(*) FILTER (WHERE status = 'quality_rejections_logged') AS failed_quality_runs
FROM fx_run_logs;
