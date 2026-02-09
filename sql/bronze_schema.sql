CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.ingestion_events (
    uid UUID PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    request_params JSONB NOT NULL,
    asset TEXT NOT NULL,
    currency TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_events_source ON bronze.ingestion_events (source);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_asset ON bronze.ingestion_events (asset);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_currency ON bronze.ingestion_events (currency);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_uid ON bronze.ingestion_events (uid);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_fetched_at ON bronze.ingestion_events (fetched_at);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_payload_gin ON bronze.ingestion_events USING GIN (payload);
