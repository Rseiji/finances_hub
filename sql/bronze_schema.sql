CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.ingestion_events (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    request_params JSONB NOT NULL,
    request_id UUID NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    run_id UUID NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_events_source ON bronze.ingestion_events (source);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_run_id ON bronze.ingestion_events (run_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_fetched_at ON bronze.ingestion_events (fetched_at);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_payload_gin ON bronze.ingestion_events USING GIN (payload);
