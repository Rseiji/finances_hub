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

CREATE TABLE IF NOT EXISTS bronze.pdf_nuinvest_trade_events (
    uid UUID PRIMARY KEY,
    source TEXT NOT NULL,
    request_params JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    mercado TEXT NOT NULL,
    cv TEXT NOT NULL,
    tipo_mercado TEXT NOT NULL,
    espec_titulo TEXT NOT NULL,
    observacao TEXT NOT NULL,
    quantidade INTEGER NOT NULL,
    preco NUMERIC(18, 6) NOT NULL,
    valor NUMERIC(18, 6) NOT NULL,
    dc CHAR(1) NOT NULL,
    taxa_liquidacao NUMERIC(18, 6) NOT NULL,
    emolumentos NUMERIC(18, 6) NOT NULL,
    taxa_transf_ativos NUMERIC(18, 6) NOT NULL,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    statement_date DATE NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pdf_nuinvest_trade_events_source ON bronze.pdf_nuinvest_trade_events (source);
CREATE INDEX IF NOT EXISTS idx_pdf_nuinvest_trade_events_file_name ON bronze.pdf_nuinvest_trade_events (file_name);
CREATE INDEX IF NOT EXISTS idx_pdf_nuinvest_trade_events_statement_date ON bronze.pdf_nuinvest_trade_events (statement_date);
CREATE INDEX IF NOT EXISTS idx_pdf_nuinvest_trade_events_fetched_at ON bronze.pdf_nuinvest_trade_events (fetched_at);
