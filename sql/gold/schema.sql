CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fiat_fx_rates_daily (
    pair_code TEXT NOT NULL,
    base_currency TEXT NOT NULL,
    quote_currency TEXT NOT NULL,
    rate NUMERIC(18, 8) NOT NULL,
    rate_date DATE NOT NULL,
    rate_ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    silver_request_id UUID NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fiat_fx_rates_daily_uniq UNIQUE (pair_code, rate_date)
);

CREATE INDEX IF NOT EXISTS idx_fiat_fx_rates_daily_pair ON gold.fiat_fx_rates_daily (pair_code);
CREATE INDEX IF NOT EXISTS idx_fiat_fx_rates_daily_date ON gold.fiat_fx_rates_daily (rate_date);

CREATE TABLE IF NOT EXISTS gold.crypto_prices_daily (
    asset TEXT NOT NULL,
    currency TEXT NOT NULL,
    price NUMERIC(38, 18) NOT NULL,
    price_date DATE NOT NULL,
    price_ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    silver_request_id UUID NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT crypto_prices_daily_uniq UNIQUE (asset, currency, price_date)
);

CREATE INDEX IF NOT EXISTS idx_crypto_prices_daily_asset ON gold.crypto_prices_daily (asset);
CREATE INDEX IF NOT EXISTS idx_crypto_prices_daily_date ON gold.crypto_prices_daily (price_date);

CREATE TABLE IF NOT EXISTS gold.br_stocks_daily (
    symbol TEXT NOT NULL,
    asset TEXT NOT NULL,
    currency TEXT NOT NULL,
    price NUMERIC(18, 6) NOT NULL,
    price_date DATE NOT NULL,
    price_ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    silver_request_id UUID NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT br_stocks_daily_uniq UNIQUE (symbol, price_date)
);

CREATE INDEX IF NOT EXISTS idx_br_stocks_daily_symbol ON gold.br_stocks_daily (symbol);
CREATE INDEX IF NOT EXISTS idx_br_stocks_daily_asset ON gold.br_stocks_daily (asset);
CREATE INDEX IF NOT EXISTS idx_br_stocks_daily_date ON gold.br_stocks_daily (price_date);

CREATE TABLE IF NOT EXISTS gold.indices_daily (
    index_code TEXT NOT NULL,
    asset TEXT NOT NULL,
    currency TEXT NOT NULL,
    price NUMERIC(18, 6) NOT NULL,
    price_date DATE NOT NULL,
    price_ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    silver_request_id UUID NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT indices_daily_uniq UNIQUE (index_code, price_date)
);

CREATE INDEX IF NOT EXISTS idx_indices_daily_code ON gold.indices_daily (index_code);
CREATE INDEX IF NOT EXISTS idx_indices_daily_asset ON gold.indices_daily (asset);
CREATE INDEX IF NOT EXISTS idx_indices_daily_date ON gold.indices_daily (price_date);

CREATE TABLE IF NOT EXISTS gold.etfs_daily (
    symbol TEXT NOT NULL,
    asset TEXT NOT NULL,
    currency TEXT NOT NULL,
    price NUMERIC(18, 6) NOT NULL,
    price_date DATE NOT NULL,
    price_ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    silver_request_id UUID NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT etfs_daily_uniq UNIQUE (symbol, price_date)
);

CREATE INDEX IF NOT EXISTS idx_etfs_daily_symbol ON gold.etfs_daily (symbol);
CREATE INDEX IF NOT EXISTS idx_etfs_daily_asset ON gold.etfs_daily (asset);
CREATE INDEX IF NOT EXISTS idx_etfs_daily_date ON gold.etfs_daily (price_date);
