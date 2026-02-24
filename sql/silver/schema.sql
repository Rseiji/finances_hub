CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.yfinance_prices (
    request_id UUID NOT NULL,
    symbol TEXT NOT NULL,
    asset TEXT NOT NULL,
    currency TEXT NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    price_ts TIMESTAMPTZ NOT NULL,
    price_date DATE NOT NULL,
    series_type TEXT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT yfinance_prices_uniq UNIQUE (symbol, currency, price_date)
);

CREATE INDEX IF NOT EXISTS idx_yfinance_prices_symbol ON silver.yfinance_prices (symbol);
CREATE INDEX IF NOT EXISTS idx_yfinance_prices_asset ON silver.yfinance_prices (asset);
CREATE INDEX IF NOT EXISTS idx_yfinance_prices_price_ts ON silver.yfinance_prices (price_ts);
CREATE INDEX IF NOT EXISTS idx_yfinance_prices_fetched_at ON silver.yfinance_prices (fetched_at);

CREATE TABLE IF NOT EXISTS silver.binance_prices (
    request_id UUID NOT NULL,
    symbol TEXT NOT NULL,
    asset TEXT NOT NULL,
    currency TEXT NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    price_ts TIMESTAMPTZ NOT NULL,
    price_date DATE NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT binance_prices_uniq UNIQUE (symbol, currency, price_date)
);

CREATE INDEX IF NOT EXISTS idx_binance_prices_symbol ON silver.binance_prices (symbol);
CREATE INDEX IF NOT EXISTS idx_binance_prices_asset ON silver.binance_prices (asset);
CREATE INDEX IF NOT EXISTS idx_binance_prices_price_ts ON silver.binance_prices (price_ts);
CREATE INDEX IF NOT EXISTS idx_binance_prices_fetched_at ON silver.binance_prices (fetched_at);


CREATE TABLE IF NOT EXISTS silver.nubank_trade_events (
    event_id TEXT PRIMARY KEY,
    batch_id UUID NOT NULL,
    ticker TEXT NOT NULL,
    quantidade DOUBLE PRECISION NOT NULL,
    preco DOUBLE PRECISION NOT NULL,
    valor DOUBLE PRECISION NOT NULL,
    date DATE NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nubank_trade_events_batch_id ON silver.nubank_trade_events (batch_id);
CREATE INDEX IF NOT EXISTS idx_nubank_trade_events_ticker ON silver.nubank_trade_events (ticker);
CREATE INDEX IF NOT EXISTS idx_nubank_trade_events_date ON silver.nubank_trade_events (date);
CREATE INDEX IF NOT EXISTS idx_nubank_trade_events_fetched_at ON silver.nubank_trade_events (fetched_at);
