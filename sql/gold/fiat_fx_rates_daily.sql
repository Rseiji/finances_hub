INSERT INTO gold.fiat_fx_rates_daily (
    asset,
    base_currency,
    quote_currency,
    currency,
    rate,
    rate_date,
    rate_ts,
    source,
    silver_request_id
)
SELECT
    replace(s.symbol, '=X', '') AS asset,
    left(replace(s.symbol, '=X', ''), 3) AS base_currency,
    right(replace(s.symbol, '=X', ''), 3) AS quote_currency,
    right(replace(s.symbol, '=X', ''), 3) AS currency,
    s.price AS rate,
    s.price_date AS rate_date,
    s.price_ts AS rate_ts,
    'yfinance' AS source,
    s.request_id AS silver_request_id
FROM silver.yfinance_prices AS s
WHERE s.symbol LIKE '%=X'
  AND length(replace(s.symbol, '=X', '')) = 6
ON CONFLICT (asset, currency, rate_date)
DO UPDATE SET
    rate = EXCLUDED.rate,
    rate_ts = EXCLUDED.rate_ts,
    source = EXCLUDED.source,
    silver_request_id = EXCLUDED.silver_request_id,
    ingested_at = NOW();
