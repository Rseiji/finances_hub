INSERT INTO gold.br_stocks_daily (
    symbol,
    asset,
    currency,
    price,
    price_date,
    price_ts,
    source,
    silver_request_id
)
SELECT
    s.symbol,
    s.asset,
    s.currency,
    s.price,
    s.price_date,
    s.price_ts,
    'yfinance' AS source,
    s.request_id AS silver_request_id
FROM silver.yfinance_prices AS s
WHERE s.currency = 'brl'
  AND s.symbol NOT LIKE '^%'
  AND s.symbol NOT LIKE '%=X'
  AND s.symbol NOT IN ('IVVB11', 'IVVB11.SA')
ON CONFLICT (symbol, price_date)
DO UPDATE SET
    price = EXCLUDED.price,
    asset = EXCLUDED.asset,
    currency = EXCLUDED.currency,
    price_ts = EXCLUDED.price_ts,
    source = EXCLUDED.source,
    silver_request_id = EXCLUDED.silver_request_id,
    ingested_at = NOW();
