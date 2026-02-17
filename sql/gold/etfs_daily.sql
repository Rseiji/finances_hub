INSERT INTO gold.etfs_daily (
    asset,
    currency,
    price,
    price_date,
    price_ts,
    source,
    silver_request_id
)
SELECT
    s.asset,
    s.currency,
    s.price,
    s.price_date,
    s.price_ts,
    'yfinance' AS source,
    s.request_id AS silver_request_id
FROM silver.yfinance_prices AS s
WHERE s.symbol IN ('IVVB11', 'IVVB11.SA')
ON CONFLICT (asset, currency, price_date)
DO UPDATE SET
    price = EXCLUDED.price,
    currency = EXCLUDED.currency,
    price_ts = EXCLUDED.price_ts,
    source = EXCLUDED.source,
    silver_request_id = EXCLUDED.silver_request_id,
    ingested_at = NOW();
