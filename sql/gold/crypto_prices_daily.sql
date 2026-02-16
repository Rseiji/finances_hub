INSERT INTO gold.crypto_prices_daily (
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
    CASE
        WHEN s.currency ILIKE '%usdt%' THEN 'USD'
        ELSE s.currency
    END AS currency,
    s.price::numeric(38, 18) AS price,
    s.price_date,
    s.price_ts,
    'binance' AS source,
    s.request_id AS silver_request_id
FROM silver.binance_prices AS s
ON CONFLICT (asset, currency, price_date)
DO UPDATE SET
    price = EXCLUDED.price,
    asset = EXCLUDED.asset,
    price_ts = EXCLUDED.price_ts,
    source = EXCLUDED.source,
    silver_request_id = EXCLUDED.silver_request_id,
    ingested_at = NOW();
