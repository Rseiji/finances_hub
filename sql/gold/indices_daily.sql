INSERT INTO gold.indices_daily (
    index_code,
    asset,
    currency,
    price,
    price_date,
    price_ts,
    source,
    silver_request_id
)
SELECT
    CASE
        WHEN s.symbol = '^GSPC' THEN 'SP500'
        WHEN s.symbol = '^BVSP' THEN 'IBOV'
        ELSE ltrim(s.symbol, '^')
    END AS index_code,
    s.asset,
    s.currency,
    s.price,
    s.price_date,
    s.price_ts,
    'yfinance' AS source,
    s.request_id AS silver_request_id
FROM silver.yfinance_prices AS s
WHERE s.symbol LIKE '^%'
ON CONFLICT (index_code, price_date)
DO UPDATE SET
    price = EXCLUDED.price,
    asset = EXCLUDED.asset,
    currency = EXCLUDED.currency,
    price_ts = EXCLUDED.price_ts,
    source = EXCLUDED.source,
    silver_request_id = EXCLUDED.silver_request_id,
    ingested_at = NOW();
