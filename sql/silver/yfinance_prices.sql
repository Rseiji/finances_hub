WITH price_rows AS (
    SELECT
        e.uid AS request_id,
        e.request_params->>'symbol' AS symbol,
        e.asset,
        e.currency,
        (row->>'value')::numeric AS price,
        (row->>'date')::date::timestamptz AS price_ts,
        (row->>'date')::date AS price_date,
        e.request_params->>'series_type' AS series_type,
        e.fetched_at
    FROM bronze.ingestion_events AS e
    CROSS JOIN LATERAL jsonb_array_elements(e.payload->'rows') AS row
    WHERE e.source = 'yfinance'
      AND e.payload ? 'rows'
      AND e.request_params ? 'symbol'
)
INSERT INTO silver.yfinance_prices (
    request_id,
    symbol,
    asset,
    currency,
    price,
    price_ts,
    price_date,
    series_type,
    fetched_at
)
SELECT DISTINCT ON (symbol, currency, price_date)
    request_id,
    symbol,
    asset,
    currency,
    price,
    price_ts,
    price_date,
    series_type,
    fetched_at
FROM price_rows
ORDER BY symbol, currency, price_date, price_ts DESC, fetched_at DESC
ON CONFLICT (symbol, currency, price_date)
DO UPDATE SET
    price = EXCLUDED.price,
    asset = EXCLUDED.asset,
    currency = EXCLUDED.currency,
    request_id = EXCLUDED.request_id,
    price_ts = EXCLUDED.price_ts,
    series_type = EXCLUDED.series_type,
    fetched_at = EXCLUDED.fetched_at;
