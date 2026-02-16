WITH price_rows AS (
    SELECT
        e.uid AS request_id,
        e.request_params->>'symbol' AS symbol,
        e.asset,
        e.currency,
        (row->>'close')::numeric AS price,
        to_timestamp((row->>'close_time')::double precision / 1000.0) AS price_ts,
        to_timestamp((row->>'close_time')::double precision / 1000.0)::date AS price_date,
        e.fetched_at
    FROM bronze.ingestion_events AS e
    CROSS JOIN LATERAL jsonb_array_elements(e.payload->'rows') AS row
    WHERE e.source = 'binance'
      AND e.payload ? 'rows'
      AND e.request_params ? 'symbol'
      AND e.currency IS NOT NULL
)
INSERT INTO silver.binance_prices (
    request_id,
    symbol,
    asset,
    currency,
    price,
    price_ts,
    price_date,
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
    fetched_at = EXCLUDED.fetched_at;
