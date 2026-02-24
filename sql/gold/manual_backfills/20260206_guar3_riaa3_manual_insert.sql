INSERT INTO gold.nubank_trade_events (
    event_id,
    batch_id,
    ticker,
    quantidade,
    preco,
    valor,
    date,
    fetched_at,
    ingested_at
)
VALUES (
    'manual_20260206_guar3_sell',
    '11111111-1111-1111-1111-202602060004',
    'GUAR3',
    -544,
    8.68,
    -4721.92,
    DATE '2026-02-06',
    NOW(),
    NOW()
)
ON CONFLICT (date, ticker, quantidade, preco)
DO UPDATE SET
    event_id = EXCLUDED.event_id,
    batch_id = EXCLUDED.batch_id,
    valor = EXCLUDED.valor,
    fetched_at = EXCLUDED.fetched_at,
    ingested_at = NOW();

INSERT INTO gold.nubank_trade_events (
    event_id,
    batch_id,
    ticker,
    quantidade,
    preco,
    valor,
    date,
    fetched_at,
    ingested_at
)
VALUES (
    'manual_20260206_riaa3_buy',
    '11111111-1111-1111-1111-202602060005',
    'RIAA3',
    544,
    8.68,
    4721.92,
    DATE '2026-02-06',
    NOW(),
    NOW()
)
ON CONFLICT (date, ticker, quantidade, preco)
DO UPDATE SET
    event_id = EXCLUDED.event_id,
    batch_id = EXCLUDED.batch_id,
    valor = EXCLUDED.valor,
    fetched_at = EXCLUDED.fetched_at,
    ingested_at = NOW();
