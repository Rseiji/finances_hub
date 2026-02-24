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
    'manual_20241119_trpl4_sell',
    '11111111-1111-1111-1111-202411190002',
    'TRPL4',
    -121,
    24.52,
    -2966.92,
    DATE '2024-11-19',
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
    'manual_20241119_isae4_buy',
    '11111111-1111-1111-1111-202411190003',
    'ISAE4',
    121,
    24.52,
    2966.92,
    DATE '2024-11-19',
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
