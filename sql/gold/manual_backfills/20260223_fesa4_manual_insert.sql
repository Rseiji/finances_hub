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
    'manual_20240124',
    '11111111-1111-1111-1111-202401240001',
    'FESA4',
    90,
    0,
    0,
    DATE '2024-01-23',
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
