WITH latest_batches AS (
    SELECT
        ranked.date,
        ranked.batch_id
    FROM (
        SELECT
            s.date,
            s.batch_id,
            max(s.fetched_at) AS max_fetched_at,
            row_number() OVER (
                PARTITION BY s.date
                ORDER BY max(s.fetched_at) DESC, s.batch_id DESC
            ) AS row_rank
        FROM silver.nubank_trade_events AS s
        WHERE s.date IS NOT NULL
        GROUP BY s.date, s.batch_id
    ) AS ranked
    WHERE ranked.row_rank = 1
)
DELETE FROM gold.nubank_trade_events AS g
USING latest_batches AS lb
WHERE g.date = lb.date
  AND g.batch_id <> lb.batch_id;

WITH latest_batches AS (
    SELECT
        ranked.date,
        ranked.batch_id
    FROM (
        SELECT
            s.date,
            s.batch_id,
            max(s.fetched_at) AS max_fetched_at,
            row_number() OVER (
                PARTITION BY s.date
                ORDER BY max(s.fetched_at) DESC, s.batch_id DESC
            ) AS row_rank
        FROM silver.nubank_trade_events AS s
        WHERE s.date IS NOT NULL
        GROUP BY s.date, s.batch_id
    ) AS ranked
    WHERE ranked.row_rank = 1
),
base AS (
    SELECT
        s.event_id,
        s.batch_id,
        s.ticker,
        s.quantidade,
        s.preco,
        s.valor,
        s.date,
        s.fetched_at
    FROM silver.nubank_trade_events AS s
    JOIN latest_batches AS lb
      ON lb.date = s.date
     AND lb.batch_id = s.batch_id
),
transformed AS (
    SELECT
        event_id,
        batch_id,
        ticker,
        quantidade,
        preco,
        valor,
        date,
        fetched_at
    FROM base
),
ranked AS (
    SELECT
        event_id,
        batch_id,
        ticker,
        quantidade,
        preco,
        valor,
        date,
        fetched_at,
        row_number() OVER (
            PARTITION BY date, ticker, quantidade, preco
            ORDER BY fetched_at DESC, batch_id DESC, event_id DESC
        ) AS row_rank
    FROM transformed
)
INSERT INTO gold.nubank_trade_events (
    event_id,
    batch_id,
    ticker,
    quantidade,
    preco,
    valor,
    date,
    fetched_at
)
SELECT
    event_id,
    batch_id,
    ticker,
    quantidade,
    preco,
    valor,
    date,
    fetched_at
FROM ranked
WHERE row_rank = 1
ON CONFLICT (date, ticker, quantidade, preco)
DO UPDATE SET
    event_id = EXCLUDED.event_id,
    batch_id = EXCLUDED.batch_id,
    ticker = EXCLUDED.ticker,
    quantidade = EXCLUDED.quantidade,
    preco = EXCLUDED.preco,
    valor = EXCLUDED.valor,
    date = EXCLUDED.date,
    fetched_at = EXCLUDED.fetched_at,
    ingested_at = NOW();
