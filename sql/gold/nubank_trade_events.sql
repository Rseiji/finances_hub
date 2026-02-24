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
),
final_rows AS (
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
),
date_taxes AS (
    SELECT
        t.date,
        (
            COALESCE(SUM(t.taxa_liquidacao), 0)
            + COALESCE(SUM(t.emolumento), 0)
            + COALESCE(SUM(t.transf_ativos), 0)
        )::DOUBLE PRECISION AS total_tax
    FROM bronze.nubank_trade_taxes AS t
    GROUP BY t.date
),
allocated AS (
    SELECT
        f.event_id,
        f.batch_id,
        f.ticker,
        f.quantidade,
        f.preco,
        f.valor,
        CASE
            WHEN SUM(ABS(f.valor)) OVER (PARTITION BY f.date) > 0
            THEN COALESCE(dt.total_tax, 0)
                 * (
                     ABS(f.valor)
                     / SUM(ABS(f.valor)) OVER (PARTITION BY f.date)
                 )
            ELSE 0
        END AS tax,
        f.date,
        f.fetched_at
    FROM final_rows AS f
    LEFT JOIN date_taxes AS dt
      ON dt.date = f.date
)
INSERT INTO gold.nubank_trade_events (
    event_id,
    batch_id,
    ticker,
    quantidade,
    preco,
    valor,
    tax,
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
    tax,
    date,
    fetched_at
FROM allocated
ON CONFLICT (date, ticker, quantidade, preco)
DO UPDATE SET
    event_id = EXCLUDED.event_id,
    batch_id = EXCLUDED.batch_id,
    ticker = EXCLUDED.ticker,
    quantidade = EXCLUDED.quantidade,
    preco = EXCLUDED.preco,
    valor = EXCLUDED.valor,
    tax = EXCLUDED.tax,
    date = EXCLUDED.date,
    fetched_at = EXCLUDED.fetched_at,
    ingested_at = NOW();
