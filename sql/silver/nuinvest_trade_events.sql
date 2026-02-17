WITH base AS (
    SELECT
        uid AS event_id,
        cv,
        quantidade,
        preco,
        valor,
        taxa_liquidacao,
        emolumentos,
        taxa_transf_ativos,
        statement_date,
        fetched_at,
        regexp_replace(split_part(trim(espec_titulo), ' ', 1), 'F$', '', 'i') AS ticker_raw
    FROM bronze.pdf_nuinvest_trade_events
),
transformed AS (
    SELECT
        event_id,
        upper(ticker_raw) AS ticker,
        CASE
            WHEN upper(trim(cv)) = 'C' THEN quantidade
            ELSE -quantidade
        END AS quantidade,
        preco,
        valor,
        (coalesce(taxa_liquidacao, 0) + coalesce(emolumentos, 0) + coalesce(taxa_transf_ativos, 0))
            * abs(valor)
            / nullif(sum(abs(valor)) OVER (PARTITION BY statement_date, fetched_at), 0)
            AS tax,
        statement_date,
        fetched_at
    FROM base
)
INSERT INTO silver.nuinvest_trade_events (
    event_id,
    ticker,
    quantidade,
    preco,
    valor,
    tax,
    statement_date,
    fetched_at
)
SELECT
    event_id,
    ticker,
    quantidade,
    preco,
    valor,
    tax,
    statement_date,
    fetched_at
FROM transformed
ON CONFLICT (event_id)
DO UPDATE SET
    ticker = EXCLUDED.ticker,
    quantidade = EXCLUDED.quantidade,
    preco = EXCLUDED.preco,
    valor = EXCLUDED.valor,
    tax = EXCLUDED.tax,
    statement_date = EXCLUDED.statement_date,
    fetched_at = EXCLUDED.fetched_at;
