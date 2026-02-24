WITH normalized AS (
    SELECT
        b.uid AS batch_id,
        b.date,
        b.fetched_at,
        b.cv,
        upper(
            regexp_replace(
                coalesce(
                    (
                        regexp_match(
                            regexp_replace(trim(b.espec_titulo), '\\s+', ' ', 'g'),
                            '([A-Za-z]{4,6}[0-9]{1,2}F?)'
                        )
                    )[1],
                    split_part(trim(b.espec_titulo), ' ', 1)
                ),
                'F$',
                '',
                'i'
            )
        ) AS ticker,
        cast(
            CASE
                WHEN trim(b.quantidade) = '' THEN NULL
                WHEN position(',' IN b.quantidade) > 0 THEN replace(replace(trim(b.quantidade), '.', ''), ',', '.')
                WHEN trim(b.quantidade) ~ '^[0-9]{1,3}(\.[0-9]{3})+$' THEN replace(trim(b.quantidade), '.', '')
                ELSE trim(b.quantidade)
            END AS DOUBLE PRECISION
        ) AS quantidade_abs,
        cast(
            CASE
                WHEN trim(b.preco) = '' THEN NULL
                WHEN position(',' IN b.preco) > 0 THEN replace(replace(trim(b.preco), '.', ''), ',', '.')
                WHEN trim(b.preco) ~ '^[0-9]{1,3}(\.[0-9]{3})+$' THEN replace(trim(b.preco), '.', '')
                ELSE trim(b.preco)
            END AS DOUBLE PRECISION
        ) AS preco,
        cast(
            CASE
                WHEN trim(b.valor) = '' THEN NULL
                WHEN position(',' IN b.valor) > 0 THEN replace(replace(trim(b.valor), '.', ''), ',', '.')
                WHEN trim(b.valor) ~ '^[0-9]{1,3}(\.[0-9]{3})+$' THEN replace(trim(b.valor), '.', '')
                ELSE trim(b.valor)
            END AS DOUBLE PRECISION
        ) AS valor_abs,
        row_number() OVER (
            PARTITION BY b.uid
            ORDER BY
                b.fetched_at,
                b.file_name,
                b.file_path,
                b.espec_titulo,
                b.cv,
                b.quantidade,
                b.preco,
                b.valor,
                b.dc,
                b.mercado,
                b.tipo_mercado,
                b.observacao
        ) AS row_num
    FROM bronze.pdf_nubank_trade_events AS b
    WHERE NOT EXISTS (
        SELECT 1
        FROM silver.nubank_trade_events AS s
        WHERE s.batch_id = b.uid
    )
),
transformed AS (
    SELECT
        md5(
            concat_ws(
                '|',
                batch_id::text,
                coalesce(date::text, ''),
                row_num::text,
                ticker,
                coalesce(cv, '')
            )
        ) AS event_id,
        batch_id,
        ticker,
        CASE
            WHEN upper(trim(cv)) = 'C' THEN quantidade_abs
            ELSE -quantidade_abs
        END AS quantidade,
        preco,
        CASE
            WHEN upper(trim(cv)) = 'C' THEN valor_abs
            ELSE -valor_abs
        END AS valor,
        date,
        fetched_at
    FROM normalized
)
INSERT INTO silver.nubank_trade_events (
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
FROM transformed
ON CONFLICT (event_id)
DO NOTHING;
