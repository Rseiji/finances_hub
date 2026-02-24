DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM gold.nubank_trade_events
        WHERE event_id IS NULL
           OR batch_id IS NULL
           OR ticker IS NULL
           OR quantidade IS NULL
           OR preco IS NULL
           OR valor IS NULL
           OR tax IS NULL
              OR date IS NULL
           OR fetched_at IS NULL
    ) THEN
        RAISE EXCEPTION 'gold.nubank_trade_events: nulls found in required columns';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM gold.nubank_trade_events
        WHERE preco < 0
    ) THEN
        RAISE EXCEPTION 'gold.nubank_trade_events: non-positive preco found';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM gold.nubank_trade_events
        WHERE tax < 0
    ) THEN
        RAISE EXCEPTION 'gold.nubank_trade_events: negative tax found';
    END IF;

    IF EXISTS (
        WITH gold_by_date AS (
            SELECT date, COALESCE(SUM(tax), 0)::DOUBLE PRECISION AS allocated_tax
            FROM gold.nubank_trade_events
            GROUP BY date
        ),
        bronze_taxes_by_date AS (
            SELECT
                date,
                (
                    COALESCE(SUM(taxa_liquidacao), 0)
                    + COALESCE(SUM(emolumento), 0)
                    + COALESCE(SUM(transf_ativos), 0)
                )::DOUBLE PRECISION AS total_tax
            FROM bronze.nubank_trade_taxes
            GROUP BY date
        )
        SELECT 1
        FROM gold_by_date AS g
        LEFT JOIN bronze_taxes_by_date AS b
          ON b.date = g.date
        WHERE ABS(g.allocated_tax - COALESCE(b.total_tax, 0)) > 0.01
    ) THEN
        RAISE EXCEPTION 'gold.nubank_trade_events: tax allocation mismatch by date';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM gold.nubank_trade_events
        GROUP BY date, ticker, quantidade, preco
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'gold.nubank_trade_events: duplicate business key found';
    END IF;

END $$;
