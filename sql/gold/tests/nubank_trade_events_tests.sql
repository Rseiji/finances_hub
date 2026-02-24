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
              OR date IS NULL
           OR fetched_at IS NULL
    ) THEN
        RAISE EXCEPTION 'gold.nubank_trade_events: nulls found in required columns';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM gold.nubank_trade_events
        WHERE preco <= 0
    ) THEN
        RAISE EXCEPTION 'gold.nubank_trade_events: non-positive preco found';
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
