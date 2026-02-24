DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.nubank_trade_events
        WHERE event_id IS NULL
           OR batch_id IS NULL
           OR ticker IS NULL
           OR quantidade IS NULL
           OR preco IS NULL
           OR valor IS NULL
           OR fetched_at IS NULL
    ) THEN
        RAISE EXCEPTION 'silver.nubank_trade_events: nulls found in required columns';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM silver.nubank_trade_events
        WHERE preco < 0
    ) THEN
        RAISE EXCEPTION 'silver.nubank_trade_events: non-positive preco found';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM silver.nubank_trade_events
        WHERE ticker !~ '[0-9]'
    ) THEN
        RAISE EXCEPTION 'silver.nubank_trade_events: ticker without digit found';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM silver.nubank_trade_events
        GROUP BY event_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'silver.nubank_trade_events: duplicate event_id found';
    END IF;
END $$;
