DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.yfinance_prices
        WHERE request_id IS NULL
           OR symbol IS NULL
           OR asset IS NULL
           OR currency IS NULL
           OR price IS NULL
           OR price_ts IS NULL
           OR price_date IS NULL
           OR fetched_at IS NULL
    ) THEN
        RAISE EXCEPTION 'silver.yfinance_prices: nulls found in required columns';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM silver.yfinance_prices
        WHERE price <= 0
    ) THEN
        RAISE EXCEPTION 'silver.yfinance_prices: non-positive price found';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM silver.yfinance_prices
        GROUP BY symbol, currency, price_date
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'silver.yfinance_prices: duplicate (symbol, currency, price_date)';
    END IF;
END $$;
