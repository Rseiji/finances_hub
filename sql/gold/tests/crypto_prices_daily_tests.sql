DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM gold.crypto_prices_daily
        WHERE asset IS NULL
           OR currency IS NULL
           OR price IS NULL
           OR price_date IS NULL
           OR price_ts IS NULL
           OR source IS NULL
           OR silver_request_id IS NULL
    ) THEN
        RAISE EXCEPTION 'gold.crypto_prices_daily: nulls found in required columns';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM gold.crypto_prices_daily
        WHERE price <= 0
    ) THEN
        RAISE EXCEPTION 'gold.crypto_prices_daily: non-positive price found';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM gold.crypto_prices_daily
        GROUP BY asset, currency, price_date
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'gold.crypto_prices_daily: duplicate (asset, currency, price_date)';
    END IF;
END $$;
