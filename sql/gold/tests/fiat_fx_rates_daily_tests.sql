DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM gold.fiat_fx_rates_daily
          WHERE asset IS NULL
              OR base_currency IS NULL
              OR quote_currency IS NULL
              OR currency IS NULL
              OR rate IS NULL
              OR rate_date IS NULL
              OR rate_ts IS NULL
              OR source IS NULL
              OR silver_request_id IS NULL
    ) THEN
        RAISE EXCEPTION 'gold.fiat_fx_rates_daily: nulls found in required columns';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM gold.fiat_fx_rates_daily
        WHERE rate <= 0
    ) THEN
        RAISE EXCEPTION 'gold.fiat_fx_rates_daily: non-positive rate found';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM gold.fiat_fx_rates_daily
        GROUP BY asset, currency, rate_date
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'gold.fiat_fx_rates_daily: duplicate (asset, currency, rate_date)';
    END IF;
END $$;
