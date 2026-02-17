DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM bronze.ingestion_events
        WHERE uid IS NULL
           OR source IS NULL
           OR endpoint IS NULL
           OR request_params IS NULL
           OR asset IS NULL
           OR currency IS NULL
           OR fetched_at IS NULL
           OR payload IS NULL
           OR created_at IS NULL
    ) THEN
        RAISE EXCEPTION 'bronze.ingestion_events: nulls found in required columns';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM bronze.ingestion_events
        GROUP BY uid
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'bronze.ingestion_events: duplicate uid found';
    END IF;
END $$;
