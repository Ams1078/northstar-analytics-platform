-- ==========================================================================
-- 02_merge_dimension.sql
-- --------------------------------------------------------------------------
-- What it does: Upserts dimension rows.
-- What it demonstrates: Matched, not-matched and source-missing handled
-- explicitly. Nothing is deleted implicitly.
-- Source: spend_pipeline.py (extracted verbatim)
-- ==========================================================================
MERGE dbo.fact_marketing_spend_daily AS tgt
        USING (VALUES (
            :DateKey, :PropertyKey, :VendorKey, :Spend, :DataSource
        )) AS src (
            DateKey, PropertyKey, VendorKey, Spend, DataSource
        )
        ON  tgt.DateKey     = src.DateKey
        AND tgt.PropertyKey = src.PropertyKey
        AND tgt.VendorKey   = src.VendorKey
        AND tgt.DataSource  = src.DataSource
        WHEN MATCHED THEN
            UPDATE SET Spend = src.Spend
        WHEN NOT MATCHED THEN
            INSERT (DateKey, PropertyKey, VendorKey, Spend, DataSource)
            VALUES (
                src.DateKey, src.PropertyKey, src.VendorKey,
                src.Spend, src.DataSource
            );
