-- ==========================================================================
-- 03_spend_at_comparable_grain.sql
-- --------------------------------------------------------------------------
-- What it does: Aggregates spend to the grain the source file can actually
-- be compared at.
-- What it demonstrates: The hard part of vendor integration. A multi-vendor
-- file cannot be anchored to a single VendorKey, so the comparison is lifted
-- to a grain where both sides mean the same thing.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
SELECT s1.DateKey, s1.PropertyKey, s1.VendorKey,
               CAST(s1.Spend AS DECIMAL(18,4)) AS canonical_spend,
               CAST(s2.Spend AS DECIMAL(18,4)) AS source_spend,
               CAST(ABS(s1.Spend - s2.Spend) AS DECIMAL(18,4)) AS divergence
        FROM   dbo.fact_marketing_spend_daily s1
        JOIN   dbo.fact_marketing_spend_daily s2
            ON s1.DateKey     = s2.DateKey
           AND s1.PropertyKey = s2.PropertyKey
           AND s1.VendorKey   = s2.VendorKey
        WHERE  s1.DataSource = :canon
          AND  s2.DataSource = :src
          AND  s1.DateKey    = :dk
          AND  ABS(s1.Spend - s2.Spend) > :tol
        ORDER BY s1.PropertyKey, s1.VendorKey
