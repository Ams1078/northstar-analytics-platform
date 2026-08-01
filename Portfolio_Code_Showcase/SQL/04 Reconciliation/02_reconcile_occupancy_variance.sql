-- ==========================================================================
-- 02_reconcile_occupancy_variance.sql
-- --------------------------------------------------------------------------
-- What it does: Detects occupancy differences beyond tolerance.
-- What it demonstrates: A tolerance band rather than an equality test,
-- because two systems recording the same building will never agree to the
-- row.
-- Source: crm_pipeline.py (extracted verbatim)
-- ==========================================================================
MERGE dbo.fact_leasing_daily AS target
        USING (VALUES (
            :date_key, :prop_key, :leads, :new_leases, :visits,
            :attr_leases, :unattr_leases, :datasource
        )) AS source (
            DateKey, PropertyKey, Leads, NewLeases, Visits,
            AttributedNewLeases, UnattributedLeases, DataSource
        )
        ON  target.DateKey      = source.DateKey
        AND target.PropertyKey  = source.PropertyKey
        AND target.DataSource   = source.DataSource
        WHEN MATCHED THEN UPDATE SET
            target.Leads                = source.Leads,
            target.NewLeases            = source.NewLeases,
            target.Visits               = source.Visits,
            target.AttributedNewLeases  = source.AttributedNewLeases,
            target.UnattributedLeases   = source.UnattributedLeases
        WHEN NOT MATCHED THEN INSERT (
            DateKey, PropertyKey, Leads, NewLeases, Visits,
            AttributedNewLeases, UnattributedLeases, DataSource
        ) VALUES (
            source.DateKey, source.PropertyKey, source.Leads,
            source.NewLeases, source.Visits,
            source.AttributedNewLeases, source.UnattributedLeases,
            source.DataSource
        );
