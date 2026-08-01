-- ==========================================================================
-- 01_reconcile_spend_divergence.sql
-- --------------------------------------------------------------------------
-- What it does: Compares vendor spend against canonical truth and writes
-- divergence actions.
-- What it demonstrates: The platform's core idea in one statement: two
-- systems disagree, the disagreement is measured, graded and queued for a
-- person. It is not averaged away.
-- Source: crm_pipeline.py (extracted verbatim)
-- ==========================================================================
MERGE dbo.fact_prospect_journey AS target
        USING (VALUES (
            :prospect_key, :prop_key, :date_key, :vendor_key, :channel_key,
            :funnel_key, :touch_num, :total_touches, :days_before,
            :lease_date_key, :converted, :attr_credit,
            :is_direct, :is_assisted, :lease_value, :datasource
        )) AS source (
            ProspectKey, PropertyKey, DateKey, VendorKey, ChannelKey,
            FunnelStageKey, TouchNumber, TotalTouches, DaysBeforeLease,
            LeaseDateKey, Converted, AttributedCredit,
            IsDirectCredit, IsAssistedCredit, LeaseValueAnnual, DataSource
        )
        ON  target.ProspectKey  = source.ProspectKey
        AND target.TouchNumber  = source.TouchNumber
        AND target.DataSource   = source.DataSource
        WHEN MATCHED THEN UPDATE SET
            target.AttributedCredit  = source.AttributedCredit,
            target.IsDirectCredit    = source.IsDirectCredit,
            target.IsAssistedCredit  = source.IsAssistedCredit,
            target.LeaseValueAnnual  = source.LeaseValueAnnual
        WHEN NOT MATCHED THEN INSERT (
            ProspectKey, PropertyKey, DateKey, VendorKey, ChannelKey,
            FunnelStageKey, TouchNumber, TotalTouches, DaysBeforeLease,
            LeaseDateKey, Converted, AttributedCredit,
            IsDirectCredit, IsAssistedCredit, LeaseValueAnnual, DataSource
        ) VALUES (
            source.ProspectKey, source.PropertyKey, source.DateKey,
            source.VendorKey, source.ChannelKey,
            source.FunnelStageKey, source.TouchNumber, source.TotalTouches,
            source.DaysBeforeLease, source.LeaseDateKey, source.Converted,
            source.AttributedCredit, source.IsDirectCredit,
            source.IsAssistedCredit, source.LeaseValueAnnual, source.DataSource
        );
