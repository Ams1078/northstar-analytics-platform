-- ==========================================================================
-- 04_write_action.sql
-- --------------------------------------------------------------------------
-- What it does: Writes a reconciliation action with type, severity and
-- status.
-- What it demonstrates: Every detector converges on this one shape, which is
-- why the dashboard queue can filter across sources that share nothing else.
-- Source: compute_reconciliation_actions.py (extracted verbatim)
-- ==========================================================================
INSERT INTO pipeline.reconciliation_actions
            (DateKey, BronzeDate, SourceSystem, ActionType, ActionCount, Severity,
             Description, BronzeRowsTotal, BronzeRowsAfterAction,
             DeltaAmount, DeltaPct,
             BronzePath, SampleRecordId, RunId, PipelineKey)
        VALUES
            (:DateKey, :BronzeDate, :SourceSystem, :ActionType, :ActionCount, :Severity,
             :Description, :BronzeRowsTotal, :BronzeRowsAfterAction,
             :DeltaAmount, :DeltaPct,
             :BronzePath, :SampleRecordId, :RunId, :PipelineKey)
