-- ==========================================================================
-- 01_quarantine_row.sql
-- --------------------------------------------------------------------------
-- What it does: Writes a rejected row to quarantine with its reason.
-- What it demonstrates: Bad rows are set aside with a cause, never dropped.
-- The row survives, so a defect can be investigated instead of inferred from
-- a count that does not add up.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
INSERT INTO pipeline.silver_quarantine
            (RunId, RunDate, PipelineKey, SourceObject, SourceRecordId,
             QuarantineReason, RawData, Notes, CreatedAt)
        VALUES
            (:run_id, :run_date, :pk, :src_obj, :src_id,
             :reason, :raw, :notes, SYSDATETIME())
