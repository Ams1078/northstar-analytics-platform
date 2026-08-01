-- ==========================================================================
-- 04_clear_quarantine_for_rerun.sql
-- --------------------------------------------------------------------------
-- What it does: Clears quarantine for one date and source before a re-run.
-- What it demonstrates: The idempotency pattern in miniature: scoped by both
-- date and source, so re-running a night reproduces it rather than doubling
-- it.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
DELETE FROM pipeline.silver_quarantine
        WHERE PipelineKey = :pk
          AND RunDate = :rd
