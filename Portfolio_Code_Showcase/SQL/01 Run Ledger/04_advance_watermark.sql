-- ==========================================================================
-- 04_advance_watermark.sql
-- --------------------------------------------------------------------------
-- What it does: Advances the watermark only after a successful load.
-- What it demonstrates: Ordering is the point: the watermark moves last. A
-- crash mid-load leaves it unchanged, so the next run reprocesses rather
-- than skips.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
UPDATE pipeline.pipeline_watermarks
        SET    LastSuccessfulRun     = :wm,
               LastSuccessfulDateKey = :dk,
               LastRunStatus        = 'SUCCESS',
               UpdatedAt            = SYSDATETIME()
        WHERE  PipelineKey = :pk
