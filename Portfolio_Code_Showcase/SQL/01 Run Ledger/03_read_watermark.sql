-- ==========================================================================
-- 03_read_watermark.sql
-- --------------------------------------------------------------------------
-- What it does: Reads the last successful run for a pipeline.
-- What it demonstrates: Incremental processing starts here. The watermark is
-- per pipeline, not global, so one slow source never holds back the others.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
SELECT LastSuccessfulRun
        FROM   pipeline.pipeline_watermarks
        WHERE  PipelineKey = :pk
