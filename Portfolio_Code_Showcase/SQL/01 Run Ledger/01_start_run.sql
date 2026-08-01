-- ==========================================================================
-- 01_start_run.sql
-- --------------------------------------------------------------------------
-- What it does: Opens a run row with status RUNNING and returns its
-- identity.
-- What it demonstrates: OUTPUT INSERTED to get the key back in one round
-- trip. Every downstream write is keyed to this RunId, which is what makes a
-- night auditable.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
INSERT INTO pipeline.pipeline_runs
            (PipelineKey, PipelineName, RunDate, RunStartTime, Status, WatermarkUsed)
        OUTPUT INSERTED.RunId
        VALUES
            (:pk, :pname, :rd, SYSDATETIME(), 'RUNNING', :wm)
