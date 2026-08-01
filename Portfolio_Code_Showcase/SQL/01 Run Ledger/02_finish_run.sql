-- ==========================================================================
-- 02_finish_run.sql
-- --------------------------------------------------------------------------
-- What it does: Closes the run with status, duration and five row counters.
-- What it demonstrates: Duration is computed in SQL rather than Python, so
-- it is measured against one clock. Extracted, cleansed, loaded, flagged and
-- quarantined are stored separately because a run that extracts 7,957 rows
-- and loads 180 is doing something worth seeing.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
UPDATE pipeline.pipeline_runs
        SET    Status            = :status,
               RunEndTime        = SYSDATETIME(),
               DurationSeconds   = DATEDIFF(SECOND, RunStartTime, SYSDATETIME()),
               RowsExtracted     = :extracted,
               RowsCleansed      = :cleansed,
               RowsLoaded        = :loaded,
               RowsFlagged       = :flagged,
               RowsQuarantined   = :quarantined,
               RowsAttrConflict  = :attr_conflict,
               WatermarkNew      = :wm_new
        WHERE  RunId = :run_id
