-- ==========================================================================
-- 02_raise_flag.sql
-- --------------------------------------------------------------------------
-- What it does: Raises a typed, severity-graded flag against a run.
-- What it demonstrates: Severity is assigned at write time by the rule that
-- detected the problem, not later by a human triaging a list.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
INSERT INTO pipeline.pipeline_flags
            (RunId, RunDate, PipelineKey, SourceObject, SourceRecordId,
             ProspectKey, FlagType, FlagField, OriginalValue, ResolvedValue,
             SuppressedKey, Notes, CreatedAt)
        VALUES
            (:run_id, :run_date, :pk, :src_obj, :src_id,
             :prospect_key, :flag_type, :flag_field, :orig_val, :res_val,
             :suppressed_key, :notes, SYSDATETIME())
