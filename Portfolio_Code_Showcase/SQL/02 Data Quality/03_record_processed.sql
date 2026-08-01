-- ==========================================================================
-- 03_record_processed.sql
-- --------------------------------------------------------------------------
-- What it does: Records rows that passed cleansing.
-- What it demonstrates: Processed and quarantined are written to separate
-- tables. Their sum reconciles to extracted, which is the check that catches
-- a silent drop.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
INSERT INTO pipeline.silver_processed
            (RunId, RunDate, ProcessedAt, PipelineKey, SourceObject,
             SourceRecordId, Outcome, ProspectKey, PropertyKey, VendorKey,
             DuplicateOf, FlagSummary, GoldTable)
        VALUES
            (:run_id, :run_date, SYSDATETIME(), :pk, :src_obj,
             :src_id, :outcome, :prospect_key, :property_key, :vendor_key,
             :dup_of, :flag_summary, :gold_table)
