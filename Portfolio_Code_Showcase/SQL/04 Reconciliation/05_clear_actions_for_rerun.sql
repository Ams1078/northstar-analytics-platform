-- ==========================================================================
-- 05_clear_actions_for_rerun.sql
-- --------------------------------------------------------------------------
-- What it does: Removes prior actions for a date before recomputation.
-- What it demonstrates: Same idempotency contract as the rest of the
-- platform, applied to derived output.
-- Source: compute_reconciliation_actions.py (extracted verbatim)
-- ==========================================================================
DELETE FROM pipeline.reconciliation_actions
        WHERE DateKey = :dk
