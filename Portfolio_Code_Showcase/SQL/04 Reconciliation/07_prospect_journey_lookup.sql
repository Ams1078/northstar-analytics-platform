-- ==========================================================================
-- 07_prospect_journey_lookup.sql
-- --------------------------------------------------------------------------
-- What it does: Retrieves reconstructed prospect journeys.
-- What it demonstrates: Feeds attribution scoring. The journey is rebuilt
-- once and read many times rather than recomputed per model.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
SELECT sp.SourceSalesforceId, sp.ProspectKey
            FROM   pipeline.silver_prospects sp
            JOIN   #sfid_batch b
              ON   b.SourceSalesforceId = sp.SourceSalesforceId
