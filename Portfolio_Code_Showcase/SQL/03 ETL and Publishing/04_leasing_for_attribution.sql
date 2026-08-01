-- ==========================================================================
-- 04_leasing_for_attribution.sql
-- --------------------------------------------------------------------------
-- What it does: Reads leasing facts for attribution reconstruction.
-- What it demonstrates: Note the DataSource filter. Any query here that
-- omitted it would blend canonical truth with the CRM rebuild and return a
-- number that means nothing.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
SELECT DateKey, PropertyKey, NewLeases, AttributedNewLeases,
               (AttributedNewLeases - NewLeases) AS ConflictDelta
        FROM   dbo.fact_leasing_daily
        WHERE  DataSource = 2
          AND  AttributedNewLeases > NewLeases
          AND  DateKey >= :dk
