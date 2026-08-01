-- ==========================================================================
-- 06_ops_canonical_snapshot.sql
-- --------------------------------------------------------------------------
-- What it does: Reads the canonical operations position for comparison.
-- What it demonstrates: The DS=1 side of the occupancy check. Short, but it
-- is the anchor the whole comparison hangs from.
-- Source: compute_reconciliation_actions.py (extracted verbatim)
-- ==========================================================================
SELECT PropertyKey,
               CAST(OccupiedUnits AS FLOAT) /
                 NULLIF(CAST(OccupiedUnits + VacantUnits AS FLOAT), 0) AS pct
        FROM   dbo.fact_property_ops_daily
        WHERE  DataSource = 1
          AND  DateKey    = :dk
