-- ==========================================================================
-- 05_property_key_resolution.sql
-- --------------------------------------------------------------------------
-- What it does: Resolves incoming property identifiers to the dimension key.
-- What it demonstrates: Seven vendor formats name the property seven ways.
-- This is where they converge on one key.
-- Source: pipeline_utils.py (extracted verbatim)
-- ==========================================================================
SELECT PropertyKey, PropertyName, MarketKey, RegionKey,
               PropertyState AS State, PropertyCity AS City, TotalUnits
        FROM   dbo.dim_property
        WHERE  IsActive = 1
