-- ==========================================================================
-- 01_merge_canonical_truth.sql
-- --------------------------------------------------------------------------
-- What it does: Merges staged canonical truth into the warehouse fact.
-- What it demonstrates: The largest statement in the platform and the one
-- worth reading closely. Staged first, then merged on the natural key, so
-- the published table is never left half-written.
-- Source: ops_pipeline.py (extracted verbatim)
-- ==========================================================================
MERGE dbo.fact_property_ops_daily AS target
        USING (VALUES (
            :date_key, :prop_key,
            :OccupiedUnits, :VacantUnits, :AvailableUnits,
            :MoveIns, :MoveOuts, :LeaseExpirations, :ScheduledMoveIns,
            :LeaseExpirations_Next60D, :ScheduledMoveIns_Next60D,
            :datasource
        )) AS source (
            DateKey, PropertyKey,
            OccupiedUnits, VacantUnits, AvailableUnits,
            MoveIns, MoveOuts, LeaseExpirations, ScheduledMoveIns,
            LeaseExpirations_Next60D, ScheduledMoveIns_Next60D,
            DataSource
        )
        ON  target.DateKey     = source.DateKey
        AND target.PropertyKey = source.PropertyKey
        AND target.DataSource  = source.DataSource
        WHEN MATCHED THEN UPDATE SET
            target.OccupiedUnits             = source.OccupiedUnits,
            target.VacantUnits               = source.VacantUnits,
            target.AvailableUnits            = source.AvailableUnits,
            target.MoveIns                   = source.MoveIns,
            target.MoveOuts                  = source.MoveOuts,
            target.LeaseExpirations          = source.LeaseExpirations,
            target.ScheduledMoveIns          = source.ScheduledMoveIns,
            target.LeaseExpirations_Next60D  = source.LeaseExpirations_Next60D,
            target.ScheduledMoveIns_Next60D  = source.ScheduledMoveIns_Next60D
        WHEN NOT MATCHED THEN INSERT (
            DateKey, PropertyKey,
            OccupiedUnits, VacantUnits, AvailableUnits,
            MoveIns, MoveOuts, LeaseExpirations, ScheduledMoveIns,
            LeaseExpirations_Next60D, ScheduledMoveIns_Next60D,
            DataSource
        ) VALUES (
            source.DateKey, source.PropertyKey,
            source.OccupiedUnits, source.VacantUnits, source.AvailableUnits,
            source.MoveIns, source.MoveOuts, source.LeaseExpirations,
            source.ScheduledMoveIns,
            source.LeaseExpirations_Next60D, source.ScheduledMoveIns_Next60D,
            source.DataSource
        );
