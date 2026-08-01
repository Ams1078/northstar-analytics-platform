// ==========================================================================
// Consideration Score
// Model folder: 07 - VPI / 7.2 - KPI
// --------------------------------------------------------------------------
// Purpose: Consideration stage score.
// Why it exists: Same shape, different inputs. Repetition here is deliberate
// and readable.
// ==========================================================================

VAR VisitsScore = [Consideration_Visits_Percentile]
VAR CPVScore = [Consideration_CPV_Percentile]
VAR V2LScore = [Consideration_VisitToLead_Percentile]

VAR ValidScores = 
    IF(NOT(ISBLANK(VisitsScore)), 1, 0) +
    IF(NOT(ISBLANK(CPVScore)), 1, 0) +
    IF(NOT(ISBLANK(V2LScore)), 1, 0)

RETURN
IF(
    ValidScores = 0,
    BLANK(),
    (
        IF(ISBLANK(VisitsScore), 0, VisitsScore * 0.33) +
        IF(ISBLANK(CPVScore), 0, CPVScore * 0.34) +
        IF(ISBLANK(V2LScore), 0, V2LScore * 0.33)
    )
)
