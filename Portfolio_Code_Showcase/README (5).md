// ==========================================================================
// Ops Index - Strongest Component
// Model folder: 04 - Ops Index / 4.1 - Components
// --------------------------------------------------------------------------
// Purpose: What is holding up.
// Why it exists: Prevents an intervention aimed at something already
// working.
// ==========================================================================

VAR Occ = [Ops Index - Occupancy]
VAR Vac = [Ops Index - Vacancy]
VAR Abs = [Ops Index - Absorption]
VAR Cov = [Ops Index - Coverage]
VAR Vel = [Ops Index - Lease Velocity]
VAR Net = [Ops Index - Net Absorption]
VAR SLA = [Ops Index - SLA Compliance]

VAR MaxScore = MAX(Occ, MAX(Vac, MAX(Abs, MAX(Cov, MAX(Vel, MAX(Net, SLA))))))

RETURN
SWITCH(
    TRUE(),
    MaxScore = Occ, "Occupancy Rate",
    MaxScore = Vac, "Vacancy Management",
    MaxScore = Abs, "Absorption Speed",
    MaxScore = Cov, "Coverage Ratio",
    MaxScore = Vel, "Lease Velocity",
    MaxScore = Net, "Net Absorption",
    MaxScore = SLA, "SLA Compliance",
    BLANK()
)
