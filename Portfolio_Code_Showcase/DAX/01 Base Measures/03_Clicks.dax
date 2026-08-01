// ==========================================================================
// Clicks
// Model folder: 00 - Core Metrics / 1.1 Funnel Volumes
// --------------------------------------------------------------------------
// Purpose: Click volume by vendor.
// Why it exists: Pairs with impressions to give CTR.
// ==========================================================================

CALCULATE(SUM(fact_marketing_funnel_daily[MetricValue]), dim_funnel_stage[FunnelStageName] = "Clicks")
