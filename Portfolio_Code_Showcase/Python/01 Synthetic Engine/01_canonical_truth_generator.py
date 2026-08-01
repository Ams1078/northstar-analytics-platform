"""
MAA Marketing Analytics Synthetic Data Generator + Azure SQL Loader
Version 12.5 - CPL Rebalance Patch
Approved analytical target band for this case study:
  - Portfolio-level 24-month attributed CPL should generally sit in a controlled range
    designed for dashboard readability rather than raw market realism.
  - Preferred distribution target:
      Median attributed CPL: $300-$450
      P75 attributed CPL:    $550-$750
      P90 attributed CPL:    $800-$1,050
      Max attributed CPL:    ~$1,200
  - Acceptable visual-use band:
      Most properties between ~$275 and ~$950, with rare edge cases up to ~$1,200 max.
  - Design intent:
      Preserve relative marketing-performance separation while suppressing long-tail
      denominator noise that would distort portfolio dashboards and scatterplots.
  v12.4 CPL floor tightening patch:
  1. Top-of-funnel lead rebalance reduced again to soften lease supply
  2. Lease floor tightened materially across all tiers to lift CPL floor
  3. Final-stage lease variance preserved to keep realistic spread
  4. Target band unchanged: raise realized CPL, not just printed thresholds
  v9 issues fixed:
  1. Attribution floor removed — attributed_today is now capped by new_leases, not a floor
  2. TARGET_L2L_TIER lowered to match FUNNEL_RATES calibrated values
  3. Hard daily lease cap added (tier-based monthly rate / 30)
  Result: realistic monthly lease counts (8-25/property), L2L 5-15%, sell rate 5-15%

═══════════════════════════════════════════════════════════════════════
CHANGELOG: v6 → v7
═══════════════════════════════════════════════════════════════════════

FIX 1 — OccupiedUnits > TotalUnits overflow (57% of rows affected)
─────────────────────────────────────────────────────────────────────
Problem (v6): 71 of 120 properties had OccupiedUnits > TotalUnits for
  most or all of their date range (max OccRate = 1.947). Root cause:
  the np.clip ceiling used int(occ_ceiling * total_units) which is
  correct, but vacant_after was computed BEFORE moveins were capped,
  so when leasing_lookup provided a NewLeases value > vacant_after,
  the min() cap on moveins_today was applied correctly but then the
  occupied carry-forward could still overflow due to rounding at the
  integer boundary. Also, vacant = total_units - occupied could go
  negative when occupied rounded above total_units.

Fix (v7):
  1. np.clip ceiling changed from int(occ_ceiling * total_units) to
     min(int(occ_ceiling * total_units), total_units) — absolute hard
     cap at total_units regardless of rounding.
  2. vacant = max(0, total_units - occupied) — floor at 0, never
     produces negative vacant units.

FIX 2 — MoveIns sparsity (80% of daily rows had MoveIns = 0)
─────────────────────────────────────────────────────────────────────
Problem (v6): The binomial chain (leads → visits → new_leases) produces
  many zero days at the daily grain because each stage multiplies small
  probabilities. At the monthly aggregated grain this caused Absorb Days
  to compute as median 195 days and mean 414 days (vs realistic 15-60).
  The Operations Index Absorption component (20% weight) was scoring
  ~5-10/100 for most properties as a result.

Fix (v7): After the binomial chain, if new_leases == 0 but the expected
  daily lease rate (daily_leads_base * adj_v2l * adj_l2l * seasonal) is
  >= 0.25, draw directly from Poisson(expected_rate) instead. This fills
  in the zero-streaks without inflating total lease volume — it just
  distributes the same expected volume more evenly across days.
  Result: MoveIns nonzero on ~50-60% of days (up from 20%),
  monthly Absorb Days median drops to ~20-40 days range.

FIX 3 — Coverage Ratio structurally capped below scoring ceiling
─────────────────────────────────────────────────────────────────────
Problem (v6): ScheduledMoveIns_Next60D was computed as:
    exp_next60 * RENEWAL_RATE(0.50) + vacant * 0.4
  This formula structurally produced a max ratio of ~0.942 because
  ScheduledMoveIns could never meaningfully exceed LeaseExpirations.
  The DAX scoring ceiling of 1.20 was therefore unreachable — no
  property could ever score above ~66/100 on this component.

Fix (v7):
  Generator: tier-based renewal boost replaces flat RENEWAL_RATE:
    Star: 0.72, Good: 0.62, Average: 0.52, Struggler: 0.42
  vacant contribution raised from 0.40 → 0.55
  This allows Star/Good properties to produce ratios up to ~1.10-1.15,
  making the full scoring range reachable.

  DAX (Operations_Index_Score + Coverage component):
  Ceiling lowered from 1.20 → 0.95 to match realistic data distribution.
  Floor unchanged at 0.45.

RULES (unchanged from v6):
───────────────────────────
- SQL mode: ONLY fact tables written. All dims protected.
- CSV mode: all tables saved including dims (for reference)
- dim_property internal columns excluded from CSV via EXCLUDE_COLS

Usage:
    python maa_data_generator_v7.py --mode csv
    python maa_data_generator_v7.py --mode sql
    python maa_data_generator_v7.py --mode both
"""

import argparse
import math
import os
import random
import urllib.parse
from datetime import date, timedelta

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────
# REPRODUCIBILITY
# ─────────────────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)


# ─────────────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────────────
import os

OUTPUT_DIR = os.environ.get("MAA_OUTPUT_DIR", "/tmp/maa_generated_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)
PROPERTY_SEED_PATH = "./dim_property.csv"


# ─────────────────────────────────────────────────────────────────────
# AZURE SQL CONFIGURATION
# ─────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "server":   "maa-analytics-sql.database.windows.net",
    "database": "maa_marketing_analytics",
    "username": "PowerBI",
    "password": "Suede1978",
    "driver":   "ODBC Driver 18 for SQL Server",
}


# ─────────────────────────────────────────────────────────────────────
# DATE RANGE
# ─────────────────────────────────────────────────────────────────────
DATE_START = date(2024, 1, 1)
DATE_END = date.today() - timedelta(days=1)

N_REGIONS    = 4
N_MARKETS    = 12
N_PROPERTIES = 120   # 10 per market

# ─────────────────────────────────────────────────────────────────────
# CASE STUDY ANALYTICAL TARGET BAND
# These are validation targets for 24-month attributed CPL distribution.
# They are intentionally tighter than raw market reality to preserve
# dashboard readability and proportionality in the case study.
# ─────────────────────────────────────────────────────────────────────
TARGET_CPL_BAND = {
    "preferred_min": 275,
    "preferred_max": 950,
    "soft_cap_max": 1200,
    "median_low": 300,
    "median_high": 450,
    "p75_low": 550,
    "p75_high": 750,
    "p90_low": 800,
    "p90_high": 1050,
}


# ─────────────────────────────────────────────────────────────────────
# PROPERTY SIZE
# ─────────────────────────────────────────────────────────────────────
PROPERTY_SIZE = {"min": 150, "max": 350, "mean": 230, "std": 45}

LEASE_TERMS      = {"12mo": 0.70, "6mo": 0.20, "mtm": 0.10}
LEASE_TERM_DAYS  = {"12mo": 365,  "6mo": 180,  "mtm": 30}
RENEWAL_RATE     = 0.50
ABSORPTION_DAYS  = (15, 28)


# ─────────────────────────────────────────────────────────────────────
# PERFORMANCE TIERS  (property-level)
# ─────────────────────────────────────────────────────────────────────
PERFORMANCE_TIERS = {
    "Star": {
        "share": 0.20,
        "occ_base": 0.945, "occ_std": 0.01,
        "cpl_mult": 0.60,
        "performance_cap": 1.08, "conv_mult": 1.20,
        "spend_min": 10_000, "spend_max": 14_000,
        "lead_peak_min": 100, "lead_peak_max": 140,
        "lead_offpeak_min": 50, "lead_offpeak_max": 70,
    },
    "Good": {
        "share": 0.40,
        "occ_base": 0.90, "occ_std": 0.015,
        "cpl_mult": 0.85, "conv_mult": 1.08,
        "spend_min": 8_000, "spend_max": 11_000,
        "lead_peak_min": 70, "lead_peak_max": 100,
        "lead_offpeak_min": 35, "lead_offpeak_max": 55,
    },
    "Average": {
        "share": 0.25,
        "occ_base": 0.85, "occ_std": 0.02,
        "cpl_mult": 1.15, "conv_mult": 0.95,
        "spend_min": 5_500, "spend_max": 8_000,
        "lead_peak_min": 45, "lead_peak_max": 70,
        "lead_offpeak_min": 20, "lead_offpeak_max": 35,
    },
    "Struggler": {
        "share": 0.15,
        "occ_base": 0.78, "occ_std": 0.025,
        "cpl_mult": 1.60, "conv_mult": 0.75,
        "spend_min": 3_000, "spend_max": 5_500,
        "lead_peak_min": 25, "lead_peak_max": 45,
        "lead_offpeak_min": 10, "lead_offpeak_max": 20,
    },
}


# ─────────────────────────────────────────────────────────────────────
# MARKET STRENGTH TIERS
# Strong  → above-average ops + marketing performance
# Mid     → baseline
# Weak    → below-average ops AND marketing
# ─────────────────────────────────────────────────────────────────────
MARKET_STRENGTH = {
    # East
    "Northeast Corridor": "Strong",
    "Mid-Atlantic":       "Mid",
    "New England":        "Mid",
    # West
    "Pacific Coast":      "Strong",
    "Mountain West":      "Weak",
    "Southwest":          "Weak",
    # Central
    "Great Lakes":        "Mid",
    "Midwest Plains":     "Weak",
    "Upper Midwest":      "Weak",
    # South
    "Southeast":          "Mid",
    "Gulf Coast":         "Mid",
    "Sun Belt":           "Strong",
}

# Multipliers applied on top of property-tier values
MARKET_STRENGTH_MULT = {
    "Strong": {"occ_delta": +0.03, "spend_mult": 1.10, "conv_mult": 1.08, "lead_mult": 1.15, "cohesion": 0.08},
    "Mid":    {"occ_delta":  0.00, "spend_mult": 1.00, "conv_mult": 1.00, "lead_mult": 1.00, "cohesion": 0.14},
    "Weak":   {"occ_delta": -0.04, "spend_mult": 0.88, "conv_mult": 0.90, "lead_mult": 0.80, "cohesion": 0.22},
}


# ─────────────────────────────────────────────────────────────────────
# PROPERTY TIER MAP  (10 per market)
# v6: corrected to align with spec market strength tiers (Section 2 of spec).
#   Strong markets  → No Strugglers; Star/Good dominant
#   Mid markets     → Moderate Star, Good/Average majority, limited Strugglers
#   Weak markets    → No Stars; Average/Struggler dominant per spec
#     ("poor operational performance concentrated in Midwest")
# ─────────────────────────────────────────────────────────────────────
MARKET_TIER_MAP = {
    # ── EAST REGION ─────────────────────────────────────────────────
    "Northeast Corridor": ["Star","Star","Star","Good","Good","Good","Good","Good","Average","Average"],   # Strong — unchanged
    "Mid-Atlantic":       ["Star","Good","Good","Good","Good","Average","Average","Average","Struggler","Struggler"],  # Mid — v5 was too Star-heavy (2S,6G,2A)
    "New England":        ["Star","Good","Good","Good","Average","Average","Average","Average","Struggler","Struggler"],  # Mid — unchanged
    # ── WEST REGION ─────────────────────────────────────────────────
    "Pacific Coast":      ["Star","Star","Star","Star","Good","Good","Good","Good","Average","Average"],   # Strong — v5 had 1 Struggler, removed
    "Mountain West":      ["Good","Good","Average","Average","Average","Average","Struggler","Struggler","Struggler","Struggler"],  # Weak — v5 had 4 Stars, removed all
    "Southwest":          ["Good","Good","Average","Average","Average","Average","Struggler","Struggler","Struggler","Struggler"],  # Weak — aligned to rule set
    # ── CENTRAL REGION ──────────────────────────────────────────────
    "Great Lakes":        ["Star","Good","Good","Good","Average","Average","Average","Average","Struggler","Struggler"],  # Mid — unchanged
    "Midwest Plains":     ["Good","Average","Average","Average","Average","Struggler","Struggler","Struggler","Struggler","Struggler"],  # Weak — v5 too few X
    "Upper Midwest":      ["Good","Average","Average","Average","Average","Struggler","Struggler","Struggler","Struggler","Struggler"],  # Weak — v5 had 1 Star, removed
    # ── SOUTH REGION ────────────────────────────────────────────────
    "Southeast":          ["Star","Good","Good","Good","Average","Average","Average","Average","Struggler","Struggler"],  # Mid — unchanged
    "Gulf Coast":         ["Good","Good","Average","Average","Average","Average","Average","Struggler","Struggler","Struggler"],  # Mid — v5 had 6 Strugglers, reduced
    "Sun Belt":           ["Star","Star","Good","Good","Good","Good","Good","Average","Average","Average"],  # Strong — v5 was completely inverted (0S,0G,4A,6X)
}


# ─────────────────────────────────────────────────────────────────────
# REGIONAL SEASONALITY CURVES
# month → multiplier (1.0 = baseline)
# Compressed email range applied separately
# ─────────────────────────────────────────────────────────────────────
REGIONAL_SEASONAL = {
    # Northeast + Mid-Atlantic: sharp spring peak, slow winter
    "East": {
        1: 0.55, 2: 0.60, 3: 0.82, 4: 1.18,
        5: 1.35, 6: 1.40, 7: 1.28, 8: 1.12,
        9: 0.98, 10: 0.85, 11: 0.68, 12: 0.52,
    },
    # Southeast + Gulf Coast + Sun Belt: flat, mild winter dip, secondary Sept-Oct
    "South": {
        1: 0.78, 2: 0.80, 3: 0.92, 4: 1.05,
        5: 1.15, 6: 1.18, 7: 1.12, 8: 1.08,
        9: 1.05, 10: 1.02, 11: 0.88, 12: 0.78,
    },
    # Great Lakes + Midwest Plains + Upper Midwest: later peak May-Jul, sharp winter
    "Central": {
        1: 0.50, 2: 0.55, 3: 0.72, 4: 0.95,
        5: 1.22, 6: 1.32, 7: 1.30, 8: 1.18,
        9: 1.00, 10: 0.82, 11: 0.62, 12: 0.48,
    },
    # Pacific Coast + Mountain West + Southwest: moderate, less extreme
    "West": {
        1: 0.72, 2: 0.75, 3: 0.88, 4: 1.05,
        5: 1.18, 6: 1.25, 7: 1.20, 8: 1.15,
        9: 1.05, 10: 0.95, 11: 0.82, 12: 0.70,
    },
}

# Region key → seasonal curve mapping
REGION_KEY_TO_CURVE = {1: "East", 2: "Central", 3: "West", 4: "South"}

# Email channel: compressed multiplier 0.80-1.15 (warm list, less seasonal)
EMAIL_SEASONAL = {
    1: 0.82, 2: 0.84, 3: 0.90, 4: 1.00,
    5: 1.08, 6: 1.12, 7: 1.10, 8: 1.06,
    9: 1.02, 10: 0.98, 11: 0.90, 12: 0.80,
}

# Legacy global seasonal (used for ops / fallback)
SEASONAL = {
    1: 0.65, 2: 0.70, 3: 0.85, 4: 1.10,
    5: 1.25, 6: 1.30, 7: 1.25, 8: 1.15,
    9: 1.00, 10: 0.90, 11: 0.75, 12: 0.60,
}

# v6: tightened deltas — v5 values were too aggressive, especially South (-0.09)
# which stacked on top of low Struggler bases and produced ~63% occupancy.
# These are small modifiers; MarketStrength already handles major regional variance.
REGION_OCC_DELTA = {"East": 0.02, "Central": -0.01, "West": 0.01, "South": -0.03}

# ─────────────────────────────────────────────────────────────────────
# OCCUPANCY BANDS  (hard floor/ceiling per tier — used in ops builder)
# v6: moved from inside build_fact_property_ops_daily() to module level
#     for visibility. Values raised to match new occ_base calibration.
# ─────────────────────────────────────────────────────────────────────
TIER_OCC_BAND = {
    "Star":      (0.88, 0.99),
    "Good":      (0.82, 0.96),
    "Average":   (0.75, 0.92),
    "Struggler": (0.65, 0.87),
}


# ─────────────────────────────────────────────────────────────────────
# CHANNEL / VENDOR SPEND MIX
# channel 5 = Email (VendorKey 11)
# ─────────────────────────────────────────────────────────────────────
CHANNEL_SPEND_MIX = {
    1: 0.45,  # ILS
    2: 0.23,  # Paid Search
    3: 0.14,  # Paid Social
    4: 0.10,  # Display
    5: 0.08   # Email
} 

# Base vendor shares within channel (before drift)
VENDOR_CHANNEL_SHARE_BASE = {
    1: {1: 0.40, 2: 0.38, 3: 0.22},   # Zillow dominant, Apartment List distant
    2: {4: 0.70, 5: 0.30},
    3: {6: 0.60, 7: 0.40},
    4: {8: 0.50, 9: 0.30, 10: 0.20},
    5: {11: 1.00},
}

IMPRESSIONS_PER_DOLLAR = {
    1: 8,    # ILS
    2: 15,   # Paid Search
    3: 25,   # Paid Social
    4: 35,   # Display
    5: 3,    # Email — warm list sends, not ad impressions; low volume by design
}


# Vendor-specific conversion bias so lead-to-lease is not flat across vendors.
# Values are directional and modest: channel benchmarks still dominate, but
# vendors now have persistent efficiency differences inside each channel.
VENDOR_CONVERSION_BIAS = {
    # Hierarchy: Email > Zillow > Google Ads > Apartments.com > Bing Ads
    #          > Apartment List > Instagram > Facebook > Google Display > StackAdapt > TradeDesk
    # ILS spread widened (Zillow 1.12 vs Apt List 0.88 = 24pt gap)
    # Email pulled down to 1.08 — was inflating total leases
    # Google vs Bing tightened slightly per GPT guidance
    1:  {"ctr": 1.06, "c2v": 1.04, "v2l": 1.04, "l2l": 1.12},  # Zillow — ILS leader
    2:  {"ctr": 1.01, "c2v": 1.00, "v2l": 1.01, "l2l": 1.03},  # Apartments.com — mid
    3:  {"ctr": 0.93, "c2v": 0.93, "v2l": 0.91, "l2l": 0.88},  # Apartment List — laggard
    4:  {"ctr": 1.04, "c2v": 1.02, "v2l": 1.03, "l2l": 1.05},  # Google Ads — search leader
    5:  {"ctr": 0.93, "c2v": 0.96, "v2l": 0.98, "l2l": 0.95},  # Bing Ads — lower efficiency
    6:  {"ctr": 0.98, "c2v": 0.96, "v2l": 0.95, "l2l": 0.90},  # Facebook — awareness weak
    7:  {"ctr": 1.01, "c2v": 1.00, "v2l": 0.99, "l2l": 0.94},  # Instagram — modest edge on FB
    8:  {"ctr": 1.03, "c2v": 1.01, "v2l": 1.01, "l2l": 1.02},  # Google Display — display leader
    9:  {"ctr": 0.97, "c2v": 0.97, "v2l": 0.96, "l2l": 0.91},  # StackAdapt — mid display
    10: {"ctr": 0.90, "c2v": 0.91, "v2l": 0.92, "l2l": 0.82},  # TradeDesk — weakest
    11: {"ctr": 1.06, "c2v": 1.04, "v2l": 1.04, "l2l": 1.08},  # Email — highest L2L
}


# ─────────────────────────────────────────────────────────────────────
# CHANNEL FUNNEL RATES  (benchmarked)
# ─────────────────────────────────────────────────────────────────────
CHANNEL_FUNNEL_RATES = {
    # Real-world channel hierarchy enforced in l2l:
    #   Email > ILS > Paid Search > Paid Social > Display
    # ILS gets strong l2l because it's the primary lease-generation channel
    # Display gets low l2l — top-of-funnel awareness, rarely closes alone
    # Search sits between ILS and Social — intent-based but lower volume
    1: {   # ILS — primary lease channel, strong qualified intent
        "Star":      {"ctr": 0.150, "c2v": 0.30, "v2l": 0.28, "l2l": 0.22},
        "Good":      {"ctr": 0.120, "c2v": 0.24, "v2l": 0.22, "l2l": 0.17},
        "Average":   {"ctr": 0.090, "c2v": 0.18, "v2l": 0.16, "l2l": 0.12},
        "Struggler": {"ctr": 0.060, "c2v": 0.12, "v2l": 0.11, "l2l": 0.07},
    },
    2: {   # Paid Search — intent-based but lower volume than ILS
        "Star":      {"ctr": 0.080, "c2v": 0.060, "v2l": 0.34, "l2l": 0.16},
        "Good":      {"ctr": 0.065, "c2v": 0.048, "v2l": 0.26, "l2l": 0.12},
        "Average":   {"ctr": 0.050, "c2v": 0.036, "v2l": 0.20, "l2l": 0.09},
        "Struggler": {"ctr": 0.035, "c2v": 0.025, "v2l": 0.15, "l2l": 0.06},
    },
    3: {   # Paid Social — awareness/mid-funnel, lower conversion
        "Star":      {"ctr": 0.020, "c2v": 0.040, "v2l": 0.28, "l2l": 0.10},
        "Good":      {"ctr": 0.015, "c2v": 0.032, "v2l": 0.22, "l2l": 0.08},
        "Average":   {"ctr": 0.011, "c2v": 0.024, "v2l": 0.17, "l2l": 0.05},
        "Struggler": {"ctr": 0.008, "c2v": 0.016, "v2l": 0.13, "l2l": 0.03},
    },
    4: {   # Display — top-of-funnel only, rarely converts alone
        "Star":      {"ctr": 0.011, "c2v": 0.020, "v2l": 0.24, "l2l": 0.06},
        "Good":      {"ctr": 0.008, "c2v": 0.015, "v2l": 0.19, "l2l": 0.04},
        "Average":   {"ctr": 0.006, "c2v": 0.010, "v2l": 0.14, "l2l": 0.03},
        "Struggler": {"ctr": 0.004, "c2v": 0.006, "v2l": 0.10, "l2l": 0.02},
    },
    5: {   # Email — warm/nurture list, highest L2L, low volume
        "Star":      {"ctr": 0.050, "c2v": 0.050, "v2l": 0.36, "l2l": 0.24},
        "Good":      {"ctr": 0.040, "c2v": 0.040, "v2l": 0.29, "l2l": 0.20},
        "Average":   {"ctr": 0.028, "c2v": 0.030, "v2l": 0.23, "l2l": 0.15},
        "Struggler": {"ctr": 0.018, "c2v": 0.022, "v2l": 0.17, "l2l": 0.11},
    },
}

# Fallback tier-level funnel rates (for leasing daily, prospect journey)
# l2l recalibrated: base at Mid/balanced produces operational L2L midpoints:
#   Struggler ~4.8% | Average ~7.5% | Good ~10% | Star ~13%
# After multiplier stack (lq_conv_mult × ms_conv_mult):
#   Struggler 3-6% | Average 5-10% | Good 7-13% | Star 9-17%
FUNNEL_RATES = {
    "Star":      {"ctr": 0.055, "c2v": 0.32, "v2l": 0.55, "l2l": 0.145},
    "Good":      {"ctr": 0.038, "c2v": 0.24, "v2l": 0.45, "l2l": 0.105},
    "Average":   {"ctr": 0.025, "c2v": 0.16, "v2l": 0.35, "l2l": 0.070},
    "Struggler": {"ctr": 0.015, "c2v": 0.09, "v2l": 0.25, "l2l": 0.040},
}


# ─────────────────────────────────────────────────────────────────────
# STORY-DRIVEN MACRO EVENTS
# Each event: date range + affected channels + effect on spend/funnel
# ─────────────────────────────────────────────────────────────────────
# Event structure:
#   "name"         : label
#   "start"        : (year, month) inclusive
#   "end"          : (year, month) inclusive
#   "channel_mult" : {channel_key: spend_multiplier}
#   "funnel_mult"  : {channel_key: {"ctr"|"c2v"|"v2l"|"l2l": mult}}
#   "realloc_to"   : channel_key that receives diverted budget (or None)
STORY_EVENTS = [
    {
        # Q2 2024: Meta CPM spike +15-20% → budget shifts toward Google Search
        "name": "Meta CPM Spike Q2-2024",
        "start": (2024, 4), "end": (2024, 6),
        "channel_mult":  {3: 0.82},          # Paid Social spend -18%
        "funnel_mult":   {3: {"ctr": 0.85}}, # CTR drops (higher CPM, same clicks)
        "realloc_to":    2,                   # overflow → Paid Search
    },
    {
        # Q3 2024: Apartments.com price increase → ILS budget trims, some to Paid Search
        "name": "Apartments.com Price Hike Q3-2024",
        "start": (2024, 7), "end": (2024, 9),
        "channel_mult":  {1: 0.90},          # ILS spend -10%
        "funnel_mult":   {},
        "realloc_to":    2,
        # Within ILS, Apartments.com share shrinks; Zillow absorbs
        "vendor_share_override": {1: {1: 0.48, 2: 0.28, 3: 0.24}},
    },
    {
        # Q1 2025: Google Search maturation — strong market conversion improves
        "name": "Google Search Maturation Q1-2025",
        "start": (2025, 1), "end": (2025, 3),
        "channel_mult":  {},
        "funnel_mult":   {2: {"l2l": 1.07}},  # +7% l2l on Paid Search
        "realloc_to":    None,
        "strong_markets_only": True,           # only Strong market properties
    },
    {
        # Q3 2025: Display/programmatic pullback — TradeDesk + StackAdapt underperform
        "name": "Display Pullback Q3-2025",
        "start": (2025, 7), "end": (2025, 9),
        "channel_mult":  {4: 0.75},           # Display spend -25%
        "funnel_mult":   {4: {"ctr": 0.80, "c2v": 0.82}},
        "realloc_to":    2,
        # Within Display, Google Display absorbs; StackAdapt + TradeDesk cut
        "vendor_share_override": {4: {8: 0.70, 9: 0.18, 10: 0.12}},
    },
]


# ─────────────────────────────────────────────────────────────────────
# ATTRIBUTION RULES
# ─────────────────────────────────────────────────────────────────────
ATTRIBUTION_LOOKBACK_DAYS = 7
DECAY_LAMBDA              = 0.1
JOURNEY_MIX               = {1: 0.40, 2: 0.35, 3: 0.25}

PROSPECT_CONVERSION_RATE = {
    # Recalibrated v8_patched: rates set to produce 65-80% attribution coverage
    # (% of tracked prospects who sign a lease — warm pipeline, not cold traffic)
    "Star":      0.38,
    "Good":      0.29,
    "Average":   0.22,
    "Struggler": 0.15,
}
PROSPECT_VOLUME_MONTHLY = {
    # Scaled ~2x to bring prospect conversion rate to realistic 8-12%
    # Converted count stays fixed (driven by funnel_lookup).
    # More non-converting prospects = realistic top-of-funnel drop-off.
    "peak":    {"min": 156, "max": 254},
    "offpeak": {"min": 58,  "max": 117},
}

ATTRIBUTION_CAPTURE_BY_TIER = {
    # Small nudge up to push coverage from 57.7% → 60-65%.
    # All other funnel/vendor logic unchanged.
    "Star":      (0.96, 0.99),
    "Good":      (0.93, 0.97),
    "Average":   (0.88, 0.94),
    "Struggler": (0.82, 0.90),
}

ORGANIC_LEASE_SHARE_BY_TIER = {
    # Tightened further — organic/dark-funnel share reduced to stop
    # unattributed leases from inflating the denominator.
    # ATTRIBUTION_CAPTURE_BY_TIER is not wired to the active chain;
    # this constant IS the live lever for attribution coverage.
    "Star":      (0.00, 0.02),
    "Good":      (0.00, 0.04),
    "Average":   (0.01, 0.06),
    "Struggler": (0.03, 0.08),
}

MAX_ATTRIBUTION_SHARE_BY_TIER = {
    "Star": 0.96,
    "Good": 0.94,
    "Average": 0.91,
    "Struggler": 0.88,
}

TOUCH_STAGE_DIST = {
    1: {1: 0.35, 2: 0.30, 3: 0.20, 4: 0.15},
    2: {1: 0.15, 2: 0.25, 3: 0.35, 4: 0.25},
    3: {1: 0.05, 2: 0.10, 3: 0.30, 4: 0.55},
}

VENDOR_TOUCH_AFFINITY = {
    1:  {1: 1.6, 2: 1.4, 3: 0.8},
    2:  {1: 1.5, 2: 1.3, 3: 0.9},
    3:  {1: 1.3, 2: 1.2, 3: 1.0},
    4:  {1: 0.8, 2: 1.0, 3: 1.8},
    5:  {1: 0.7, 2: 1.0, 3: 1.5},
    6:  {1: 1.2, 2: 1.3, 3: 0.9},
    7:  {1: 1.3, 2: 1.1, 3: 0.8},
    8:  {1: 1.4, 2: 0.9, 3: 0.6},
    9:  {1: 1.3, 2: 0.9, 3: 0.6},
    10: {1: 1.2, 2: 0.9, 3: 0.7},
    11: {1: 1.1, 2: 1.2, 3: 1.0},
}


# ─────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────
def get_seasonal_mult(month: int, region_key: int = None) -> float:
    """Return regional seasonal multiplier. Falls back to global SEASONAL."""
    if region_key is not None:
        curve_name = REGION_KEY_TO_CURVE.get(region_key, "East")
        return REGIONAL_SEASONAL[curve_name][month]
    return SEASONAL[month]


def get_email_seasonal(month: int) -> float:
    return EMAIL_SEASONAL[month]


def get_region_name(region_key: int) -> str:
    return {1: "East", 2: "Central", 3: "West", 4: "South"}[region_key]


def get_month_end(d: date) -> date:
    return (d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)


def interpolate_between_seasonal_bounds(
    seasonal_mult: float, offpeak_val: float, peak_val: float
) -> float:
    return offpeak_val + (peak_val - offpeak_val) * (seasonal_mult - 0.60) / 0.70


def get_target_attr_coverage(tier: str) -> float:
    """
    Target tracked-lease coverage by property tier.
    Returns the share of funnel leases expected to remain attributable after
    dark-funnel leakage, missed tracking, and offline conversion.
    """
    low, high = ATTRIBUTION_CAPTURE_BY_TIER.get(tier, (0.62, 0.72))
    return float(np.random.uniform(low, high))


def get_organic_lease_share(tier: str) -> float:
    """Share of operational leasing expected to come from non-attributed demand."""
    low, high = ORGANIC_LEASE_SHARE_BY_TIER.get(tier, (0.22, 0.34))
    return float(np.random.uniform(low, high))


def get_active_events(year: int, month: int) -> list:
    """Return list of story events active in (year, month)."""
    active = []
    for ev in STORY_EVENTS:
        ey, em = ev["start"]
        ey2, em2 = ev["end"]
        if (ey, em) <= (year, month) <= (ey2, em2):
            active.append(ev)
    return active


def get_story_channel_mult(year: int, month: int, channel_key: int,
                            is_strong_market: bool = False) -> float:
    """Composite spend multiplier from all active story events for a channel."""
    mult = 1.0
    for ev in get_active_events(year, month):
        if ev.get("strong_markets_only") and not is_strong_market:
            continue
        mult *= ev.get("channel_mult", {}).get(channel_key, 1.0)
    return mult


def get_story_funnel_mult(year: int, month: int, channel_key: int,
                           metric: str, is_strong_market: bool = False) -> float:
    """Composite funnel rate multiplier from active story events."""
    mult = 1.0
    for ev in get_active_events(year, month):
        if ev.get("strong_markets_only") and not is_strong_market:
            continue
        ch_funnel = ev.get("funnel_mult", {}).get(channel_key, {})
        mult *= ch_funnel.get(metric, 1.0)
    return mult


def get_story_vendor_shares(year: int, month: int, channel_key: int) -> dict | None:
    """Return vendor share override if any active event specifies one for this channel."""
    for ev in get_active_events(year, month):
        overrides = ev.get("vendor_share_override", {})
        if channel_key in overrides:
            return overrides[channel_key]
    return None


def get_story_realloc_channel(year: int, month: int, channel_key: int) -> int | None:
    """If an event cuts this channel's spend, return where the overflow goes."""
    for ev in get_active_events(year, month):
        if channel_key in ev.get("channel_mult", {}):
            cut = ev["channel_mult"][channel_key]
            if cut < 1.0 and ev.get("realloc_to") is not None:
                return ev["realloc_to"]
    return None



def compress_rate(rate: float, floor: float, ceiling: float, power: float = 1.35) -> float:
    """
    Compress high-end conversion extremes while preserving relative ordering.
    power > 1 pulls values back toward the floor; power < 1 would expand them.
    """
    if ceiling <= floor:
        return float(rate)
    norm = (rate - floor) / (ceiling - floor)
    norm = float(np.clip(norm, 0.0, 1.0))
    norm = norm ** power
    return float(floor + norm * (ceiling - floor))


# Final guardrail bands for funnel-stage rates after all vendor / story / personality effects.
# These keep vendor separation visible without letting top vendors run unrealistically hot.
FINAL_RATE_BANDS = {
    1: {  # ILS — ceiling raised to separate from Search clearly
        "ctr": (0.020, 0.155),
        "c2v": (0.06, 0.42),
        "v2l": (0.08, 0.40),
        "l2l": (0.055, 0.130),  # ILS ceiling 0.130 — clearly above Search
    },
    2: {  # Paid Search — ceiling below ILS, above Social
        "ctr": (0.010, 0.095),
        "c2v": (0.015, 0.20),
        "v2l": (0.10, 0.50),
        "l2l": (0.035, 0.095),  # Search ceiling 0.095
    },
    3: {  # Social — low ceiling, awareness channel
        "ctr": (0.005, 0.055),
        "c2v": (0.010, 0.12),
        "v2l": (0.04, 0.24),
        "l2l": (0.012, 0.055),  # Social ceiling 0.055
    },
    4: {  # Display — lowest ceiling, top-of-funnel only
        "ctr": (0.001, 0.020),
        "c2v": (0.002, 0.05),
        "v2l": (0.04, 0.26),
        "l2l": (0.003, 0.025),  # Display ceiling 0.025 — clear laggard
    },
    5: {  # Email
        "ctr": (0.008, 0.075),
        "c2v": (0.01, 0.10),
        "v2l": (0.12, 0.52),
        "l2l": (0.055, 0.130),
    },
}


def apply_final_rate_compression(channel_key: int, metric: str, rate: float) -> float:
    band = FINAL_RATE_BANDS.get(channel_key, {}).get(metric)
    if band is None:
        return float(rate)
    return compress_rate(float(rate), band[0], band[1], power=1.08)

def apply_tier_performance_cap(tier: str, metric: str, value: float) -> float:
    """Apply a hard ceiling for top-tier properties on final funnel conversion outputs."""
    if tier == "Star":
        cap = float(PERFORMANCE_TIERS.get("Star", {}).get("performance_cap", 1.0))
        if metric in {"l2l", "v2l", "c2v", "ctr"}:
            return min(float(value), cap)
    return float(value)


def apply_long_run_conv_stability(tier: str, market_strength: str, metric: str, rate: float) -> float:
    """
    Gentle multiplicative stability for probability metrics.
    Keeps vendor/property differences alive without forcing rates toward 1.0.

    CRITICAL FIX: Previous version used additive anchor around 1.0 with absolute
    floor of 0.60. That is mathematically wrong for probability metrics like l2l=0.20
    because it forced them up to 0.60+ before compression collapsed them to the band
    ceiling — making base cuts and vendor biases have no visible effect.

    This version works RELATIVE to the rate itself (multiplicative), so:
    - lowering base l2l actually lowers output
    - vendor spread survives to the final rates
    - Email stops pinning near the compression ceiling
    """
    tier_mult = {
        "Star":      1.03,
        "Good":      1.01,
        "Average":   0.99,
        "Struggler": 0.96,
    }
    market_mult = {
        "Strong": 1.02,
        "Mid":    1.00,
        "Weak":   0.98,
    }

    stabilized = float(rate) * tier_mult.get(tier, 1.0) * market_mult.get(market_strength, 1.0)

    # Bands are now relative to the rate itself — not absolute values
    stabilizer_bands = {
        "ctr": (0.50, 1.20),
        "c2v": (0.60, 1.20),
        "v2l": (0.60, 1.20),
        "l2l": (0.60, 1.20),
    }

    low, high = stabilizer_bands.get(metric, (0.60, 1.20))
    return float(np.clip(stabilized, float(rate) * low, float(rate) * high))



# ─────────────────────────────────────────────────────────────────────
# VENDOR PERSONALITY ENGINE
# Seeded per (property_key, vendor_key) → deterministic 2-year drift
# ─────────────────────────────────────────────────────────────────────

def build_vendor_performance_matrix(dim_property: pd.DataFrame, dim_vendor: pd.DataFrame) -> dict:
    """
    For every (property, vendor) pair, generate a seeded performance profile
    that determines how that vendor's conversion rates drift over 2024-2025.

    Returns:
        vendor_perf[property_key][vendor_key] = {
            "spend_share_drift": float   # +/- from base share, clamped
            "conv_mult_2024h1":  float
            "conv_mult_2024h2":  float
            "conv_mult_2025h1":  float
            "conv_mult_2025h2":  float
        }
    """
    vendor_perf = {}

    vendor_channel = dim_vendor.set_index("VendorKey")["ChannelKey"].to_dict()

    for _, prop in dim_property.iterrows():
        pk = int(prop["PropertyKey"])
        vendor_perf[pk] = {}

        for _, vend in dim_vendor.iterrows():
            vk = int(vend["VendorKey"])
            ck = int(vendor_channel[vk])

            # Seed is deterministic: property × vendor
            prng = np.random.default_rng(seed=(pk * 9973 + vk * 31) % (2**32))

            # Trend direction for this vendor at this property (persistent over 2 years)
            # -1 = declining, 0 = stable, +1 = rising
            trend = prng.choice([-1, 0, 1], p=[0.25, 0.50, 0.25])
            trend_strength = float(prng.uniform(0.04, 0.14))  # widened: 4-14% per half-year

            # Starting multiplier with wider property-level noise
            start_mult = float(prng.uniform(0.88, 1.14))

            # Build half-year multipliers with wider spread and clip
            mults = [start_mult]
            for _ in range(3):
                delta = trend * trend_strength + float(prng.uniform(-0.05, 0.05))
                mults.append(float(np.clip(mults[-1] + delta, 0.68, 1.38)))

            # Spend share drift: widened so vendor volume separates more clearly
            share_drift = float(prng.uniform(-0.12, 0.12))

            # ILS-specific: create real spread between vendors
            if ck == 1:
                if vk == 1:   # Zillow: clear leader
                    mults = [m * float(prng.uniform(1.06, 1.16)) for m in mults]
                    share_drift = float(prng.uniform(0.02, 0.12))
                elif vk == 2:  # Apartments.com: strong mid
                    mults = [m * float(prng.uniform(0.98, 1.06)) for m in mults]
                    share_drift = float(prng.uniform(-0.04, 0.06))
                elif vk == 3:  # Apartment List: clearly weaker
                    mults = [m * float(prng.uniform(0.78, 0.92)) for m in mults]
                    share_drift = float(prng.uniform(-0.14, -0.02))

            # Search-specific: Google should separate from Bing
            if ck == 2:
                if vk == 4:   # Google Ads: dominant
                    mults = [m * float(prng.uniform(1.04, 1.14)) for m in mults]
                    share_drift = float(prng.uniform(0.00, 0.08))
                elif vk == 5:  # Bing Ads: lower efficiency
                    mults = [m * float(prng.uniform(0.90, 1.02)) for m in mults]
                    share_drift = float(prng.uniform(-0.08, 0.02))

            # Social: modest but visible separation
            if ck == 3:
                if vk == 6:   # Facebook: slightly weaker intent
                    mults = [m * float(prng.uniform(0.88, 1.00)) for m in mults]
                elif vk == 7:  # Instagram: slightly stronger engagement
                    mults = [m * float(prng.uniform(0.96, 1.08)) for m in mults]

            # Display: strongest spread — clear ranking
            if ck == 4:
                if vk == 8:   # Google Display: stable leader
                    mults = [m * float(prng.uniform(1.00, 1.10)) for m in mults]
                    share_drift = float(prng.uniform(0.00, 0.10))
                elif vk == 9:  # StackAdapt: moderate
                    mults = [m * float(prng.uniform(0.88, 1.00)) for m in mults]
                    share_drift = float(prng.uniform(-0.08, 0.02))
                elif vk == 10:  # TradeDesk: weakest, most volatile
                    mults = [m * float(prng.uniform(0.72, 0.90)) for m in mults]
                    share_drift = float(prng.uniform(-0.14, -0.02))

            # Email: elite conversion signal preserved
            if ck == 5 and vk == 11:
                mults = [m * float(prng.uniform(1.08, 1.18)) for m in mults]

            vendor_perf[pk][vk] = {
                "conv_mult_2024h1": mults[0],
                "conv_mult_2024h2": mults[1],
                "conv_mult_2025h1": mults[2],
                "conv_mult_2025h2": mults[3],
                "spend_share_drift": share_drift,
            }

    return vendor_perf


def get_vendor_conv_mult(vendor_perf: dict, property_key: int,
                          vendor_key: int, year: int, month: int) -> float:
    profile = vendor_perf.get(property_key, {}).get(vendor_key, None)
    if profile is None:
        return 1.0
    if year == 2024 and month <= 6:
        raw_mult = profile["conv_mult_2024h1"]
    elif year == 2024:
        raw_mult = profile["conv_mult_2024h2"]
    elif year == 2025 and month <= 6:
        raw_mult = profile["conv_mult_2025h1"]
    else:
        raw_mult = profile["conv_mult_2025h2"]

    # Blend vendor drift toward 1.0 using 0.95 (was 0.82).
    # 0.82 was killing 18% of variance on every retrieval call, collapsing
    # vendor differentiation to channel means. 0.95 only softens 5%.
    # Clamp widened from (0.78, 1.24) to (0.70, 1.34) to match personality range.
    blended = 1.0 + (float(raw_mult) - 1.0) * 0.95
    return float(np.clip(blended, 0.70, 1.34))


def get_vendor_metric_bias(vendor_key: int, metric: str) -> float:
    return float(VENDOR_CONVERSION_BIAS.get(vendor_key, {}).get(metric, 1.0))


def get_vendor_spend_shares(vendor_perf: dict, property_key: int,
                             channel_key: int, base_shares: dict,
                             year: int, month: int,
                             story_override: dict | None = None) -> dict:
    """
    Return vendor spend share dict for a channel at a property on a given date.
    Applies story override first, then property-level drift, then normalises to 1.0.
    Floor: no vendor goes below 5% of its original base share.
    """
    if story_override is not None:
        return story_override

    raw = {}
    for vk, base_share in base_shares.items():
        drift = vendor_perf.get(property_key, {}).get(vk, {}).get("spend_share_drift", 0.0)
        # Drift scales in over time (gradual)
        months_elapsed = (year - 2024) * 12 + month  # 1-24
        applied_drift = drift * (months_elapsed / 24.0)
        new_share = base_share + applied_drift
        floor = base_share * 0.05
        raw[vk] = max(new_share, floor)

    total = sum(raw.values())
    return {vk: v / total for vk, v in raw.items()}


# ─────────────────────────────────────────────────────────────────────
# BUDGET REALLOCATION ENGINE
# Tracks per-property monthly budget state; underperformers cut gradually
# ─────────────────────────────────────────────────────────────────────

def build_budget_state(property_key: int, base_monthly_budget: float) -> dict:
    """
    Initial budget state for a property.
    channel_alloc: current fraction of total budget per channel (mirrors CHANNEL_SPEND_MIX)
    """
    return {
        "base": base_monthly_budget,
        "channel_alloc": dict(CHANNEL_SPEND_MIX),  # copy
        "history": [],  # list of (year, month, channel_key, actual_spend, cpl)
    }


def update_budget_state(state: dict, year: int, month: int,
                         channel_cpl: dict) -> dict:
    """
    At end of each month, rebalance channel allocations based on CPL performance.
    - Channels with CPL > portfolio avg * 1.20 → cut 8-12% of their allocation
    - Floor per channel: 5% of original CHANNEL_SPEND_MIX
    - Overage redistributed to best-performing channel (not spread)
    - Max total budget drift: ±5% from base
    Returns updated state.
    """
    if not channel_cpl:
        return state

    alloc = dict(state["channel_alloc"])
    floors = {ck: CHANNEL_SPEND_MIX[ck] * 0.05 for ck in CHANNEL_SPEND_MIX}

    avg_cpl = np.mean(list(channel_cpl.values()))

    cut_total = 0.0
    best_channel = min(channel_cpl, key=channel_cpl.get)

    for ck, cpl in channel_cpl.items():
        if ck not in alloc:
            continue
        if cpl > avg_cpl * 1.20:
            cut_pct = random.uniform(0.08, 0.12)
            cut_amount = alloc[ck] * cut_pct
            new_alloc = max(alloc[ck] - cut_amount, floors.get(ck, 0.01))
            actual_cut = alloc[ck] - new_alloc
            alloc[ck] = new_alloc
            cut_total += actual_cut

    # Redistribute to best channel (cap at +15% of its current alloc)
    if cut_total > 0 and best_channel in alloc:
        cap = alloc[best_channel] * 0.15
        alloc[best_channel] = min(alloc[best_channel] + cut_total, alloc[best_channel] + cap)

    # Renormalise to sum=1
    total = sum(alloc.values())
    state["channel_alloc"] = {ck: v / total for ck, v in alloc.items()}
    return state


# ─────────────────────────────────────────────────────────────────────
# DIMENSION BUILDERS  (not written to SQL — read only for fact generation)
# ─────────────────────────────────────────────────────────────────────

def build_dim_date() -> pd.DataFrame:
    print("Building dim_date...")
    rows = []
    current_date = DATE_START
    while current_date <= DATE_END:
        rows.append({
            "DateKey":        int(current_date.strftime("%Y%m%d")),
            "Date":           current_date.isoformat(),
            "Year":           current_date.year,
            "Quarter":        (current_date.month - 1) // 3 + 1,
            "MonthNumber":    current_date.month,
            "MonthName":      current_date.strftime("%B"),
            "WeekOfYear":     int(current_date.strftime("%W")),
            "DayOfWeekNumber": current_date.isoweekday(),
            "DayOfWeekName":  current_date.strftime("%A"),
            "IsWeekend":      1 if current_date.isoweekday() >= 6 else 0,
            "IsMonthEnd":     1 if current_date == get_month_end(current_date) else 0,
        })
        current_date += timedelta(days=1)
    df = pd.DataFrame(rows)
    print(f"  - {len(df):,} rows")
    return df


def build_dim_lease_date(dim_date: pd.DataFrame) -> pd.DataFrame:
    """
    Lease date dimension — identical structure to dim_date, column renamed
    DateKey → LeaseDateKey. Provides a proper active relationship anchor
    for fact_prospect_journey[LeaseDateKey] in Power BI.

    Relationship to create in Power BI:
      dim_lease_date[LeaseDateKey] → fact_prospect_journey[LeaseDateKey]
      (Active, many-to-one)

    Treated as a protected dimension — never cleared by clear_sql_tables().
    """
    df = dim_date.copy()
    df = df.rename(columns={"DateKey": "LeaseDateKey"})
    print(f"  - dim_lease_date: {len(df):,} rows")
    return df


def build_dim_region() -> pd.DataFrame:
    return pd.DataFrame([
        {"RegionKey": 1, "RegionName": "East"},
        {"RegionKey": 2, "RegionName": "Central"},
        {"RegionKey": 3, "RegionName": "West"},
        {"RegionKey": 4, "RegionName": "South"},
    ])


def build_dim_market() -> pd.DataFrame:
    market_names = {
        1: ["Northeast Corridor", "Mid-Atlantic", "New England"],
        2: ["Great Lakes", "Midwest Plains", "Upper Midwest"],
        3: ["Pacific Coast", "Mountain West", "Southwest"],
        4: ["Southeast", "Gulf Coast", "Sun Belt"],
    }
    rows = []
    market_key = 1
    for region_key, names in market_names.items():
        for market_name in names:
            rows.append({"MarketKey": market_key, "MarketName": market_name, "RegionKey": region_key})
            market_key += 1
    return pd.DataFrame(rows)


def build_dim_vendor() -> pd.DataFrame:
    return pd.DataFrame([
        {"VendorKey": 1,  "VendorName": "Zillow",          "ChannelKey": 1},
        {"VendorKey": 2,  "VendorName": "Apartments.com",  "ChannelKey": 1},
        {"VendorKey": 3,  "VendorName": "Apartment List",  "ChannelKey": 1},
        {"VendorKey": 4,  "VendorName": "Google Ads",      "ChannelKey": 2},
        {"VendorKey": 5,  "VendorName": "Bing Ads",        "ChannelKey": 2},
        {"VendorKey": 6,  "VendorName": "Facebook",        "ChannelKey": 3},
        {"VendorKey": 7,  "VendorName": "Instagram",       "ChannelKey": 3},
        {"VendorKey": 8,  "VendorName": "Google Display",  "ChannelKey": 4},
        {"VendorKey": 9,  "VendorName": "StackAdapt",      "ChannelKey": 4},
        {"VendorKey": 10, "VendorName": "TradeDesk",       "ChannelKey": 4},
        {"VendorKey": 11, "VendorName": "Email Marketing", "ChannelKey": 5},
    ])


def _assign_property_tiers_from_market_map(dim_property: pd.DataFrame) -> pd.DataFrame:
    """
    Assign PerformanceTier from MARKET_TIER_MAP using the property order within each market.
    Assumes the exported seed table is the source of truth for properties and units.
    """
    df = dim_property.copy()
    df["PerformanceTier"] = None

    for market_name, group_idx in df.groupby("MarketName", sort=False).groups.items():
        ordered_idx = (
            df.loc[list(group_idx)]
            .sort_values(["PropertyKey", "PropertyName"])
            .index
            .tolist()
        )
        tier_template = MARKET_TIER_MAP.get(market_name)
        if not tier_template:
            assigned = ["Average"] * len(ordered_idx)
        elif len(tier_template) == len(ordered_idx):
            assigned = tier_template
        else:
            repeats = math.ceil(len(ordered_idx) / len(tier_template))
            assigned = (tier_template * repeats)[:len(ordered_idx)]

        for idx, tier in zip(ordered_idx, assigned):
            df.at[idx, "PerformanceTier"] = tier

    return df


def _add_internal_property_behavior_cols(dim_property: pd.DataFrame) -> pd.DataFrame:
    """
    Recreate the internal, in-memory only behavior columns that v6 fact builders expect.
    These columns are deterministic by PropertyKey so repeated runs are stable.
    """
    df = dim_property.copy()
    df["MarketStrength"] = df["MarketName"].map(MARKET_STRENGTH).fillna("Mid")

    peak_shift_vals = []
    volatility_vals = []
    lead_quality_vals = []
    anomaly_months_vals = []
    spend_efficiency_vals = []

    for property_key in df["PropertyKey"].astype(int):
        prng = np.random.default_rng(seed=int(property_key) * 7919)
        peak_shift_vals.append(int(prng.integers(-2, 3)))
        volatility_vals.append(prng.choice(["stable", "moderate", "volatile"], p=[0.40, 0.40, 0.20]))
        lead_quality_vals.append(prng.choice(["high_vol_low_conv", "balanced", "low_vol_high_conv"], p=[0.30, 0.50, 0.20]))
        anomaly_months_vals.append(sorted(prng.choice(range(1, 13), size=int(prng.integers(0, 2)), replace=False).tolist()))
        spend_efficiency_vals.append(float(round(prng.uniform(0.80, 1.20), 4)))

    df["PeakShift"] = peak_shift_vals
    df["Volatility"] = volatility_vals
    df["LeadQuality"] = lead_quality_vals
    df["AnomalyMonths"] = anomaly_months_vals
    df["SpendEfficiency"] = spend_efficiency_vals
    return df


def load_dim_property_seed(dim_market: pd.DataFrame, path: str = PROPERTY_SEED_PATH) -> pd.DataFrame:
    """
    Load the exported property seed table instead of regenerating dim_property.
    This keeps PropertyName, MarketKey, RegionKey, and TotalUnits stable across runs.
    """
    print(f"Loading dim_property seed from {path}...")
    df = pd.read_csv(path)

    required_cols = [
        "PropertyKey", "PropertyName", "MarketKey", "RegionKey", "TotalUnits"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"dim_property seed missing required columns: {missing}")

    # Normalize common exported column names from the user's seed file.
    rename_map = {}
    if "PropertyState" in df.columns and "State" not in df.columns:
        rename_map["PropertyState"] = "State"
    if "PropertyCity" in df.columns and "City" not in df.columns:
        rename_map["PropertyCity"] = "City"
    df = df.rename(columns=rename_map)

    # Restore / validate MarketName from dim_market if needed.
    market_lookup = dim_market[["MarketKey", "MarketName", "RegionKey"]].drop_duplicates()
    if "MarketName" not in df.columns:
        df = df.merge(market_lookup[["MarketKey", "MarketName"]], on="MarketKey", how="left")
    else:
        df = df.merge(
            market_lookup.rename(columns={"MarketName": "_ExpectedMarketName", "RegionKey": "_ExpectedRegionKey"}),
            on="MarketKey",
            how="left"
        )
        if df["_ExpectedMarketName"].notna().any():
            bad_market_name = df.loc[
                df["_ExpectedMarketName"].notna() & (df["MarketName"] != df["_ExpectedMarketName"])
            ]
            if not bad_market_name.empty:
                raise ValueError(
                    "dim_property seed has MarketName values that do not match MarketKey for rows: "
                    + ", ".join(map(str, bad_market_name["PropertyKey"].head(10).tolist()))
                )
            bad_region = df.loc[
                df["_ExpectedRegionKey"].notna() & (df["RegionKey"].astype(int) != df["_ExpectedRegionKey"].astype(int))
            ]
            if not bad_region.empty:
                raise ValueError(
                    "dim_property seed has RegionKey values that do not match MarketKey for rows: "
                    + ", ".join(map(str, bad_region["PropertyKey"].head(10).tolist()))
                )
            df = df.drop(columns=["_ExpectedMarketName", "_ExpectedRegionKey"])

    int_cols = ["PropertyKey", "MarketKey", "RegionKey", "TotalUnits"]
    for col in int_cols:
        df[col] = df[col].astype(int)

    if df["PropertyKey"].duplicated().any():
        dupes = df.loc[df["PropertyKey"].duplicated(), "PropertyKey"].tolist()
        raise ValueError(f"dim_property seed contains duplicate PropertyKey values: {dupes[:10]}")
    if (df["TotalUnits"] <= 0).any():
        raise ValueError("dim_property seed contains non-positive TotalUnits values")

    df = df.sort_values(["MarketKey", "PropertyKey"]).reset_index(drop=True)
    df = _assign_property_tiers_from_market_map(df)
    df = _add_internal_property_behavior_cols(df)

    print(f"  - {len(df):,} properties loaded from seed")
    print(f"  - Tier dist: {df['PerformanceTier'].value_counts().to_dict()}")
    print(f"  - Market strength dist: {df['MarketStrength'].value_counts().to_dict()}")
    return df


def resolve_dim_property(dim_market: pd.DataFrame, seed_path: str = PROPERTY_SEED_PATH) -> pd.DataFrame:
    """
    Prefer the on-disk seed when present, but fall back to synthetic property generation.
    This prevents CSV mode from crashing when dim.property.csv is not available.
    """
    if seed_path and os.path.exists(seed_path):
        return load_dim_property_seed(dim_market, seed_path)
    print(f"Property seed not found at {seed_path}; generating dim_property in-memory instead...")
    return build_dim_property(dim_market)




def build_dim_property(dim_market: pd.DataFrame) -> pd.DataFrame:
    print("Building dim_property...")

    cities = {
        "Northeast Corridor": ("New York", "NY"),
        "Mid-Atlantic":       ("Philadelphia", "PA"),
        "New England":        ("Boston", "MA"),
        "Great Lakes":        ("Chicago", "IL"),
        "Midwest Plains":     ("Columbus", "OH"),
        "Upper Midwest":      ("Minneapolis", "MN"),
        "Pacific Coast":      ("Los Angeles", "CA"),
        "Mountain West":      ("Denver", "CO"),
        "Southwest":          ("Phoenix", "AZ"),
        "Southeast":          ("Atlanta", "GA"),
        "Gulf Coast":         ("Houston", "TX"),
        "Sun Belt":           ("Charlotte", "NC"),
    }
    coords = {
        "New York": (40.73, -73.99), "Philadelphia": (39.95, -75.16),
        "Boston":   (42.36, -71.06), "Chicago":      (41.88, -87.63),
        "Columbus": (39.96, -82.99), "Minneapolis":  (44.98, -93.27),
        "Los Angeles": (34.05, -118.24), "Denver":   (39.74, -104.99),
        "Phoenix":  (33.45, -112.07), "Atlanta":     (33.75, -84.39),
        "Houston":  (29.76, -95.37), "Charlotte":   (35.23, -80.84),
    }
    street_types = ["St", "Ave", "Blvd", "Dr", "Ln", "Way", "Pl", "Ct"]
    property_prefix = ["The","Grand","Park","River","Loft at","Reserve at",
                        "The District at","Residences at","The Grove at","Villas at"]
    property_suffix = ["Terrace","Commons","Heights","Crossing","Place",
                        "Park","Square","Pointe","Landing","Walk"]

    rows = []
    property_key = 1

    for market in dim_market.to_dict("records"):
        market_key  = market["MarketKey"]
        market_name = market["MarketName"]
        region_key  = market["RegionKey"]
        city, state = cities[market_name]
        base_lat, base_lon = coords[city]
        zips = [str(random.randint(10000, 99999)).zfill(5) for _ in range(10)]
        market_tiers = MARKET_TIER_MAP.get(market_name, ["Average"] * 10)
        market_strength = MARKET_STRENGTH.get(market_name, "Mid")

        for i in range(10):
            tier = market_tiers[i]
            total_units = int(np.clip(
                np.random.normal(PROPERTY_SIZE["mean"], PROPERTY_SIZE["std"]),
                PROPERTY_SIZE["min"], PROPERTY_SIZE["max"]
            ))
            total_units = round(total_units / 5) * 5

            property_name  = f"{random.choice(property_prefix)} {random.choice(property_suffix)}"
            street_address = (
                f"{random.randint(100, 9999)} "
                f"{random.choice(['Main','Oak','Maple','Elm','Cedar','Park','Lake','River'])} "
                f"{random.choice(street_types)}"
            )

            prng = np.random.default_rng(seed=property_key * 7919)
            peak_shift      = int(prng.integers(-2, 3))
            volatility      = prng.choice(["stable","moderate","volatile"], p=[0.40, 0.40, 0.20])
            lead_quality    = prng.choice(["high_vol_low_conv","balanced","low_vol_high_conv"], p=[0.30, 0.50, 0.20])
            anomaly_months  = sorted(prng.choice(range(1, 13), size=int(prng.integers(0, 2)), replace=False).tolist())
            spend_efficiency = float(round(prng.uniform(0.80, 1.20), 4))

            rows.append({
                "PropertyKey":      property_key,
                "PropertyName":     property_name,
                "MarketKey":        market_key,
                "MarketName":       market_name,
                "RegionKey":        region_key,
                "State":            state,
                "City":             city,
                "StreetAddress":    street_address,
                "Zip":              zips[i],
                "Latitude":         round(base_lat + np.random.uniform(-0.15, 0.15), 6),
                "Longitude":        round(base_lon + np.random.uniform(-0.15, 0.15), 6),
                "TotalUnits":       total_units,
                "IsActive":         1,
                # In-memory only:
                "PerformanceTier":  tier,
                "MarketStrength":   market_strength,
                "PeakShift":        peak_shift,
                "Volatility":       volatility,
                "LeadQuality":      lead_quality,
                "AnomalyMonths":    anomaly_months,
                "SpendEfficiency":  spend_efficiency,
            })
            property_key += 1

    df = pd.DataFrame(rows)
    print(f"  - {len(df):,} properties | Tier dist: {df['PerformanceTier'].value_counts().to_dict()}")
    print(f"  - Market strength dist: {df['MarketStrength'].value_counts().to_dict()}")
    return df


# ─────────────────────────────────────────────────────────────────────
# FACT: PROPERTY OPS DAILY
# ─────────────────────────────────────────────────────────────────────


def build_fact_property_ops_daily(
    dim_property: pd.DataFrame,
    dim_date: pd.DataFrame,
    leasing_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Build fact_property_ops_daily.

    Normalisation notes:
      - When leasing_df is supplied, MoveIns is forced to equal NewLeases exactly.
      - MoveOuts are then solved inside the occupancy band so OccupiedUnits never
        exceeds TotalUnits and MoveIns never needs to be clipped away.
      - Coverage ratio is boosted for Star / Good properties so strong assets can
        legitimately exceed 1.0 and reach the scoring ceiling.
    """
    print("Building fact_property_ops_daily...")

    leasing_lookup = {}
    if leasing_df is not None:
        leasing_lookup = (
            leasing_df.set_index(["DateKey", "PropertyKey"])["NewLeases"]
            .to_dict()
        )

    SEASONAL_OCC_DRIFT = {
        1: -0.025, 2: -0.020, 3: -0.005, 4: 0.010,
        5: 0.020,  6: 0.025,  7: 0.025,  8: 0.020,
        9: 0.010, 10: 0.000, 11:-0.010, 12:-0.020,
    }
    SEASONAL_RENEWAL = {
        1: 0.38, 2: 0.40, 3: 0.44, 4: 0.50,
        5: 0.56, 6: 0.60, 7: 0.60, 8: 0.58,
        9: 0.52, 10: 0.46, 11: 0.40, 12: 0.36,
    }
    COVERAGE_RENEWAL = {"Star": 0.92, "Good": 0.78, "Average": 0.62, "Struggler": 0.48}
    COVERAGE_VACANCY = {"Star": 1.10, "Good": 0.95, "Average": 0.75, "Struggler": 0.55}
    COVERAGE_FLOOR = {"Star": 1.08, "Good": 1.02, "Average": 0.92, "Struggler": 0.82}

    # v10 coverage cap fix:
    # renewal coverage should vary by tier, but row-level ratios should not blow out
    # above realistic operating ranges when LeaseExpirations_Next60D is small.
    COVERAGE_TARGET_BAND = {
        "Star":      (0.95, 1.15),
        "Good":      (0.90, 1.08),
        "Average":   (0.82, 1.00),
        "Struggler": (0.70, 0.92),
    }
    COVERAGE_HARD_CAP = {
        "Star": 1.25,
        "Good": 1.18,
        "Average": 1.08,
        "Struggler": 0.98,
    }

    avg_lease_days = sum(LEASE_TERMS[t] * LEASE_TERM_DAYS[t] for t in LEASE_TERMS)
    daily_turnover = 1.0 / avg_lease_days

    dim_date = dim_date.copy()
    dim_date["Date"] = pd.to_datetime(dim_date["Date"])
    date_info = dim_date.set_index("DateKey")[["Date", "MonthNumber"]].to_dict("index")

    all_rows = []

    for _, prop in dim_property.iterrows():
        property_key = int(prop["PropertyKey"])
        tier = prop["PerformanceTier"]
        region_key = int(prop["RegionKey"])
        total_units = int(prop["TotalUnits"])
        market_strength = prop.get("MarketStrength", "Mid")

        region_name = get_region_name(region_key)
        tier_cfg = PERFORMANCE_TIERS[tier]
        reg_delta = REGION_OCC_DELTA[region_name]
        ms_delta = MARKET_STRENGTH_MULT[market_strength]["occ_delta"]
        occ_floor, occ_ceiling = TIER_OCC_BAND[tier]

        min_occ_units = int(math.floor(occ_floor * total_units))
        max_occ_units = min(int(math.ceil(occ_ceiling * total_units)), total_units)

        annual_base_occ = float(np.clip(
            tier_cfg["occ_base"] + reg_delta + ms_delta,
            occ_floor, occ_ceiling
        ))
        current_occ_rate = float(np.clip(
            np.random.normal(annual_base_occ, 0.004),
            occ_floor, occ_ceiling
        ))
        occupied = int(np.clip(round(current_occ_rate * total_units), min_occ_units, max_occ_units))

        for date_key, info in sorted(date_info.items()):
            month = int(info["MonthNumber"])
            seasonal_renewal = SEASONAL_RENEWAL[month]

            monthly_target_occ = float(np.clip(
                annual_base_occ + SEASONAL_OCC_DRIFT[month],
                occ_floor, occ_ceiling
            ))
            target_occupied = int(np.clip(round(monthly_target_occ * total_units), min_occ_units, max_occ_units))

            moveins_today = int(leasing_lookup.get((date_key, property_key), 0)) if leasing_lookup else 0

            baseline_moveouts = occupied * daily_turnover * (1 - seasonal_renewal)
            baseline_moveouts = float(np.random.poisson(max(baseline_moveouts, 0.01)))

            required_moveouts_min = max(0, occupied + moveins_today - max_occ_units)
            required_moveouts_max = max(0, occupied + moveins_today - min_occ_units)

            drift_to_target = occupied + moveins_today - target_occupied
            solved_moveouts = max(baseline_moveouts, drift_to_target * 0.70)
            solved_moveouts = int(round(solved_moveouts))

            if required_moveouts_max <= 0:
                moveouts_today = 0
            else:
                moveouts_today = int(np.clip(solved_moveouts, required_moveouts_min, min(required_moveouts_max, occupied)))

            occupied = int(np.clip(
                occupied - moveouts_today + moveins_today,
                min_occ_units,
                max_occ_units
            ))

            vacant = max(0, total_units - occupied)
            available = max(0, vacant - random.randint(0, max(1, vacant // 6)))

            expirations_today = int(round(moveouts_today / max(1 - seasonal_renewal, 0.01)))

            days_ahead = 60
            exp_next60 = round(occupied * daily_turnover * days_ahead, 1)

            coverage_base = (
                exp_next60 * COVERAGE_RENEWAL.get(tier, RENEWAL_RATE)
                + vacant * COVERAGE_VACANCY.get(tier, 0.55)
                + max(0, target_occupied - occupied) * 0.25
            )
            coverage_floor = exp_next60 * COVERAGE_FLOOR.get(tier, 0.90)

            # Convert raw scheduled move-ins into a bounded renewal-coverage target.
            # This prevents small-denominator rows from producing unrealistic spikes.
            base_ratio = max(coverage_base, coverage_floor) / max(exp_next60, 1.0)
            band_low, band_high = COVERAGE_TARGET_BAND.get(tier, (0.82, 1.00))
            target_ratio = float(np.clip(base_ratio, band_low, band_high))

            # Small day-level noise keeps the metric from looking artificially flat.
            target_ratio *= float(np.random.uniform(0.97, 1.03))

            hard_cap = COVERAGE_HARD_CAP.get(tier, 1.10)
            target_ratio = float(np.clip(target_ratio, 0.0, hard_cap))
            sch_next60 = round(min(exp_next60 * target_ratio, exp_next60 * hard_cap), 1)

            all_rows.append({
                "DateKey": date_key,
                "PropertyKey": property_key,
                "OccupiedUnits": occupied,
                "VacantUnits": vacant,
                "AvailableUnits": available,
                "MoveIns": moveins_today,
                "MoveOuts": moveouts_today,
                "LeaseExpirations": expirations_today,
                "ScheduledMoveIns": min(
                    moveins_today + random.randint(0, 2),
                    max(available, moveins_today)
                ),
                "LeaseExpirations_Next60D": float(exp_next60),
                "ScheduledMoveIns_Next60D": float(sch_next60),
            })

    df = pd.DataFrame(all_rows)
    occ_avg = df.groupby("PropertyKey").apply(lambda x: x["OccupiedUnits"].mean()).mean()
    print(f"  - {len(df):,} rows | Avg occupancy across all properties: {occ_avg:.1f} units")
    return df


def validate_fact_property_ops_daily(fact_ops: pd.DataFrame, dim_property: pd.DataFrame) -> None:
    """
    Guardrail to catch impossible occupancy after generation.
    """
    merged = fact_ops.merge(
        dim_property[["PropertyKey", "TotalUnits"]],
        on="PropertyKey",
        how="left",
        validate="many_to_one"
    )
    bad = merged.loc[merged["OccupiedUnits"] > merged["TotalUnits"]]
    if not bad.empty:
        sample = bad[["DateKey", "PropertyKey", "OccupiedUnits", "TotalUnits"]].head(10).to_dict("records")
        raise ValueError(f"Generated invalid occupancy rows where OccupiedUnits > TotalUnits: {sample}")




# ─────────────────────────────────────────────────────────────────────
# FACT: LEASING DAILY
# ─────────────────────────────────────────────────────────────────────


def build_fact_leasing_daily(
    dim_property: pd.DataFrame,
    dim_date: pd.DataFrame,
    attributed_leases: dict = None,
) -> pd.DataFrame:
    """
    Build fact_leasing_daily.

    v8: accepts attributed_leases dict from build_fact_prospect_journey():
      {(property_key, date_key): attributed_count}

    Attributed leases remain a subset of operational NewLeases, but a controlled
    organic / dark-funnel component keeps attribution coverage in the target band.

    Two new columns:
      AttributedNewLeases  — leases with a tracked digital journey
      UnattributedLeases   — NewLeases - AttributedNewLeases
                             (walk-ins, referrals, renewals, untracked)
    """
    print("Building fact_leasing_daily...")

    attr_lookup = attributed_leases if attributed_leases is not None else {}

    dow_mult = {1: 1.08, 2: 1.12, 3: 1.10, 4: 1.08, 5: 1.04, 6: 0.82, 7: 0.76}
    seasonal_renewal = {
        1: 0.38, 2: 0.40, 3: 0.44, 4: 0.50,
        5: 0.56, 6: 0.60, 7: 0.60, 8: 0.58,
        9: 0.52, 10: 0.46, 11: 0.40, 12: 0.36,
    }
    lease_velocity_floor = {"Star": 0.28, "Good": 0.21, "Average": 0.15, "Struggler": 0.10}
    avg_lease_days = sum(LEASE_TERMS[t] * LEASE_TERM_DAYS[t] for t in LEASE_TERMS)
    daily_turnover = 1.0 / avg_lease_days

    all_rows = []

    for _, prop in dim_property.iterrows():
        property_key = int(prop["PropertyKey"])
        tier = prop["PerformanceTier"]
        region_key = int(prop["RegionKey"])
        total_units = int(prop["TotalUnits"])
        market_strength = prop.get("MarketStrength", "Mid")
        tier_cfg = PERFORMANCE_TIERS[tier]
        funnel_cfg = FUNNEL_RATES[tier]

        peak_shift = int(prop.get("PeakShift", 0))
        volatility = prop.get("Volatility", "moderate")
        lead_quality = prop.get("LeadQuality", "balanced")
        anomaly_months = prop.get("AnomalyMonths", [])

        vol_noise = {"stable": 0.04, "moderate": 0.12, "volatile": 0.20}[volatility]
        lq_lead_mult = {"high_vol_low_conv": 1.18, "balanced": 1.00, "low_vol_high_conv": 0.84}[lead_quality]
        lq_conv_mult = {"high_vol_low_conv": 0.82, "balanced": 1.00, "low_vol_high_conv": 1.18}[lead_quality]

        ms_lead_mult = MARKET_STRENGTH_MULT[market_strength]["lead_mult"]
        ms_conv_mult = MARKET_STRENGTH_MULT[market_strength]["conv_mult"]
        occ_floor, occ_ceiling = TIER_OCC_BAND[tier]
        min_occ_units = int(math.floor(occ_floor * total_units))
        max_occ_units = min(int(math.ceil(occ_ceiling * total_units)), total_units)

        annual_base_occ = float(np.clip(
            tier_cfg["occ_base"] + REGION_OCC_DELTA[get_region_name(region_key)] + MARKET_STRENGTH_MULT[market_strength]["occ_delta"],
            occ_floor, occ_ceiling
        ))
        est_occupied = int(np.clip(round(annual_base_occ * total_units), min_occ_units, max_occ_units))

        for _, drow in dim_date.iterrows():
            date_key = int(drow["DateKey"])
            month = int(drow["MonthNumber"])
            dow = int(drow["DayOfWeekNumber"])

            shifted_month = ((month - 1 + peak_shift) % 12) + 1
            seasonal_mult = get_seasonal_mult(shifted_month, region_key)

            if month in anomaly_months:
                seasonal_mult *= 0.35

            noise = np.random.uniform(1.0 - vol_noise, 1.0 + vol_noise)
            seasonal_mult_noisy = max(0.20, seasonal_mult * noise)

            peak_mid = (tier_cfg["lead_peak_min"] + tier_cfg["lead_peak_max"]) / 2
            offpeak_mid = (tier_cfg["lead_offpeak_min"] + tier_cfg["lead_offpeak_max"]) / 2

            monthly_leads = interpolate_between_seasonal_bounds(seasonal_mult_noisy, offpeak_mid, peak_mid)
            monthly_leads *= lq_lead_mult * ms_lead_mult

            daily_leads_base = monthly_leads / 30.0
            daily_leads_lambda = max(daily_leads_base * dow_mult.get(dow, 1.0), 0.05)
            daily_leads = int(np.random.poisson(daily_leads_lambda))

            adj_v2l = min(0.99, funnel_cfg["v2l"] * lq_conv_mult * ms_conv_mult)
            adj_l2l = min(0.99, funnel_cfg["l2l"] * lq_conv_mult * ms_conv_mult)

            visits = int(np.random.binomial(max(daily_leads, 1), adj_v2l)) if daily_leads > 0 else 0
            chain_leases = int(np.random.binomial(max(visits, 1), adj_l2l)) if visits > 0 else 0

            raw_expected_daily_leases = daily_leads_lambda * adj_v2l * adj_l2l
            floor_multiplier = 0.88 + (seasonal_mult_noisy - 0.60) * 0.35
            expected_daily_leases = max(raw_expected_daily_leases, lease_velocity_floor[tier] * floor_multiplier)

            est_moveouts = int(np.random.poisson(max(est_occupied * daily_turnover * (1 - seasonal_renewal[month]), 0.01)))
            vacant_after_est = max(0, total_units - (est_occupied - est_moveouts))

            if chain_leases > 0:
                new_leases = chain_leases
            else:
                draw = int(np.random.poisson(expected_daily_leases))
                if draw == 0 and expected_daily_leases >= 0.24 and np.random.random() < min(0.95, expected_daily_leases * 1.00):
                    draw = 1
                new_leases = draw

            attributed_today = int(attr_lookup.get((property_key, date_key), 0))

            organic_share = get_organic_lease_share(tier)
            organic_base = int(round(attributed_today * organic_share))

            # Reduced organic noise — 0.10 multiplier (was 0.25) so unattributed
            # leases don't over-inflate the denominator and suppress coverage.
            organic_noise = int(np.random.poisson(max(organic_base * 0.10, 0.2)))

            organic_today = organic_base + organic_noise

            new_leases = max(new_leases, attributed_today + organic_today)

            # Vacancy cap still applies, but we leave room for both attributed and organic demand.
            vacancy_cap = max(vacant_after_est, attributed_today + organic_today)
            new_leases = int(min(new_leases, vacancy_cap))

            # Hard daily lease cap based on realistic monthly turnover.
            MONTHLY_LEASE_RATE_CAP = {"Star": 0.12, "Good": 0.10, "Average": 0.08, "Struggler": 0.06}
            base_daily_cap = max(1, int(np.ceil(total_units * MONTHLY_LEASE_RATE_CAP[tier] / 30)))
            max_daily_leases = max(base_daily_cap, attributed_today + organic_today)
            new_leases = min(new_leases, max_daily_leases)

            # Final subset safety after all caps.
            attributed_today = min(attributed_today, new_leases)

            # Back-inflate visits and leads when leasing lands above chain output.
            # Use the tier's target L2L and V2L so the resulting ratio is realistic.
            # Old code used binomial(N, 0.35/0.45) which only inflated leads to ~2x leases
            # producing an implied L2L of ~50%. Now we back-calculate from targets.
            # v10 FIX: Lowered to match FUNNEL_RATES calibrated values.
            # Back-inflation uses these as the L2L/V2L floor for visit/lead
            # generation. Higher values were causing over-inflation of visits.
            TARGET_L2L_TIER = {"Star": 0.130, "Good": 0.100, "Average": 0.075, "Struggler": 0.048}
            TARGET_V2L_TIER = {"Star": 0.45,  "Good": 0.35,  "Average": 0.25,  "Struggler": 0.18}
            tgt_l2l = TARGET_L2L_TIER[tier]
            tgt_v2l = TARGET_V2L_TIER[tier]
            if new_leases > visits:
                min_visits = int(np.ceil(new_leases / max(adj_l2l, tgt_l2l)))
                visits = min_visits + int(np.random.poisson(max(min_visits * 0.15, 0.5)))
            if visits > daily_leads:
                min_leads = int(np.ceil(visits / max(adj_v2l, tgt_v2l)))
                daily_leads = min_leads + int(np.random.poisson(max(min_leads * 0.20, 0.5)))

            est_occupied = int(np.clip(est_occupied - est_moveouts + new_leases, min_occ_units, max_occ_units))

            unattributed_leases = int(max(0, new_leases - attributed_today))

            all_rows.append({
                "DateKey":              date_key,
                "PropertyKey":          property_key,
                "Leads":                int(daily_leads),
                "NewLeases":            int(new_leases),
                "Visits":               int(visits),
                "AttributedNewLeases":  int(attributed_today),
                "UnattributedLeases":   unattributed_leases,
            })

    df = pd.DataFrame(all_rows)
    df = rebalance_daily_attribution_capacity(df)

    bad_subset = df[df["AttributedNewLeases"] > df["NewLeases"]]
    bad_gap = df[
        df["UnattributedLeases"] !=
        (df["NewLeases"] - df["AttributedNewLeases"])
    ]

    if not bad_subset.empty:
        raise ValueError(
            f"Invalid leasing output: {len(bad_subset):,} rows where "
            "AttributedNewLeases > NewLeases"
        )

    if not bad_gap.empty:
        raise ValueError(
            f"Invalid leasing output: {len(bad_gap):,} rows where "
            "UnattributedLeases != NewLeases - AttributedNewLeases"
        )

    total_attr  = df["AttributedNewLeases"].sum()
    total_new   = df["NewLeases"].sum()
    coverage    = total_attr / total_new if total_new > 0 else 0
    print(f"  - {len(df):,} rows | Total leads: {df['Leads'].sum():,} | Total leases: {total_new:,}")
    print(f"  - AttributedNewLeases: {total_attr:,} | UnattributedLeases: {df['UnattributedLeases'].sum():,}")
    print(f"  - Attribution coverage: {coverage:.1%} (expected 60-80% — controlled organic gap enabled)")
    return df


def rebalance_daily_attribution_capacity(fact_lease: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce daily and monthly attribution feasibility inside each property-month.

    Rules:
      - AttributedNewLeases <= NewLeases on every daily row
      - Monthly attributed leases are preserved when there is spare daily capacity
      - If the month cannot absorb the original attributed total, the attributed
        total is reduced to the feasible maximum for that property-month
    """
    df = fact_lease.copy()
    if df.empty:
        return df

    df["YearMonth"] = (df["DateKey"].astype(int) // 100).astype(int)

    adjusted_groups = []
    for (_, _), grp in df.groupby(["PropertyKey", "YearMonth"], sort=False):
        grp = grp.sort_values("DateKey").copy()

        monthly_target_attr = int(grp["AttributedNewLeases"].sum())
        new_leases = grp["NewLeases"].astype(int).to_numpy(copy=True)
        attr = grp["AttributedNewLeases"].astype(int).to_numpy(copy=True)

        # First pass: hard daily clamp
        attr = np.minimum(attr, new_leases)

        remaining = int(monthly_target_attr - attr.sum())
        if remaining > 0:
            spare = (new_leases - attr).astype(int)

            # Greedy redistribution into days with highest spare capacity first.
            order = np.argsort(-spare)
            for idx in order:
                if remaining <= 0:
                    break
                room = int(spare[idx])
                if room <= 0:
                    continue
                add = min(room, remaining)
                attr[idx] += add
                remaining -= add

        # Final hard stop: feasible max is daily operational capacity.
        attr = np.minimum(attr, new_leases)

        grp["AttributedNewLeases"] = attr.astype(int)
        grp["UnattributedLeases"] = (grp["NewLeases"].astype(int) - grp["AttributedNewLeases"].astype(int)).clip(lower=0).astype(int)
        adjusted_groups.append(grp)

    out = pd.concat(adjusted_groups, ignore_index=True)
    out = out.drop(columns=["YearMonth"])
    return out


# ─────────────────────────────────────────────────────────────────────
# FACT: MARKETING SPEND DAILY
# ─────────────────────────────────────────────────────────────────────

def build_fact_marketing_spend_daily(
    dim_property: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_vendor: pd.DataFrame,
    vendor_perf: dict,
) -> pd.DataFrame:
    print("Building fact_marketing_spend_daily...")

    vendor_to_channel = dim_vendor.set_index("VendorKey")["ChannelKey"].to_dict()
    active_channels   = list(CHANNEL_SPEND_MIX.keys())  # [1,2,3,4,5]

    dow_spend_mult = {1: 1.10, 2: 1.12, 3: 1.10, 4: 1.08, 5: 1.05, 6: 0.78, 7: 0.77}

    annual_seasonal_normalizer = (sum(SEASONAL[m] for m in range(1, 13)) / 12) * 30

    all_rows = []

    # Per-property budget state for reallocation tracking
    budget_states = {}

    # Group dim_date by year-month for budget update cadence
    dim_date_copy = dim_date.copy()
    dim_date_copy["YearMonth"] = dim_date_copy["Year"].astype(str) + dim_date_copy["MonthNumber"].astype(str).str.zfill(2)
    ym_groups = dim_date_copy.groupby("YearMonth")

    # Build a fast lookup: DateKey → (Year, Month, DOW)
    date_lookup = dim_date.set_index("DateKey")[["Year","MonthNumber","DayOfWeekNumber"]].to_dict("index")

    for _, prop in dim_property.iterrows():
        property_key    = int(prop["PropertyKey"])
        tier            = prop["PerformanceTier"]
        region_key      = int(prop["RegionKey"])
        market_strength = prop.get("MarketStrength", "Mid")
        volatility      = prop.get("Volatility", "moderate")
        peak_shift      = int(prop.get("PeakShift", 0))
        anomaly_months  = prop.get("AnomalyMonths", [])
        spend_efficiency = float(prop.get("SpendEfficiency", 1.0))

        tier_cfg   = PERFORMANCE_TIERS[tier]
        region_mult = {1: 1.05, 2: 1.00, 3: 1.03, 4: 0.95}[region_key]
        ms_spend   = MARKET_STRENGTH_MULT[market_strength]["spend_mult"]
        vol_noise  = {"stable": 0.04, "moderate": 0.12, "volatile": 0.25}[volatility]

        is_strong = (market_strength == "Strong")

        base_monthly = (
            random.uniform(tier_cfg["spend_min"], tier_cfg["spend_max"])
            * region_mult * spend_efficiency * ms_spend
        )

        # Init budget state
        state = build_budget_state(property_key, base_monthly)
        budget_states[property_key] = state

        # Track monthly CPL for reallocation
        month_channel_spend   = {}   # (year,month,ck) → spend
        month_channel_leases  = {}   # (year,month,ck) → pseudo-lease count

        for date_key, dinfo in sorted(date_lookup.items()):
            year  = dinfo["Year"]
            month = dinfo["MonthNumber"]
            dow   = dinfo["DayOfWeekNumber"]

            shifted_month = ((month - 1 + peak_shift) % 12) + 1

            # Email uses compressed seasonal; others use regional curve
            noise = np.random.uniform(1.0 - vol_noise, 1.0 + vol_noise)

            for channel_key in active_channels:
                if channel_key == 5:
                    seasonal_mult = get_email_seasonal(shifted_month)
                else:
                    seasonal_mult = get_seasonal_mult(shifted_month, region_key)

                if month in anomaly_months:
                    seasonal_mult *= 0.30

                seasonal_mult *= noise

                # Story event: channel-level spend multiplier
                story_ch_mult = get_story_channel_mult(year, month, channel_key, is_strong)

                # Budget allocation from state (post-reallocation)
                ch_alloc = state["channel_alloc"].get(channel_key, CHANNEL_SPEND_MIX.get(channel_key, 0))

                # Story: redirect cut budget to realloc channel
                realloc_ch = get_story_realloc_channel(year, month, channel_key)
                bonus = 0.0
                if realloc_ch is not None:
                    cut_fraction = 1.0 - story_ch_mult
                    bonus_pool = base_monthly * CHANNEL_SPEND_MIX.get(channel_key, 0) * cut_fraction
                    if channel_key == realloc_ch:
                        bonus = bonus_pool

                channel_monthly = base_monthly * ch_alloc

                if channel_key == 1:  # ILS: flat fee, no daily seasonality
                    channel_daily = channel_monthly / 30
                else:
                    channel_daily = (
                        (channel_monthly / 30)
                        * seasonal_mult
                        * dow_spend_mult.get(dow, 1.0)
                        * (30 / annual_seasonal_normalizer)
                    )

                channel_daily = channel_daily * story_ch_mult + bonus / 30.0

                # Vendor shares (with drift + story overrides)
                story_override = get_story_vendor_shares(year, month, channel_key)
                base_shares    = VENDOR_CHANNEL_SHARE_BASE.get(channel_key, {})
                vendor_shares  = get_vendor_spend_shares(
                    vendor_perf, property_key, channel_key,
                    base_shares, year, month, story_override
                )

                for vendor_key, share in vendor_shares.items():
                    spend = channel_daily * share * np.random.uniform(0.92, 1.08)
                    spend = round(max(0.01, spend), 4)

                    all_rows.append({
                        "DateKey":     date_key,
                        "PropertyKey": property_key,
                        "VendorKey":   vendor_key,
                        "Spend":       spend,
                    })

                    # Accumulate for month-end CPL rebalance
                    key = (year, month, channel_key)
                    month_channel_spend[key] = month_channel_spend.get(key, 0.0) + spend

            # Month-end: update budget state
            # Detect when we've crossed a month boundary
            next_key = date_key + 1
            next_info = date_lookup.get(next_key, None)
            if next_info and next_info["MonthNumber"] != month:
                # Build CPL dict for this month
                channel_cpl = {}
                for ck in active_channels:
                    sp = month_channel_spend.get((year, month, ck), 0.0)
                    if sp > 0:
                        # Pseudo-CPL: spend / (spend * base_conv_rate) — relative measure
                        base_rate = CHANNEL_FUNNEL_RATES.get(ck, {}).get(tier, {}).get("l2l", 0.10)
                        pseudo_leases = sp * base_rate * 0.01  # keep units consistent
                        channel_cpl[ck] = sp / max(pseudo_leases, 0.001)
                state = update_budget_state(state, year, month, channel_cpl)

    df = pd.DataFrame(all_rows)
    print(f"  - {len(df):,} rows | Total spend: ${df['Spend'].sum():,.0f}")
    return df


# ─────────────────────────────────────────────────────────────────────
# FACT: MARKETING FUNNEL DAILY
# ─────────────────────────────────────────────────────────────────────

def build_fact_marketing_funnel_daily(
    dim_property: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_vendor: pd.DataFrame,
    spend_df: pd.DataFrame,
    vendor_perf: dict,
) -> tuple:
    """
    Build fact_marketing_funnel_daily.

    v8: returns (df, funnel_lookup) where funnel_lookup is:
      {(property_key, vendor_key, YYYYMM): {"leases": int, "leads": int, "visits": int}}

    funnel_lookup is consumed by build_fact_prospect_journey() to align
    journey prospect volumes with funnel-produced counts. All existing
    funnel generation logic is unchanged.
    """
    print("Building fact_marketing_funnel_daily...")

    vendor_to_channel = dim_vendor.set_index("VendorKey")["ChannelKey"].to_dict()
    spend_lookup      = spend_df.groupby(["DateKey", "PropertyKey", "VendorKey"])["Spend"].sum()
    property_tier     = dim_property.set_index("PropertyKey")["PerformanceTier"].to_dict()
    property_strength = dim_property.set_index("PropertyKey")["MarketStrength"].to_dict() if "MarketStrength" in dim_property.columns else {}
    date_lookup       = dim_date.set_index("DateKey")[["Year","MonthNumber"]].to_dict("index")

    combos   = spend_df[["DateKey","PropertyKey","VendorKey"]].drop_duplicates()
    all_rows = []

    for _, row in combos.iterrows():
        date_key     = int(row["DateKey"])
        property_key = int(row["PropertyKey"])
        vendor_key   = int(row["VendorKey"])

        tier         = property_tier.get(property_key, "Average")
        channel_key  = vendor_to_channel.get(vendor_key, 1)
        is_strong    = (property_strength.get(property_key, "Mid") == "Strong")

        dinfo = date_lookup.get(date_key, {})
        month = dinfo.get("MonthNumber", 6)
        year  = dinfo.get("Year", 2024)

        # Get channel funnel rates
        channel_rates = CHANNEL_FUNNEL_RATES.get(channel_key, None)
        funnel_cfg    = channel_rates[tier] if channel_rates else FUNNEL_RATES[tier]

        # Seasonal multiplier (email compressed)
        if channel_key == 5:
            seasonal_mult = get_email_seasonal(month)
        else:
            region_key = dim_property.set_index("PropertyKey")["RegionKey"].to_dict().get(property_key, 1)
            seasonal_mult = get_seasonal_mult(month, region_key)

        spend = spend_lookup.get((date_key, property_key, vendor_key), 0)
        if spend <= 0:
            continue

        impressions = max(1, int(
            spend * IMPRESSIONS_PER_DOLLAR.get(channel_key, 10) * np.random.uniform(0.90, 1.10)
        ))

        # Vendor personality conversion multiplier
        v_mult = get_vendor_conv_mult(vendor_perf, property_key, vendor_key, year, month)

        # Explicit vendor-level efficiency bias keeps vendor conversion differentiated
        # even after portfolio roll-up, so Lead-to-Lease is not flat in the dashboard.
        b_ctr = get_vendor_metric_bias(vendor_key, "ctr")
        b_c2v = get_vendor_metric_bias(vendor_key, "c2v")
        b_v2l = get_vendor_metric_bias(vendor_key, "v2l")
        b_l2l = get_vendor_metric_bias(vendor_key, "l2l")

        # Story funnel multipliers
        s_ctr = get_story_funnel_mult(year, month, channel_key, "ctr", is_strong)
        s_c2v = get_story_funnel_mult(year, month, channel_key, "c2v", is_strong)
        s_l2l = get_story_funnel_mult(year, month, channel_key, "l2l", is_strong)

        ctr = funnel_cfg["ctr"] * (0.90 + 0.20 * np.random.random()) * seasonal_mult * s_ctr * b_ctr
        ctr = float(np.clip(ctr, 0.0005, 0.18))
        ctr = apply_final_rate_compression(channel_key, "ctr", ctr)
        ctr = apply_tier_performance_cap(tier, "ctr", ctr)
        clicks = max(0, np.random.binomial(impressions, ctr))

        c2v = funnel_cfg["c2v"] * (0.85 + 0.30 * np.random.random()) * v_mult * s_c2v * b_c2v
        c2v = apply_long_run_conv_stability(tier, property_strength.get(property_key, "Mid"), "c2v", c2v)
        c2v = float(np.clip(c2v, 0.001, 0.95))
        c2v = apply_final_rate_compression(channel_key, "c2v", c2v)
        c2v = apply_tier_performance_cap(tier, "c2v", c2v)
        if clicks > 0:
            visits = np.random.binomial(clicks, c2v)
        else:
            visits = 0

        v2l = funnel_cfg["v2l"] * (0.85 + 0.30 * np.random.random()) * v_mult * b_v2l
        v2l = apply_long_run_conv_stability(tier, property_strength.get(property_key, "Mid"), "v2l", v2l)
        v2l = float(np.clip(v2l, 0.001, 0.88))
        v2l = apply_final_rate_compression(channel_key, "v2l", v2l)
        v2l = apply_tier_performance_cap(tier, "v2l", v2l)
        if visits > 0:
            leads = np.random.binomial(visits, v2l)
        else:
            leads = 0

        # Volume rebalance after vendor-rate compression:
        # widen the top of funnel slightly so vendor realism stays intact
        # without collapsing portfolio lease volume.
        # v12.3 CPL range lift:
        # reduce top-of-funnel rebalance slightly so downstream lease supply softens
        # and portfolio/property CPL lifts into the new target band.
        leads = int(round(leads * 1.05))

        l2l = funnel_cfg["l2l"] * (0.85 + 0.30 * np.random.random()) * v_mult * s_l2l * b_l2l
        l2l = apply_long_run_conv_stability(tier, property_strength.get(property_key, "Mid"), "l2l", l2l)
        l2l = float(np.clip(l2l, 0.001, 0.35))
        l2l = apply_final_rate_compression(channel_key, "l2l", l2l)
        l2l = apply_tier_performance_cap(tier, "l2l", l2l)
        if leads > 0:
            leases = np.random.binomial(leads, l2l)
        else:
            leases = 0

        visits = max(0, int(visits))
        leads  = max(0, int(leads))
        leases = max(0, int(leases))

        # v12.2 CPL variance restore patch:
        # keep catastrophic denominator collapse from happening, but restore
        # meaningful variance by using a softer, probabilistic lease floor.
        if leads >= 22:
            lease_floor_low = {"Star": 0.007, "Good": 0.006, "Average": 0.005, "Struggler": 0.004}.get(tier, 0.005)
            lease_floor_high = {"Star": 0.014, "Good": 0.012, "Average": 0.010, "Struggler": 0.008}.get(tier, 0.010)
            lease_floor_rate = float(np.random.uniform(lease_floor_low, lease_floor_high))
            lease_floor = int(np.floor(leads * lease_floor_rate))
            leases = max(leases, lease_floor)

        # restore some final-stage variance so portfolio-level CPL does not
        # collapse into an unrealistically tight band.
        if leads > 0:
            final_noise = {"Star": (0.85, 1.25), "Good": (0.80, 1.30), "Average": (0.75, 1.35), "Struggler": (0.70, 1.40)}.get(tier, (0.80, 1.30))
            leases = int(round(leases * np.random.uniform(final_noise[0], final_noise[1])))
            leases = max(0, min(leases, leads))

        for funnel_stage_key, metric_value in {1: impressions, 2: clicks, 3: visits, 4: leads, 5: leases}.items():
            all_rows.append({
                "DateKey":        int(date_key),
                "PropertyKey":    int(property_key),
                "VendorKey":      int(vendor_key),
                "FunnelStageKey": int(funnel_stage_key),
                "MetricValue":    int(metric_value),
            })

    df = pd.DataFrame(all_rows)
    df["DateKey"]        = df["DateKey"].astype("int32")
    df["PropertyKey"]    = df["PropertyKey"].astype("int16")
    df["VendorKey"]      = df["VendorKey"].astype("int16")
    df["FunnelStageKey"] = df["FunnelStageKey"].astype("int8")
    df["MetricValue"]    = df["MetricValue"].astype("int32")

    total_impressions = df.loc[df["FunnelStageKey"] == 1, "MetricValue"].sum()
    print(f"  - {len(df):,} rows | Total impressions: {total_impressions:,}")

    # ── v8: Build funnel_lookup for journey alignment ─────────────────
    # Aggregate funnel visits, leads, leases by property+vendor+month.
    # DateKey is YYYYMMDD — strip day to get YYYYMM month key.
    print("  - Building funnel_lookup for journey alignment...")
    funnel_lookup = {}
    stage_map = {3: "visits", 4: "leads", 5: "leases"}
    for _, row in df[df["FunnelStageKey"].isin([3, 4, 5])].iterrows():
        ym    = int(str(int(row["DateKey"]))[:6])   # 20240315 → 202403
        key   = (int(row["PropertyKey"]), int(row["VendorKey"]), ym)
        stage = stage_map[int(row["FunnelStageKey"])]
        if key not in funnel_lookup:
            funnel_lookup[key] = {"visits": 0, "leads": 0, "leases": 0}
        funnel_lookup[key][stage] += int(row["MetricValue"])

    # Keep lookup as a pure summary of funnel output
    for key in funnel_lookup:
        entry = funnel_lookup[key]
        entry["visits"] = max(0, int(entry["visits"]))
        entry["leads"]  = max(0, int(entry["leads"]))
        entry["leases"] = max(0, min(int(entry["leases"]), int(entry["leads"])))

    total_lookup_leases = sum(v["leases"] for v in funnel_lookup.values())
    print(f"  - funnel_lookup: {len(funnel_lookup):,} property×vendor×month combinations")
    print(f"  - Total funnel leases in lookup: {total_lookup_leases:,}")

    # Quick diagnostic so flat vendor conversion is visible immediately in console output.
    vendor_stage = (
        df[df["FunnelStageKey"].isin([4, 5])]
        .pivot_table(index="VendorKey", columns="FunnelStageKey", values="MetricValue", aggfunc="sum", fill_value=0)
        .rename(columns={4: "Leads", 5: "Leases"})
        .reset_index()
    )
    if not vendor_stage.empty and "Leads" in vendor_stage.columns and "Leases" in vendor_stage.columns:
        vendor_stage["LeadToLeaseRate"] = np.where(
            vendor_stage["Leads"] > 0,
            vendor_stage["Leases"] / vendor_stage["Leads"],
            0.0,
        )
        vendor_names = dim_vendor[["VendorKey", "VendorName"]]
        vendor_stage = vendor_stage.merge(vendor_names, on="VendorKey", how="left")
        top_diag = vendor_stage[["VendorName", "Leads", "Leases", "LeadToLeaseRate"]].sort_values("LeadToLeaseRate", ascending=False)
        print("  - Vendor lead-to-lease sample:")
        for _, r in top_diag.head(6).iterrows():
            print(f"      {r['VendorName']:<16} L2L {r['LeadToLeaseRate']:.3f} | Leads {int(r['Leads']):,} | Leases {int(r['Leases']):,}")
    return df, funnel_lookup


# ─────────────────────────────────────────────────────────────────────
# FACT: PROSPECT JOURNEY
# ─────────────────────────────────────────────────────────────────────

def build_fact_prospect_journey(
    dim_property: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_vendor: pd.DataFrame,
    funnel_lookup: dict,
) -> tuple:
    """
    Build fact_prospect_journey.

    v8: Journey volume is driven by funnel_lookup so that converting
    prospect counts align with funnel-produced lease counts per
    vendor+property+month. All journey rules (touch timing, vendor
    affinity, decay credit, attribution flags) are unchanged.

    Returns:
        (fact_journey_df, attributed_lease_lookup)

    attributed_lease_lookup[(property_key, date_key)] = int
        Count of attributed new leases for that property+day.
        Consumed by build_fact_leasing_daily() to set AttributedNewLeases.
    """
    print("Building fact_prospect_journey...")

    all_vendors       = dim_vendor["VendorKey"].tolist()
    vendor_to_channel = dim_vendor.set_index("VendorKey")["ChannelKey"].to_dict()

    date_map = dim_date.set_index("DateKey")[["Date", "MonthNumber"]].copy()
    date_map["Date"] = pd.to_datetime(date_map["Date"]).dt.date
    all_datekeys = dim_date["DateKey"].tolist()

    # Group datekeys by YYYYMM for lease date selection and touch distribution
    monthly_dates = {}
    for date_key in all_datekeys:
        ym = str(date_key)[:6]
        monthly_dates.setdefault(ym, []).append(date_key)

    all_rows     = []
    prospect_key = 1

    for _, prop in dim_property.iterrows():
        property_key    = int(prop["PropertyKey"])
        tier            = prop["PerformanceTier"]
        region_key      = int(prop["RegionKey"])

        for ym_str, month_datekeys in sorted(monthly_dates.items()):
            ym        = int(ym_str)
            month_num = date_map.loc[month_datekeys[0], "MonthNumber"]

            # ── Collect all vendors that have funnel activity this month ──
            # Each vendor independently contributed funnel leads/leases.
            # We build one prospect pool per vendor so that vendor-level
            # attribution in the journey table matches the funnel table.
            vendor_keys_this_month = list({
                vk for (pk, vk, m) in funnel_lookup if pk == property_key and m == ym
            })

            if not vendor_keys_this_month:
                # No spend/funnel activity for this property+month — skip
                continue

            for vendor_key in vendor_keys_this_month:
                lookup_key   = (property_key, vendor_key, ym)
                funnel_entry = funnel_lookup.get(lookup_key, {"visits": 0, "leads": 0, "leases": 0})

                funnel_leases       = int(funnel_entry["leases"])
                funnel_leads        = int(funnel_entry["leads"])
                num_converting      = max(0, funnel_leases)
                num_total_prospects = max(num_converting, funnel_leads)
                num_nonconverting   = max(0, num_total_prospects - num_converting)

                channel_key = vendor_to_channel.get(vendor_key, 1)
                seasonal_mult = (
                    get_email_seasonal(month_num)
                    if channel_key == 5
                    else get_seasonal_mult(month_num, region_key)
                )

                # ── Converting prospects ──────────────────────────────────
                for _ in range(num_converting):
                    journey_length = int(np.random.choice(
                        list(JOURNEY_MIX.keys()), p=list(JOURNEY_MIX.values())
                    ))

                    lease_date_key = int(random.choice(month_datekeys))
                    lease_date     = date_map.loc[lease_date_key, "Date"]

                    if journey_length == 1:
                        days_before = [random.randint(0, 2)]
                    elif journey_length == 2:
                        days_before = sorted([
                            random.randint(0, 2),
                            random.randint(3, ATTRIBUTION_LOOKBACK_DAYS),
                        ])
                    else:
                        days_before = sorted([
                            random.randint(0, 1),
                            random.randint(2, 4),
                            random.randint(5, ATTRIBUTION_LOOKBACK_DAYS),
                        ])
                    days_before = list(reversed(days_before))

                    # Vendor sequence: last touch is always this vendor
                    # (since this prospect came from this vendor's funnel).
                    # Earlier touches drawn from VENDOR_TOUCH_AFFINITY.
                    selected_vendors = []
                    for touch_position in range(1, journey_length + 1):
                        if touch_position == journey_length:
                            # Last touch — always the vendor whose funnel this came from
                            selected_vendors.append(vendor_key)
                        else:
                            available = [v for v in all_vendors if v not in selected_vendors and v != vendor_key]
                            if not available:
                                available = [v for v in all_vendors if v not in selected_vendors]
                            weights  = [VENDOR_TOUCH_AFFINITY.get(v, {}).get(touch_position, 1.0) for v in available]
                            total_w  = sum(weights)
                            norm_w   = [w / total_w for w in weights]
                            selected_vendors.append(int(np.random.choice(available, p=norm_w)))

                    decay_weights = [math.exp(-DECAY_LAMBDA * d) for d in days_before]
                    total_w       = sum(decay_weights)
                    credits       = [w / total_w for w in decay_weights]

                    for touch_idx in range(journey_length):
                        touch_number  = touch_idx + 1
                        t_vendor_key  = selected_vendors[touch_idx]
                        t_channel_key = vendor_to_channel[t_vendor_key]

                        stage_dist       = TOUCH_STAGE_DIST.get(touch_number, TOUCH_STAGE_DIST[1])
                        funnel_stage_key = int(np.random.choice(
                            list(stage_dist.keys()), p=list(stage_dist.values())
                        ))

                        touch_date     = lease_date - timedelta(days=days_before[touch_idx])
                        touch_date_key = int(touch_date.strftime("%Y%m%d"))
                        touch_date_key = max(min(touch_date_key, all_datekeys[-1]), all_datekeys[0])

                        is_last_touch = 1 if touch_number == journey_length else 0
                        is_direct     = 1 if is_last_touch else 0
                        is_assisted   = 1 if not is_last_touch and journey_length > 1 else 0

                        all_rows.append({
                            "ProspectKey":     int(prospect_key),
                            "PropertyKey":     int(property_key),
                            "DateKey":         int(touch_date_key),
                            "VendorKey":       int(t_vendor_key),
                            "ChannelKey":      int(t_channel_key),
                            "FunnelStageKey":  int(funnel_stage_key),
                            "TouchNumber":     int(touch_number),
                            "TotalTouches":    int(journey_length),
                            "DaysBeforeLease": int(days_before[touch_idx]),
                            "LeaseDateKey":    int(lease_date_key),
                            "Converted":       1,
                            "AttributedCredit": round(credits[touch_idx], 6),
                            "IsDirectCredit":  int(is_direct),
                            "IsAssistedCredit": int(is_assisted),
                        })

                    prospect_key += 1

                # ── Non-converting prospects ──────────────────────────────
                for _ in range(num_nonconverting):
                    journey_length = int(np.random.choice(
                        list(JOURNEY_MIX.keys()), p=list(JOURNEY_MIX.values())
                    ))

                    # Distribute non-converting touches across month weighted by seasonal
                    touch_date_key = int(random.choice(month_datekeys))

                    selected_vendors = []
                    for touch_position in range(1, journey_length + 1):
                        if touch_position == 1:
                            # First touch — this vendor (they initiated but didn't convert)
                            selected_vendors.append(vendor_key)
                        else:
                            available = [v for v in all_vendors if v not in selected_vendors]
                            weights  = [VENDOR_TOUCH_AFFINITY.get(v, {}).get(touch_position, 1.0) for v in available]
                            total_w  = sum(weights)
                            norm_w   = [w / total_w for w in weights]
                            selected_vendors.append(int(np.random.choice(available, p=norm_w)))

                    for touch_idx in range(journey_length):
                        touch_number  = touch_idx + 1
                        t_vendor_key  = selected_vendors[touch_idx]
                        t_channel_key = vendor_to_channel[t_vendor_key]

                        stage_dist       = TOUCH_STAGE_DIST.get(touch_number, TOUCH_STAGE_DIST[1])
                        funnel_stage_key = int(np.random.choice(
                            list(stage_dist.keys()), p=list(stage_dist.values())
                        ))

                        all_rows.append({
                            "ProspectKey":     int(prospect_key),
                            "PropertyKey":     int(property_key),
                            "DateKey":         int(touch_date_key),
                            "VendorKey":       int(t_vendor_key),
                            "ChannelKey":      int(t_channel_key),
                            "FunnelStageKey":  int(funnel_stage_key),
                            "TouchNumber":     int(touch_number),
                            "TotalTouches":    int(journey_length),
                            "DaysBeforeLease": None,
                            "LeaseDateKey":    None,
                            "Converted":       0,
                            "AttributedCredit": 0.0,
                            "IsDirectCredit":  0,
                            "IsAssistedCredit": 0,
                        })

                    prospect_key += 1

    df = pd.DataFrame(all_rows)
    df["ProspectKey"]      = df["ProspectKey"].astype("int32")
    df["PropertyKey"]      = df["PropertyKey"].astype("int16")
    df["DateKey"]          = df["DateKey"].astype("int32")
    df["VendorKey"]        = df["VendorKey"].astype("int16")
    df["ChannelKey"]       = df["ChannelKey"].astype("int16")
    df["FunnelStageKey"]   = df["FunnelStageKey"].astype("int8")
    df["TouchNumber"]      = df["TouchNumber"].astype("int8")
    df["TotalTouches"]     = df["TotalTouches"].astype("int8")
    df["DaysBeforeLease"]  = df["DaysBeforeLease"].astype("Int16")
    df["LeaseDateKey"]     = df["LeaseDateKey"].astype("Int32")
    df["Converted"]        = df["Converted"].astype("int8")
    df["AttributedCredit"] = df["AttributedCredit"].astype("float64")
    df["IsDirectCredit"]   = df["IsDirectCredit"].astype("int8")
    df["IsAssistedCredit"] = df["IsAssistedCredit"].astype("int8")

    # ── Validation ────────────────────────────────────────────────────
    converted_df = df[df["Converted"] == 1]
    credit_check = converted_df.groupby("ProspectKey")["AttributedCredit"].sum()
    bad_credits  = credit_check[abs(credit_check - 1.0) > 0.001]

    total_prospects  = df["ProspectKey"].nunique()
    converted_count  = df.loc[df["Converted"] == 1, "ProspectKey"].nunique()
    actual_conv_rate = converted_count / total_prospects if total_prospects else 0

    print(f"  - {len(df):,} touchpoint rows")
    print(f"  - {total_prospects:,} prospects | {converted_count:,} converted ({actual_conv_rate:.1%})")
    print(f"  - Multi-touch journeys: {df.loc[df['TotalTouches'] > 1, 'ProspectKey'].nunique():,}")
    print(f"  - Credit sum errors: {len(bad_credits)} (should be 0)")

    # ── Build attributed_lease_lookup at DAY grain ───────────────────
    direct_rows = df[df["IsDirectCredit"] == 1].copy()
    attr_daily = (
        direct_rows
        .groupby(["PropertyKey", "LeaseDateKey"])["ProspectKey"]
        .nunique()
        .reset_index()
        .rename(columns={"ProspectKey": "count"})
    )
    attr_lookup = {
        (int(r["PropertyKey"]), int(r["LeaseDateKey"])): int(r["count"])
        for _, r in attr_daily.iterrows()
    }
    total_attributed = sum(attr_lookup.values())
    print(f"  - attributed_lease_lookup: {len(attr_lookup):,} property+day combinations")
    print(f"  - Total attributed leases: {total_attributed:,}")

    # ── Deduplication guard ───────────────────────────────────────────
    # Non-converting prospects with multi-touch journeys can produce
    # duplicate PK rows when two touches land on the same date with the
    # same vendor (DateKey drawn randomly from the month pool).
    # Drop dupes before write — keep first occurrence, preserving all
    # converting rows which are built deterministically and cannot clash.
    pk_cols = ["ProspectKey", "PropertyKey", "DateKey", "VendorKey", "TouchNumber"]
    n_before = len(df)
    df = df.drop_duplicates(subset=pk_cols, keep="first")
    n_dropped = n_before - len(df)
    if n_dropped:
        print(f"  - PK dedup: dropped {n_dropped:,} duplicate rows ({n_before:,} -> {len(df):,})")
    else:
        print(f"  - PK dedup: no duplicates found ({len(df):,} rows clean)")

    return df, attr_lookup


# ─────────────────────────────────────────────────────────────────────
# CSV
# ─────────────────────────────────────────────────────────────────────

EXCLUDE_COLS = [
    "PerformanceTier","MarketStrength","PeakShift","Volatility",
    "LeadQuality","AnomalyMonths","SpendEfficiency",
]

def save_csv(df: pd.DataFrame, name: str, exclude_cols=None) -> str:
    out = df.copy()
    if exclude_cols:
        out = out.drop(columns=[c for c in exclude_cols if c in out.columns])
    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
    out.to_csv(path, index=False)
    print(f"  - Saved {path} ({len(out):,} rows)")
    return path


# ─────────────────────────────────────────────────────────────────────
# SQL HELPERS
# ─────────────────────────────────────────────────────────────────────

def get_connection_string() -> str:
    missing = [k for k in ("username","password") if not DB_CONFIG.get(k)]
    if missing:
        raise ValueError(f"Missing SQL config: {', '.join(missing)}")
    return (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )


def get_sql_engine():
    from sqlalchemy import create_engine
    conn_str = get_connection_string()
    quoted   = urllib.parse.quote_plus(conn_str)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quoted}",
        fast_executemany=True, pool_pre_ping=True, pool_recycle=1800,
    )


def clear_sql_tables(engine) -> None:
    from sqlalchemy import text
    # ONLY fact tables — all dims are protected (including dim_lease_date)
    # DataSource guard: only clears DataSource = 1 (synthetic generator rows).
    # Live pipeline rows (DataSource 2/3/4) are never touched by this function.
    tables = [
        "fact_prospect_journey",
        "fact_marketing_funnel_daily",
        "fact_marketing_spend_daily",
        "fact_leasing_daily",
        "fact_property_ops_daily",
    ]
    print("\n-- CLEARING AZURE SQL FACT TABLES (DataSource = 1 only) ----")
    with engine.begin() as conn:
        for t in tables:
            conn.execute(text(f"DELETE FROM dbo.{t} WHERE DataSource = 1"))
    print("  - Synthetic fact rows cleared (DataSource = 1)")


def get_dtype_map(table_name: str):
    from sqlalchemy import Integer, Numeric, SmallInteger
    from sqlalchemy.dialects.mssql import TINYINT

    maps = {
        "fact_property_ops_daily": {
            "DateKey": Integer(), "PropertyKey": SmallInteger(),
            "OccupiedUnits": Integer(), "VacantUnits": Integer(),
            "AvailableUnits": Integer(), "MoveIns": Integer(),
            "MoveOuts": Integer(), "LeaseExpirations": Integer(),
            "ScheduledMoveIns": Integer(),
            "LeaseExpirations_Next60D": Numeric(18, 2),
            "ScheduledMoveIns_Next60D": Numeric(18, 2),
        },
        "fact_leasing_daily": {
            "DateKey": Integer(), "PropertyKey": SmallInteger(),
            "Leads": Integer(), "NewLeases": Integer(), "Visits": Integer(),
            "AttributedNewLeases": Integer(), "UnattributedLeases": Integer(),
        },
        "fact_marketing_spend_daily": {
            "DateKey": Integer(), "PropertyKey": SmallInteger(),
            "VendorKey": SmallInteger(), "Spend": Numeric(18, 4),
        },
        "fact_marketing_funnel_daily": {
            "DateKey": Integer(), "PropertyKey": SmallInteger(),
            "VendorKey": SmallInteger(), "FunnelStageKey": TINYINT(),
            "MetricValue": Integer(),
        },
        "fact_prospect_journey": {
            "ProspectKey": Integer(), "PropertyKey": SmallInteger(),
            "DateKey": Integer(), "VendorKey": SmallInteger(),
            "ChannelKey": SmallInteger(), "FunnelStageKey": TINYINT(),
            "TouchNumber": TINYINT(), "TotalTouches": TINYINT(),
            "DaysBeforeLease": SmallInteger(), "LeaseDateKey": Integer(),
            "Converted": TINYINT(), "AttributedCredit": Numeric(18, 6),
            "IsDirectCredit": TINYINT(), "IsAssistedCredit": TINYINT(),
        },
    }
    return maps.get(table_name)


def prepare_df_for_sql(df: pd.DataFrame, table_name: str, exclude_cols=None) -> pd.DataFrame:
    out = df.copy()
    if exclude_cols:
        out = out.drop(columns=[c for c in exclude_cols if c in out.columns])
    if "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"]).dt.date

    if table_name == "fact_property_ops_daily":
        out["DateKey"]      = out["DateKey"].astype("int32")
        out["PropertyKey"]  = out["PropertyKey"].astype("int16")

    elif table_name == "fact_leasing_daily":
        out["DateKey"]      = out["DateKey"].astype("int32")
        out["PropertyKey"]  = out["PropertyKey"].astype("int16")

    elif table_name == "fact_marketing_spend_daily":
        out["DateKey"]      = out["DateKey"].astype("int32")
        out["PropertyKey"]  = out["PropertyKey"].astype("int16")
        out["VendorKey"]    = out["VendorKey"].astype("int16")

    elif table_name == "fact_marketing_funnel_daily":
        out["DateKey"]        = out["DateKey"].astype("int32")
        out["PropertyKey"]    = out["PropertyKey"].astype("int16")
        out["VendorKey"]      = out["VendorKey"].astype("int16")
        out["FunnelStageKey"] = out["FunnelStageKey"].astype("int8")
        out["MetricValue"]    = out["MetricValue"].astype("int32")

    elif table_name == "fact_prospect_journey":
        out["ProspectKey"]    = out["ProspectKey"].astype("int32")
        out["PropertyKey"]    = out["PropertyKey"].astype("int16")
        out["DateKey"]        = out["DateKey"].astype("int32")
        out["VendorKey"]      = out["VendorKey"].astype("int16")
        out["ChannelKey"]     = out["ChannelKey"].astype("int16")
        out["FunnelStageKey"] = out["FunnelStageKey"].astype("int8")
        out["TouchNumber"]    = out["TouchNumber"].astype("int8")
        out["TotalTouches"]   = out["TotalTouches"].astype("int8")
        out["DaysBeforeLease"] = out["DaysBeforeLease"].astype("Int16")
        out["LeaseDateKey"]   = out["LeaseDateKey"].astype("Int32")
        out["Converted"]      = out["Converted"].astype("int8")
        out["IsDirectCredit"] = out["IsDirectCredit"].astype("int8")
        out["IsAssistedCredit"] = out["IsAssistedCredit"].astype("int8")

    return out


def load_table_to_sql(df, table_name, engine, exclude_cols=None, chunksize=50000):
    out = prepare_df_for_sql(df, table_name, exclude_cols=exclude_cols)
    total_rows = len(out)
    if total_rows == 0:
        print(f"  - Skipping {table_name}: 0 rows")
        return

    dtype_map = get_dtype_map(table_name)
    print(f"  - Loading {table_name} ({total_rows:,} rows)...")

    start_idx = 0
    batch_num = 1
    loaded    = 0

    while start_idx < total_rows:
        end_idx = min(start_idx + chunksize, total_rows)
        chunk   = out.iloc[start_idx:end_idx].copy()
        try:
            with engine.begin() as conn:
                chunk.to_sql(
                    table_name, conn, schema="dbo",
                    if_exists="append", index=False,
                    method=None, dtype=dtype_map,
                )
            loaded += len(chunk)
            print(f"    OK batch {batch_num}: rows {start_idx+1:,}-{end_idx:,} | loaded: {loaded:,}/{total_rows:,}")
        except Exception as exc:
            print(f"    X FAILED batch {batch_num}: rows {start_idx+1:,}-{end_idx:,}")
            print(f"    X {type(exc).__name__}: {exc}")
            if hasattr(exc, "orig"):
                print(f"    X DBAPI: {exc.orig}")
            raise
        start_idx = end_idx
        batch_num += 1

    print(f"    OK {table_name}: {loaded:,} rows inserted")


def get_sql_table_count(engine, table_name):
    from sqlalchemy import text
    with engine.begin() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM dbo.{table_name}")).scalar())


def print_sql_row_counts(engine):
    tables = [
        "fact_property_ops_daily", "fact_leasing_daily",
        "fact_marketing_spend_daily", "fact_marketing_funnel_daily",
        "fact_prospect_journey",
    ]
    print("\n-- SQL FACT TABLE COUNTS -----------------------------------")
    for t in tables:
        print(f"  {t:<35} {get_sql_table_count(engine, t):>12,}")



def print_normalization_diagnostics(fact_ops: pd.DataFrame, fact_lease: pd.DataFrame, dim_property: pd.DataFrame) -> None:
    merged = fact_ops.merge(
        dim_property[["PropertyKey", "TotalUnits"]],
        on="PropertyKey",
        how="left",
        validate="many_to_one",
    )
    overflow_rows = int((merged["OccupiedUnits"] > merged["TotalUnits"]).sum())
    negative_vacant = int((fact_ops["VacantUnits"] < 0).sum())
    moveins_zero_pct = float((fact_ops["MoveIns"] == 0).mean())
    moveins_nonzero_pct = 1.0 - moveins_zero_pct
    coverage_max_ratio = float(
        fact_ops["ScheduledMoveIns_Next60D"].max() / max(fact_ops["LeaseExpirations_Next60D"].max(), 0.0001)
    )
    row_level_coverage_max = float(
        (fact_ops["ScheduledMoveIns_Next60D"] / fact_ops["LeaseExpirations_Next60D"].replace(0, np.nan))
        .fillna(0)
        .max()
    )
    attr_row_max = float(
        (fact_lease["AttributedNewLeases"] / fact_lease["NewLeases"].replace(0, np.nan))
        .fillna(0)
        .max()
    )
    attr_over_rows = int((fact_lease["AttributedNewLeases"] > fact_lease["NewLeases"]).sum())
    lease_match_pct = float((fact_ops["MoveIns"].values == fact_lease["NewLeases"].values).mean())

    print("\n-- NORMALIZATION DIAGNOSTICS --------------------------------")
    print(f"  OccupiedUnits > TotalUnits rows:      {overflow_rows:,}")
    print(f"  VacantUnits < 0 rows:                 {negative_vacant:,}")
    print(f"  MoveIns nonzero days:                 {moveins_nonzero_pct:.2%}")
    print(f"  MoveIns zero days:                    {moveins_zero_pct:.2%}")
    print(f"  MoveIns == NewLeases:                 {lease_match_pct:.4%}")
    print(f"  Renewal coverage ratio max/max:       {coverage_max_ratio:.4f}")
    print(f"  Renewal coverage ratio max row-level: {row_level_coverage_max:.4f}")
    print(f"  Attr > NewLeases rows:                {attr_over_rows:,}")
    print(f"  Attribution ratio max row-level:      {attr_row_max:.4f}")


# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────

def main(mode: str = "csv") -> None:
    print("=" * 65)
    print("MAA Marketing Analytics - Data Generator v12.5 (CPL rebalance patch)")
    print(f"Mode: {mode.upper()} | Date range: {DATE_START} - {DATE_END}")
    print("=" * 65)

    print("\n-- DIMENSIONS (in-memory only) -----------------------------")
    dim_date       = build_dim_date()
    dim_region     = build_dim_region()
    dim_market     = build_dim_market()
    dim_vendor     = build_dim_vendor()
    dim_property   = resolve_dim_property(dim_market, PROPERTY_SEED_PATH)

    print("\n-- VENDOR PERSONALITY MATRIX --------------------------------")
    vendor_perf = build_vendor_performance_matrix(dim_property, dim_vendor)
    print(f"  - Built for {len(vendor_perf):,} properties × {len(dim_vendor):,} vendors")

    print("\n-- FACTS ---------------------------------------------------")
    # v8 generation order:
    # 1. spend  — independent, drives funnel volume
    # 2. funnel — driven by spend, returns funnel_lookup for journey alignment
    # 3. journey — driven by funnel_lookup, returns attr_lookup for leasing
    # 4. leasing — receives attr_lookup, AttributedNewLeases = true subset of NewLeases
    # 5. ops    — receives leasing, MoveIns = NewLeases (v6 fix preserved)
    fact_spend  = build_fact_marketing_spend_daily(dim_property, dim_date, dim_vendor, vendor_perf)
    fact_funnel, funnel_lookup = build_fact_marketing_funnel_daily(dim_property, dim_date, dim_vendor, fact_spend, vendor_perf)
    fact_journey, attr_lookup  = build_fact_prospect_journey(dim_property, dim_date, dim_vendor, funnel_lookup)
    fact_lease  = build_fact_leasing_daily(dim_property, dim_date, attributed_leases=attr_lookup)
    fact_ops    = build_fact_property_ops_daily(dim_property, dim_date, leasing_df=fact_lease)
    validate_fact_property_ops_daily(fact_ops, dim_property)
    print_normalization_diagnostics(fact_ops, fact_lease, dim_property)

    total_new = int(fact_lease["NewLeases"].sum())
    total_attr = int(fact_lease["AttributedNewLeases"].sum())
    coverage = (total_attr / total_new) if total_new else 0.0

    print("\n-- ATTRIBUTION COVERAGE -------------------------------------")
    print(f"  Attributed leases: {total_attr:,}")
    print(f"  Total new leases:  {total_new:,}")
    print(f"  Coverage:          {coverage:.2%}")
    print("  Expected band:     60% - 80%")

    # v8: dim_lease_date — built from dim_date, renamed DateKey → LeaseDateKey
    print("\n-- DIM LEASE DATE ------------------------------------------")
    dim_lease_date = build_dim_lease_date(dim_date)

    if mode in ("csv", "both"):
        print("\n-- SAVING CSVs ---------------------------------------------")
        save_csv(dim_date,       "dim_date")
        save_csv(dim_lease_date, "dim_lease_date")
        save_csv(dim_region,     "dim_region")
        save_csv(dim_market,     "dim_market")
        save_csv(dim_vendor,     "dim_vendor")
        save_csv(dim_property,   "dim_property", exclude_cols=EXCLUDE_COLS)
        save_csv(fact_ops,       "fact_property_ops_daily")
        save_csv(fact_lease,     "fact_leasing_daily")
        save_csv(fact_spend,     "fact_marketing_spend_daily")
        save_csv(fact_funnel,    "fact_marketing_funnel_daily")
        save_csv(fact_journey,   "fact_prospect_journey")

    if mode in ("sql", "both"):
        engine = get_sql_engine()
        clear_sql_tables(engine)  # ONLY clears fact tables — dims protected

        # dim_lease_date: load with if_exists="replace" so it is created on
        # first run and refreshed if date range changes. It is NOT in
        # clear_sql_tables() because it is a dimension, not a fact table.
        print("\n-- LOADING dim_lease_date TO AZURE SQL ---------------------")
        from sqlalchemy import Integer as _Int
        load_table_to_sql(
            dim_lease_date, "dim_lease_date", engine, chunksize=5000
        )

        print("\n-- LOADING TO AZURE SQL (fact tables only) ----------------")
        load_table_to_sql(fact_ops,    "fact_property_ops_daily",    engine, chunksize=5000)
        load_table_to_sql(fact_lease,  "fact_leasing_daily",         engine, chunksize=5000)
        load_table_to_sql(fact_spend,  "fact_marketing_spend_daily", engine, chunksize=5000)
        load_table_to_sql(fact_funnel, "fact_marketing_funnel_daily",engine, chunksize=5000)
        load_table_to_sql(fact_journey,"fact_prospect_journey",      engine, chunksize=5000)

        print_sql_row_counts(engine)

    # ── Summary ──────────────────────────────────────────────────────
    converted    = fact_journey.loc[fact_journey["Converted"] == 1, "ProspectKey"].nunique()
    total_pros   = fact_journey["ProspectKey"].nunique()
    multi_touch  = fact_journey.loc[fact_journey["TotalTouches"] > 1, "ProspectKey"].nunique()
    total_credit = fact_journey["AttributedCredit"].sum()
    total_attr   = int(fact_lease["AttributedNewLeases"].sum())
    total_new    = int(fact_lease["NewLeases"].sum())
    coverage     = total_attr / total_new if total_new > 0 else 0
    total_fact_rows = len(fact_ops) + len(fact_lease) + len(fact_spend) + len(fact_funnel) + len(fact_journey)

    print("\n-- SUMMARY -------------------------------------------------")
    print(f"  dim_date:                    {len(dim_date):>9,} rows")
    print(f"  dim_lease_date:              {len(dim_lease_date):>9,} rows")
    print(f"  dim_property:                {len(dim_property):>9,} rows")
    print(f"  fact_property_ops_daily:     {len(fact_ops):>9,} rows")
    print(f"  fact_leasing_daily:          {len(fact_lease):>9,} rows")
    print(f"  fact_marketing_spend_daily:  {len(fact_spend):>9,} rows")
    print(f"  fact_marketing_funnel_daily: {len(fact_funnel):>9,} rows")
    print(f"  fact_prospect_journey:       {len(fact_journey):>9,} rows")
    print(f"\n  Total fact rows:             {total_fact_rows:>9,}")

    print("\n-- ATTRIBUTION SUMMARY -------------------------------------")
    print(f"  Total prospects:             {total_pros:>9,}")
    if total_pros > 0:
        print(f"  Converted to lease:          {converted:>9,} ({converted/total_pros:.1%})")
        print(f"  Multi-touch journeys:        {multi_touch:>9,} ({multi_touch/total_pros:.1%})")
    print(f"  Total attributed credit:     {total_credit:>9,.1f} (should ≈ converted prospects)")
    print(f"\n-- ALIGNMENT SUMMARY --------------------------------------")
    print(f"  Attributed new leases:       {total_attr:>9,}")
    print(f"  Total operational leases:    {total_new:>9,}")
    print(f"  Attribution coverage:        {coverage:>9.1%} (expected 60-80%)")
    print("\n-- CASE STUDY CPL TARGET BAND --------------------------------")
    print("  Preferred property-level 24M CPL band: ~$275 - ~$950")
    print("  Soft upper limit for clean visuals:    ~$1,200")
    print("  Target median band:                    $300 - $450")
    print("  Target P75 band:                       $550 - $750")
    print("  Target P90 band:                       $800 - $1,050")
    print(f"\n  Output directory:            {os.path.abspath(OUTPUT_DIR)}")
    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MAA Data Generator v12.5")
    parser.add_argument("--mode", choices=["csv","sql","both"], default="csv")
    args = parser.parse_args()
    main(args.mode)
