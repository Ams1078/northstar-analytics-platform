"""
mock_source_generator.py
========================
NorthStar MAA — Mock Pipeline Source File Generator

Produces one day of realistic source files in authentic platform formats,
calibrated from the synthetic gold data baseline.

OUTPUT (14 files total per run):
  Spend Pipeline (7 files):
    google_ads_export_{date}.csv       — Google Ads format (VK4 + VK8)
    bing_ads_export_{date}.csv         — Microsoft Advertising format (VK5)
    meta_ads_export_{date}.csv         — Meta Ads Manager format (VK6 + VK7)
    zillow_leads_{date}.csv            — Zillow Rental Manager format (VK1)
    apartments_com_{date}.csv          — CoStar/Apartments.com format (VK2)
    apartment_list_{date}.csv          — Apartment List partner format (VK3)
    display_dsp_{date}.csv             — Programmatic DSP format (VK9 + VK10)

  Ops Pipeline (1 file):
    yardi_ops_export_{date}.csv        — Yardi Voyager flat file (all 120 props)

  CRM Pipeline (6 files):
    sf_leads_raw_{date}.csv
    sf_contacts_raw_{date}.csv
    sf_opportunities_raw_{date}.csv
    sf_tasks_raw_{date}.csv
    sf_campaigns_raw_{date}.csv
    sf_campaign_members_raw_{date}.csv

USAGE:
    python mock_source_generator.py --date 2026-04-11
    python mock_source_generator.py --date 2026-04-11 --output ./mock_sources

DESIGN:
    Numbers are seeded from the gold baseline (last known day of synthetic data)
    with realistic daily noise applied. Each file format matches real platform
    exports as closely as possible — field names, column order, quirks included.

TOUCH DISTRIBUTION NOTE:
    On a single-day slice the Task.Type distribution will appear skewed:
      Lead:  ~97% of tasks  (all non-converting prospects get 1 Lead touch)
      Tour:  ~0.5%          (only 3-touch converting journeys — ~25% of ~37 daily)
      Lease: ~1.7%          (all converting prospects get exactly 1 Lease touch)
    This is correct and expected. The distribution reflects reality: most daily
    CRM events are new lead inquiries, not tours or lease signings. At monthly
    grain the ratios match the gold baseline journey mix (40/35/25 for 1/2/3-touch).

ORGANIC / DIRECT NOTE:
    Organic (VendorKey=12) is NOT emergent from LEAD_VOLUME_WEIGHTS — that weight
    is intentionally 0. Organic leads are forced via a 3% rule on non-converting
    leads in build_prospect_registry(). This ensures the organic pipeline code path
    is exercised on every run without distorting the paid vendor distribution.

DATE LOGIC NOTE:
    ConvertedDate = CloseDate = run_dt (the date the CRM records the lease event).
    LeaseStartDate__c = run_dt + 1-30 days (the physical move-in date).
    The CRM pipeline uses LeaseStartDate__c as the LeaseDateKey anchor per
    Crosswalk v1.2.1. The attribution window (7 days) is computed relative to
    LeaseStartDate__c, not ConvertedDate. Keep this distinction explicit in ETL.
"""

import csv
import os
import random
import string
import hashlib
import argparse
import datetime
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# Path to the generated gold data (used as calibration baseline)
GOLD_DATA_PATH = "./maa_generated_data"

# Vendor keys
VK_ZILLOW        = 1
VK_APARTMENTS    = 2
VK_APTLIST       = 3
VK_GOOGLE_ADS    = 4
VK_BING          = 5
VK_FACEBOOK      = 6
VK_INSTAGRAM     = 7
VK_GOOGLE_DISP   = 8
VK_STACKADAPT    = 9
VK_TRADEDESK     = 10
VK_EMAIL         = 11

# Funnel stage keys
SK_IMPRESSIONS = 1
SK_CLICKS      = 2
SK_VISITS      = 3
SK_LEADS       = 4
SK_LEASES      = 5

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def noise(value, pct=0.08):
    """Apply ±pct random noise to a numeric value."""
    return max(0, value * random.uniform(1 - pct, 1 + pct))

def sf_id(prefix="00Q"):
    """Generate a realistic Salesforce 18-char ID."""
    chars = string.ascii_uppercase + string.digits
    return prefix + ''.join(random.choices(chars, k=15))

def sf_datetime(dt):
    """Format as Salesforce datetime string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000")

def sf_date(dt):
    return dt.strftime("%Y-%m-%d")

def yardi_date(dt):
    return dt.strftime("%m/%d/%Y")

def google_date(dt):
    return dt.strftime("%Y-%m-%d")

def meta_date(dt):
    return dt.strftime("%Y-%m-%d")

def load_gold_baseline(run_date_str):
    """
    Load the last available day of gold data as the calibration baseline.
    Returns dicts keyed by PropertyKey for spend, funnel, and ops.
    """
    # Find the most recent DateKey before run_date
    run_dt = datetime.datetime.strptime(run_date_str, "%Y-%m-%d")
    target_dk = int(run_dt.strftime("%Y%m%d"))

    print(f"Loading gold baseline for calibration (target DateKey: {target_dk})...")

    # Load dim_property
    props = {}
    with open(os.path.join(GOLD_DATA_PATH, "dim_property.csv")) as f:
        for r in csv.DictReader(f):
            props[int(r["PropertyKey"])] = r

    # Find closest available DateKey
    available_dates = set()
    with open(os.path.join(GOLD_DATA_PATH, "fact_leasing_daily.csv")) as f:
        for r in csv.DictReader(f):
            available_dates.add(int(r["DateKey"]))
    baseline_dk = max(dk for dk in available_dates if dk <= target_dk)
    print(f"  Baseline DateKey: {baseline_dk}")

    # Load spend baseline
    spend = defaultdict(lambda: defaultdict(float))  # [prop_key][vendor_key]
    with open(os.path.join(GOLD_DATA_PATH, "fact_marketing_spend_daily.csv")) as f:
        for r in csv.DictReader(f):
            if int(r["DateKey"]) == baseline_dk:
                spend[int(r["PropertyKey"])][int(r["VendorKey"])] = float(r["Spend"])

    # Load funnel baseline
    funnel = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # [prop][vendor][stage]
    with open(os.path.join(GOLD_DATA_PATH, "fact_marketing_funnel_daily.csv")) as f:
        for r in csv.DictReader(f):
            if int(r["DateKey"]) == baseline_dk:
                funnel[int(r["PropertyKey"])][int(r["VendorKey"])][int(r["FunnelStageKey"])] = int(r["MetricValue"])

    # Load ops baseline
    ops = {}
    with open(os.path.join(GOLD_DATA_PATH, "fact_property_ops_daily.csv")) as f:
        for r in csv.DictReader(f):
            if int(r["DateKey"]) == baseline_dk:
                ops[int(r["PropertyKey"])] = r

    # Load leasing baseline
    leasing = {}
    with open(os.path.join(GOLD_DATA_PATH, "fact_leasing_daily.csv")) as f:
        for r in csv.DictReader(f):
            if int(r["DateKey"]) == baseline_dk:
                leasing[int(r["PropertyKey"])] = r

    # Load monthly lease and lead totals for CRM registry calibration
    from collections import defaultdict as _dd
    monthly_leases_g = _dd(lambda: _dd(int))
    monthly_leads_g  = _dd(lambda: _dd(int))
    with open(os.path.join(GOLD_DATA_PATH, "fact_leasing_daily.csv")) as f:
        for r in csv.DictReader(f):
            dk = int(r["DateKey"]); ym = dk // 100; pk = int(r["PropertyKey"])
            monthly_leases_g[pk][ym] += int(r["NewLeases"])
            monthly_leads_g[pk][ym]  += int(r["Leads"])

    print(f"  Loaded: {len(props)} properties, {len(spend)} spend records, "
          f"{len(ops)} ops records, {len(monthly_leases_g)} props with monthly history")
    return props, spend, funnel, ops, leasing, monthly_leases_g, monthly_leads_g


# ─────────────────────────────────────────────────────────────────────────────
# PROSPECT REGISTRY — CRM relational coherence
# ─────────────────────────────────────────────────────────────────────────────

"""
CRM Mock Generator — Refactored with Prospect Registry
=======================================================
Replaces the 4 broken CRM functions in mock_source_generator.py.

Architecture:
  1. build_prospect_registry()  — builds all prospects for the run month
     anchored to the gold monthly lease baseline. Returns a list of
     prospect dicts with pre-assigned IDs shared across all 6 SF files.

  2. build_sf_crm_files()       — single entry point that calls the
     registry builder then writes all 6 files in one pass, guaranteeing
     relational coherence.

Registry structure per prospect:
  {
    lead_id:          "00Q..."   # Salesforce Lead ID — root identity
    contact_id:       "003..."   # Only set if converted
    opportunity_id:   "006..."   # Only set if converted
    property_key:     int
    property_name:    str
    vendor_key:       int        # First-touch vendor
    utm_source:       str
    utm_medium:       str
    lead_source:      str
    first_name:       str
    last_name:        str
    email:            str
    phone:            str
    created_dt:       datetime   # When lead entered CRM
    converted:        bool
    conversion_dt:    datetime   # Only if converted (lease date)
    monthly_rent:     float      # Only if converted
    unit_type:        str        # Only if converted
    journey_length:   int        # 1, 2, or 3
    touches:          list of {
                        task_id, vendor_key, channel_key,
                        touch_type, activity_dt, days_before_lease
                      }
    campaign_id:      str
    is_organic:       bool       # True if VendorKey=12
  }
"""


# ── Constants matching generator baseline ──────────────────────────
JOURNEY_MIX = {1: 0.401, 2: 0.350, 3: 0.249}  # from measured data

# Direct-touch vendor weights (who closes the deal)
DIRECT_TOUCH_WEIGHTS = {
    1: 0.342,   # Zillow
    2: 0.292,   # Apartments.com
    3: 0.151,   # Apartment List
    4: 0.146,   # Google Ads
    5: 0.054,   # Bing Ads
    6: 0.004,   # Facebook
    7: 0.003,   # Instagram
    8: 0.000,   # Google Display (rarely direct)
    9: 0.000,   # StackAdapt
    10: 0.000,  # TradeDesk
    11: 0.007,  # Email
    12: 0.000,  # Organic/Direct (never IsDirectCredit)
}

# Lead volume weights by vendor (who generates top-of-funnel leads)
LEAD_VOLUME_WEIGHTS = {
    1: 0.344,   # Zillow
    2: 0.315,   # Apartments.com
    3: 0.138,   # Apartment List
    4: 0.136,   # Google Ads
    5: 0.048,   # Bing Ads
    6: 0.007,   # Facebook
    7: 0.004,   # Instagram
    8: 0.001,   # Google Display
    9: 0.001,   # StackAdapt
    10: 0.001,  # TradeDesk
    11: 0.005,  # Email
    12: 0.000,  # Organic — weight is 0 in the weighted ecosystem by design.
               # Organic leads are NOT emergent from this weight table.
               # They are forced via a hardcoded 3% rule in build_prospect_registry()
               # applied to non-converting leads only. This ensures the organic
               # code path is always exercised without distorting the vendor mix.
               # In production, organic would emerge naturally from CRM lead source data.
}

# Vendor → UTM source/medium/lead source mapping
VENDOR_UTM = {
    1:  ("zillow",          "ils",         "Zillow"),
    2:  ("apartments.com",  "ils",         "Apartments.com"),
    3:  ("apartment_list",  "ils",         "Apartment List"),
    4:  ("google",          "cpc",         "Web"),
    5:  ("bing",            "cpc",         "Web"),
    6:  ("facebook",        "paid_social", "Social Media"),
    7:  ("instagram",       "paid_social", "Social Media"),
    8:  ("google",          "display",     "Web"),
    9:  ("stackadapt",      "display",     "Web"),
    10: ("tradedesk",       "display",     "Web"),
    11: ("email",           "email",       "Email"),
    12: (None,              None,          "Web"),  # organic/direct
}

# Vendor → channel key
VENDOR_CHANNEL = {
    1:1, 2:1, 3:1,          # ILS
    4:2, 5:2,               # Paid Search
    6:3, 7:3,               # Paid Social
    8:4, 9:4, 10:4,         # Display
    11:5,                   # Email
    12:6,                   # Organic
}

# TouchChannel__c values for Task records
VENDOR_TOUCH_CHANNEL = {
    1:"ILS", 2:"ILS", 3:"ILS",
    4:"Paid Search", 5:"Paid Search",
    6:"Paid Social", 7:"Paid Social",
    8:"Display", 9:"Display", 10:"Display",
    11:"Email",
    12:"Direct",
}

# Vendor affinity by journey position
# Top-of-funnel: Display, Social, ILS
# Bottom-of-funnel: Paid Search, ILS, Email
EARLY_TOUCH_WEIGHTS = {
    1:0.25, 2:0.22, 3:0.10,  # ILS heavy early
    4:0.08, 5:0.03,
    6:0.12, 7:0.10,           # Social heavy early
    8:0.05, 9:0.03, 10:0.02,
    11:0.00, 12:0.00,
}
MID_TOUCH_WEIGHTS = {
    1:0.20, 2:0.18, 3:0.10,
    4:0.15, 5:0.08,
    6:0.10, 7:0.08,
    8:0.04, 9:0.04, 10:0.03,
    11:0.00, 12:0.00,
}

FIRST_NAMES = ["James","Sarah","Michael","Jennifer","David","Emily","Robert","Ashley",
               "William","Jessica","Christopher","Amanda","Daniel","Stephanie","Matthew",
               "Lisa","Anthony","Patricia","Mark","Sandra","Steven","Deborah","Paul",
               "Dorothy","Andrew","Melissa","Kenneth","Nancy","George","Betty"]
LAST_NAMES  = ["Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez",
               "Martinez","Hernandez","Lopez","Wilson","Anderson","Thomas","Taylor",
               "Moore","Jackson","Martin","Lee","Perez","Thompson","White","Harris",
               "Sanchez","Clark","Ramirez","Lewis","Robinson","Walker","Young"]
UNIT_TYPES  = ["Studio","1BR","1BR","2BR","2BR","2BR","3BR"]
RENT_RANGES = {"Studio":(1200,1900),"1BR":(1400,2500),"2BR":(1800,3400),"3BR":(2200,4200)}
EMAIL_DOMAINS = ["gmail.com","yahoo.com","outlook.com","icloud.com","hotmail.com"]


def weighted_choice(weights_dict):
    keys = list(weights_dict.keys())
    weights = list(weights_dict.values())
    return random.choices(keys, weights=weights, k=1)[0]


def build_prospect_registry(run_dt, props, monthly_leases_by_prop, monthly_leads_by_prop):
    """
    Build the prospect registry for the run month.
    
    For each property:
      - Determine how many conversions should happen this month (from baseline)
      - Determine how many of those fall on the run date (proportional daily split)
      - Build converting prospects anchored to run_dt as their conversion date
      - Build non-converting prospects (leads only) for run_dt
      - Assign consistent IDs used by all 6 SF files
    """
    run_ym = int(run_dt.strftime("%Y%m"))
    days_in_month = 31 if run_dt.month in [1,3,5,7,8,10,12] else (
                    30 if run_dt.month in [4,6,9,11] else (
                    29 if run_dt.year % 4 == 0 else 28))
    
    registry = []
    
    for pk, prop in sorted(props.items()):
        # Monthly totals from gold baseline
        monthly_leases = monthly_leases_by_prop[pk].get(run_ym, 0)
        monthly_leads_vol = monthly_leads_by_prop[pk].get(run_ym, 0)
        
        # If no baseline for this month yet, estimate from prior month
        if monthly_leases == 0:
            prior_months = sorted(monthly_leases_by_prop[pk].keys())
            if prior_months:
                monthly_leases = monthly_leases_by_prop[pk][prior_months[-1]]
            else:
                monthly_leases = 15  # fallback
        if monthly_leads_vol == 0:
            prior_months = sorted(monthly_leads_by_prop[pk].keys())
            if prior_months:
                monthly_leads_vol = monthly_leads_by_prop[pk][prior_months[-1]]
            else:
                monthly_leads_vol = 500

        # Daily proportional split with noise
        daily_conversion_rate = 1.0 / days_in_month
        n_conversions_today = max(0, round(
            monthly_leases * daily_conversion_rate * random.uniform(0.6, 1.4)
        ))
        
        # Daily leads — roughly proportional, slightly more noise
        n_leads_today = max(1, round(
            monthly_leads_vol * daily_conversion_rate * random.uniform(0.7, 1.3)
        ))
        
        # Non-converting leads = total leads - converting
        n_non_converting = max(0, n_leads_today - n_conversions_today)
        
        # Build converting prospects
        for i in range(n_conversions_today):
            lead_id     = sf_id("00Q")
            contact_id  = sf_id("003")
            opp_id      = sf_id("006")
            
            # Conversion happens today — lease start is 1-30 days in future
            lease_start = run_dt + datetime.timedelta(days=random.randint(1, 30))
            
            # Journey length
            journey_len = weighted_choice(JOURNEY_MIX)
            
            # Direct (closing) vendor drawn from direct-touch weights
            direct_vk = weighted_choice(DIRECT_TOUCH_WEIGHTS)
            
            # First touch vendor — top-of-funnel if multi-touch
            if journey_len == 1:
                first_vk = direct_vk
            else:
                first_vk = weighted_choice(EARLY_TOUCH_WEIGHTS)
                # Make sure first touch isn't same as direct for 2+ touch journeys
                if first_vk == direct_vk:
                    first_vk = weighted_choice(
                        {vk:w for vk,w in EARLY_TOUCH_WEIGHTS.items() if vk != direct_vk}
                    )
            
            utm_src, utm_med, lead_src = VENDOR_UTM[first_vk]
            unit_type = random.choice(UNIT_TYPES)
            rent = round(random.uniform(*RENT_RANGES[unit_type]), 2)
            
            # Lead created date — within 7 days before conversion (attribution window)
            days_back = random.randint(0, 7)
            lead_created = run_dt - datetime.timedelta(
                days=days_back,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            # Build touch sequence — anchor to lease_start, not run_dt.
            # The opportunity's LeaseStartDate__c is run_dt + 1-30 days,
            # so anchoring touches to run_dt put them outside the CRM
            # pipeline's 7-day attribution window. Pass lease_start in
            # explicitly so touch dates align with the lease date the
            # CRM pipeline reads from sf_opportunities_raw.
            touches = _build_touches(
                lead_id, contact_id, pk, prop,
                journey_len, first_vk, direct_vk,
                run_dt, days_back, lease_start
            )
            
            first = random.choice(FIRST_NAMES)
            last  = random.choice(LAST_NAMES)
            
            registry.append({
                "lead_id":       lead_id,
                "contact_id":    contact_id,
                "opportunity_id": opp_id,
                "property_key":  pk,
                "property_name": prop["PropertyName"],
                "property_code": f"PROP{pk:05d}",
                "market":        prop["MarketName"],
                "state":         prop["State"],
                "city":          prop["City"],
                "vendor_key":    first_vk,
                "utm_source":    utm_src,
                "utm_medium":    utm_med,
                "lead_source":   lead_src,
                "first_name":    first,
                "last_name":     last,
                "email":         f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(EMAIL_DOMAINS)}",
                "phone":         f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}",
                "created_dt":    lead_created,
                "converted":     True,
                "conversion_dt": run_dt,
                "lease_start":   lease_start,
                "monthly_rent":  rent,
                "unit_type":     unit_type,
                "journey_length": journey_len,
                "touches":       touches,
                "campaign_id":   f"CAMP{first_vk:04d}{run_dt.strftime('%Y%m')}",
                "is_organic":    (first_vk == 12),
            })
        
        # Build non-converting prospects (leads only, no opportunity)
        # Force ~3% to be organic/direct (VendorKey 12) so that code path is exercised
        for i in range(n_non_converting):
            lead_id = sf_id("00Q")
            if random.random() < 0.03:
                vk = 12  # Organic / Direct — forced sample for pipeline testing
            else:
                vk = weighted_choice(LEAD_VOLUME_WEIGHTS)
            utm_src, utm_med, lead_src = VENDOR_UTM[vk]
            
            lead_created = run_dt.replace(
                hour=random.randint(7, 22),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
            
            # Non-converting: 1 touch only (the lead inquiry itself)
            touch_dt = lead_created
            task_id = sf_id("00T")
            touches = [{
                "task_id":    task_id,
                "who_id":     lead_id,
                "vendor_key": vk,
                "channel_key": VENDOR_CHANNEL[vk],
                "touch_channel": VENDOR_TOUCH_CHANNEL[vk],
                "touch_type": "Lead",
                "subject":    "New Lead Inquiry",
                "activity_dt": touch_dt,
                "days_before_lease": None,
                "what_id":    f"CAMP{vk:04d}{run_dt.strftime('%Y%m')}",
            }]
            
            first = random.choice(FIRST_NAMES)
            last  = random.choice(LAST_NAMES)
            
            registry.append({
                "lead_id":       lead_id,
                "contact_id":    None,
                "opportunity_id": None,
                "property_key":  pk,
                "property_name": prop["PropertyName"],
                "property_code": f"PROP{pk:05d}",
                "market":        prop["MarketName"],
                "state":         prop["State"],
                "city":          prop["City"],
                "vendor_key":    vk,
                "utm_source":    utm_src,
                "utm_medium":    utm_med,
                "lead_source":   lead_src,
                "first_name":    first,
                "last_name":     last,
                "email":         f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(EMAIL_DOMAINS)}",
                "phone":         f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}",
                "created_dt":    lead_created,
                "converted":     False,
                "conversion_dt": None,
                "lease_start":   None,
                "monthly_rent":  None,
                "unit_type":     None,
                "journey_length": 1,
                "touches":       touches,
                "campaign_id":   f"CAMP{vk:04d}{run_dt.strftime('%Y%m')}",
                "is_organic":    (vk == 12),
            })
    
    return registry


def _build_touches(lead_id, contact_id, pk, prop,
                   journey_len, first_vk, direct_vk,
                   run_dt, days_before_lease, lease_start):
    """
    Build 1-3 touch records for a converting prospect.
    Uses journey-shape-preserving selection:
      Touch 1 = earliest (initiator)   = first_vk
      Touch 2 = middle (influencer)    = mid-funnel vendor
      Touch 3 = latest (converter)     = direct_vk
    All touches within attribution window (7 days before lease).

    IMPORTANT — anchor logic:
      Touches must be anchored to LEASE_START, not RUN_DT. The opportunity
      writes LeaseStartDate__c = lease_start (which is run_dt + 1-30 days),
      and the CRM pipeline computes days_before_lease against that value.
      Anchoring to run_dt would put every touch 1-30 days too early and
      they would all fall outside the 7-day attribution window.
      `what_id` (campaign code) intentionally stays on run_dt.strftime('%Y%m')
      because campaign_vendor_lookup is keyed by lead-conversion month,
      not future lease month.
    """
    touches = []
    lease_dt = lease_start  # anchor for ActivityDate generation

    # Earliest a touch's date portion can land without falling outside
    # the CRM pipeline's 7-day attribution window. Without this clamp,
    # a touch with t_days=7 plus an hour offset of 8-20 would subtract
    # across midnight and produce a date 8 calendar days before lease,
    # which the pipeline would reject as OUT_OF_WINDOW even though the
    # intent was "7 days before lease". This is a real bug we caught
    # from production dry-run logs — do not remove.
    ATTR_WINDOW_DAYS = 7
    min_allowed_date = (lease_dt - datetime.timedelta(days=ATTR_WINDOW_DAYS)).date()

    def _clamp_touch_dt(dt):
        """Ensure dt.date() >= min_allowed_date. If hour offset rolled it
        past midnight into day-8-before-lease territory, snap the date
        forward to min_allowed_date while preserving the time-of-day."""
        if dt.date() < min_allowed_date:
            return datetime.datetime.combine(min_allowed_date, dt.time())
        return dt

    if journey_len == 1:
        # Single touch — direct vendor, day of or 1 day before lease
        days_back = random.randint(0, 1)
        touch_dt = lease_dt - datetime.timedelta(
            days=days_back,
            hours=random.randint(0, 12)
        )
        touch_dt = _clamp_touch_dt(touch_dt)
        touches.append({
            "task_id":      sf_id("00T"),
            "who_id":       lead_id,
            "vendor_key":   direct_vk,
            "channel_key":  VENDOR_CHANNEL[direct_vk],
            "touch_channel": VENDOR_TOUCH_CHANNEL[direct_vk],
            "touch_type":   "Lease",
            "subject":      "Lease Application Completed",
            "activity_dt":  touch_dt,
            "days_before_lease": days_back,
            "what_id":      f"CAMP{direct_vk:04d}{run_dt.strftime('%Y%m')}",
        })

    elif journey_len == 2:
        # Touch 1 (initiator): 5-7 days before lease
        t1_days = random.randint(5, 7)
        t1_dt = lease_dt - datetime.timedelta(days=t1_days, hours=random.randint(8, 20))
        t1_dt = _clamp_touch_dt(t1_dt)

        # Touch 2 (converter): 0-2 days before lease
        t2_days = random.randint(0, 2)
        t2_dt = lease_dt - datetime.timedelta(days=t2_days, hours=random.randint(8, 18))
        t2_dt = _clamp_touch_dt(t2_dt)

        touches = [
            {
                "task_id":      sf_id("00T"),
                "who_id":       lead_id,
                "vendor_key":   first_vk,
                "channel_key":  VENDOR_CHANNEL[first_vk],
                "touch_channel": VENDOR_TOUCH_CHANNEL[first_vk],
                "touch_type":   "Lead",
                "subject":      "Lead Inquiry - " + prop["PropertyName"],
                "activity_dt":  t1_dt,
                "days_before_lease": t1_days,
                "what_id":      f"CAMP{first_vk:04d}{run_dt.strftime('%Y%m')}",
            },
            {
                "task_id":      sf_id("00T"),
                "who_id":       contact_id,  # converted to contact by touch 2
                "vendor_key":   direct_vk,
                "channel_key":  VENDOR_CHANNEL[direct_vk],
                "touch_channel": VENDOR_TOUCH_CHANNEL[direct_vk],
                "touch_type":   "Lease",
                "subject":      "Lease Application Completed",
                "activity_dt":  t2_dt,
                "days_before_lease": t2_days,
                "what_id":      f"CAMP{direct_vk:04d}{run_dt.strftime('%Y%m')}",
            },
        ]

    else:  # journey_len == 3
        # Touch 1 (initiator): 6-7 days before
        t1_days = random.randint(6, 7)
        t1_dt = lease_dt - datetime.timedelta(days=t1_days, hours=random.randint(8, 20))
        t1_dt = _clamp_touch_dt(t1_dt)

        # Touch 2 (influencer): 3-5 days before (midpoint)
        t2_days = random.randint(3, 5)
        t2_dt = lease_dt - datetime.timedelta(days=t2_days, hours=random.randint(8, 20))
        t2_dt = _clamp_touch_dt(t2_dt)

        # Touch 3 (converter): 0-2 days before
        t3_days = random.randint(0, 2)
        t3_dt = lease_dt - datetime.timedelta(days=t3_days, hours=random.randint(8, 18))
        t3_dt = _clamp_touch_dt(t3_dt)

        mid_vk = weighted_choice(MID_TOUCH_WEIGHTS)

        touches = [
            {
                "task_id":      sf_id("00T"),
                "who_id":       lead_id,
                "vendor_key":   first_vk,
                "channel_key":  VENDOR_CHANNEL[first_vk],
                "touch_channel": VENDOR_TOUCH_CHANNEL[first_vk],
                "touch_type":   "Lead",
                "subject":      "Lead Inquiry - " + prop["PropertyName"],
                "activity_dt":  t1_dt,
                "days_before_lease": t1_days,
                "what_id":      f"CAMP{first_vk:04d}{run_dt.strftime('%Y%m')}",
            },
            {
                "task_id":      sf_id("00T"),
                "who_id":       lead_id,
                "vendor_key":   mid_vk,
                "channel_key":  VENDOR_CHANNEL[mid_vk],
                "touch_channel": VENDOR_TOUCH_CHANNEL[mid_vk],
                "touch_type":   "Tour",
                "subject":      "Tour Scheduled - " + prop["PropertyName"],
                "activity_dt":  t2_dt,
                "days_before_lease": t2_days,
                "what_id":      f"CAMP{mid_vk:04d}{run_dt.strftime('%Y%m')}",
            },
            {
                "task_id":      sf_id("00T"),
                "who_id":       contact_id,  # converted to contact
                "vendor_key":   direct_vk,
                "channel_key":  VENDOR_CHANNEL[direct_vk],
                "touch_channel": VENDOR_TOUCH_CHANNEL[direct_vk],
                "touch_type":   "Lease",
                "subject":      "Lease Application Completed",
                "activity_dt":  t3_dt,
                "days_before_lease": t3_days,
                "what_id":      f"CAMP{direct_vk:04d}{run_dt.strftime('%Y%m')}",
            },
        ]

    return touches


def write_crm_files(run_dt, registry, out_dir):
    """Write all 6 SF files from the registry in one pass."""
    date_str = run_dt.strftime('%Y%m%d')
    
    # Separate converting and non-converting
    converted    = [p for p in registry if p['converted']]
    non_converted = [p for p in registry if not p['converted']]
    all_prospects = registry
    
    print(f"  Registry: {len(registry):,} prospects "
          f"({len(converted):,} converting, {len(non_converted):,} non-converting)")

    # ── sf_leads_raw ──────────────────────────────────────────────────
    fname = os.path.join(out_dir, f"sf_leads_raw_{date_str}.csv")
    rows = []
    for p in all_prospects:
        rows.append({
            "Id": p['lead_id'],
            "CreatedDate": sf_datetime(p['created_dt']),
            "SystemModstamp": sf_datetime(p['created_dt']),
            "FirstName": p['first_name'],
            "LastName":  p['last_name'],
            "Email":     p['email'],
            "Phone":     p['phone'],
            "MobilePhone": "",
            "LeadSource": p['lead_source'],
            "Status": "Converted" if p['converted'] else random.choice(["New","New","Working"]),
            "IsConverted": "True" if p['converted'] else "False",
            "ConvertedDate": sf_date(p['conversion_dt']) if p['converted'] else "",
            "ConvertedOpportunityId": p['opportunity_id'] if p['converted'] else "",
            "Property__c":     p['property_code'],
            "PropertyName__c": p['property_name'],
            "PropertyState__c": p['state'],
            "Campaign__c":     p['campaign_id'],
            "CampaignName__c": f"{p['market']} - {p['utm_source'] or 'direct'} - {run_dt.strftime('%b %Y')}",
            "UTM_Source__c":   p['utm_source'] or "",
            "UTM_Medium__c":   p['utm_medium'] or "",
            "NumberOfEmployees": "",
            "AnnualRevenue": "",
            "OwnerId": sf_id("005"),
            "IsDeleted": "False",
            "LastActivityDate": sf_date(run_dt),
            "Description": "",
        })
    with open(fname, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader(); writer.writerows(rows)
    print(f"  Written: sf_leads_raw_{date_str}.csv ({len(rows):,} rows)")

    # ── sf_contacts_raw ───────────────────────────────────────────────
    fname = os.path.join(out_dir, f"sf_contacts_raw_{date_str}.csv")
    rows = []
    for p in converted:
        rows.append({
            "Id": p['contact_id'],
            "CreatedDate": sf_datetime(p['conversion_dt']),
            "SystemModstamp": sf_datetime(p['conversion_dt']),
            "FirstName": p['first_name'],
            "LastName":  p['last_name'],
            "Email":     p['email'],
            "Phone":     p['phone'],
            "MobilePhone": "",
            "Property__c":     p['property_code'],
            "PropertyName__c": p['property_name'],
            "LeadSource": p['lead_source'],
            "AccountId": sf_id("001"),
            "OwnerId":   sf_id("005"),
            "IsDeleted": "False",
            "LastModifiedDate": sf_datetime(p['conversion_dt']),
            "DoNotCall": "False",
            "HasOptedOutOfEmail": "False",
        })
    with open(fname, 'w', newline='') as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader(); writer.writerows(rows)
    print(f"  Written: sf_contacts_raw_{date_str}.csv ({len(rows):,} rows)")

    # ── sf_opportunities_raw ──────────────────────────────────────────
    fname = os.path.join(out_dir, f"sf_opportunities_raw_{date_str}.csv")
    rows = []
    for p in converted:
        lease_end = p['lease_start'] + datetime.timedelta(days=random.randint(365, 545))
        rows.append({
            "Id": p['opportunity_id'],
            "CreatedDate": sf_datetime(p['conversion_dt']),
            "SystemModstamp": sf_datetime(p['conversion_dt']),
            "Name": f"Lease - {p['property_name'][:20]} - {date_str}",
            "StageName": "Closed Won",
            "CloseDate": sf_date(p['conversion_dt']),
            "Amount": f"{p['monthly_rent'] * 12:.2f}",
            "Probability": "100",
            "LeadId__c": p['lead_id'],          # ← real lead ID from registry
            "Property__c": p['property_code'],
            "PropertyName__c": p['property_name'],
            "UnitType__c": p['unit_type'],
            "MonthlyRent__c": f"{p['monthly_rent']:.2f}",
            "LeaseStartDate__c": sf_date(p['lease_start']),
            "LeaseEndDate__c": sf_date(lease_end),
            "CampaignId": p['campaign_id'],
            "OwnerId": sf_id("005"),
            "IsDeleted": "False",
            "LastModifiedDate": sf_datetime(p['conversion_dt']),
            "LostReason__c": "",
        })
    with open(fname, 'w', newline='') as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader(); writer.writerows(rows)
    print(f"  Written: sf_opportunities_raw_{date_str}.csv ({len(rows):,} rows)")

    # ── sf_tasks_raw ──────────────────────────────────────────────────
    fname = os.path.join(out_dir, f"sf_tasks_raw_{date_str}.csv")
    rows = []
    for p in all_prospects:
        for touch in p['touches']:
            rows.append({
                "Id": touch['task_id'],
                "CreatedDate": sf_datetime(touch['activity_dt']),
                "SystemModstamp": sf_datetime(touch['activity_dt']),
                "ActivityDate": sf_date(touch['activity_dt']),
                "Subject": touch['subject'],
                "Status": "Completed",
                "Priority": "Normal",
                "WhoId": touch['who_id'],      # ← real lead_id or contact_id
                "WhatId": touch['what_id'],
                "OwnerId": sf_id("005"),
                "IsDeleted": "False",
                "Type": touch['touch_type'],
                "Description": f"{touch['touch_type']} at {p['property_name']}",
                "IsClosed": "True",
                "IsArchived": "False",
                "Property__c": p['property_code'],
                "TouchChannel__c": touch['touch_channel'],
                "LastModifiedDate": sf_datetime(touch['activity_dt']),
            })
    with open(fname, 'w', newline='') as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader(); writer.writerows(rows)
    print(f"  Written: sf_tasks_raw_{date_str}.csv ({len(rows):,} rows, "
          f"avg {len(rows)/len(all_prospects):.1f} touches/prospect)")

    # ── sf_campaigns_raw ──────────────────────────────────────────────
    fname = os.path.join(out_dir, f"sf_campaigns_raw_{date_str}.csv")
    vendor_info = {
        1:("Zillow","ILS"), 2:("Apartments.com","ILS"), 3:("Apartment List","ILS"),
        4:("Google Ads","Paid Search"), 5:("Bing Ads","Paid Search"),
        6:("Facebook","Paid Social"), 7:("Instagram","Paid Social"),
        8:("Google Display","Display"), 9:("StackAdapt","Display"),
        10:("TradeDesk","Display"), 11:("Email Marketing","Email"),
        12:("Organic / Direct","Organic"),
    }
    rows = []
    for vk, (vname, channel) in vendor_info.items():
        month_label = run_dt.strftime("%b %Y")
        rows.append({
            "Id": f"CAMP{vk:04d}{run_dt.strftime('%Y%m')}",
            "CreatedDate": sf_date(run_dt.replace(day=1)),
            "SystemModstamp": sf_datetime(run_dt),
            "Name": f"{vname} - {month_label}",
            "VendorName__c": vname,
            "ChannelType__c": channel,
            "Status": "Active",
            "BudgetedCost": str(random.randint(5000, 25000)),
            "ActualCost": f"{random.uniform(4000, 24000):.2f}",
            "StartDate": sf_date(run_dt.replace(day=1)),
            "EndDate": sf_date((run_dt.replace(day=28) + datetime.timedelta(days=4)).replace(day=1) - datetime.timedelta(days=1)),
            "IsDeleted": "False",
            "IsActive": "True",
            "OwnerId": sf_id("005"),
            "LastModifiedDate": sf_datetime(run_dt),
            "NumberOfLeads": str(random.randint(50, 800)),
            "NumberOfConvertedLeads": str(random.randint(2, 40)),
            "Description": f"Monthly {channel} campaign — {vname}",
        })
    with open(fname, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader(); writer.writerows(rows)
    print(f"  Written: sf_campaigns_raw_{date_str}.csv ({len(rows)} rows, includes Organic VK12)")

    # ── sf_campaign_members_raw ───────────────────────────────────────
    fname = os.path.join(out_dir, f"sf_campaign_members_raw_{date_str}.csv")
    rows = []
    for p in all_prospects:
        responded_dt = p['created_dt'] + datetime.timedelta(
            hours=random.randint(0, 4), minutes=random.randint(0, 59)
        )
        # HasResponded and FirstRespondedDate must be consistent:
        #   HasResponded=True  → FirstRespondedDate must be populated
        #   HasResponded=False → FirstRespondedDate must be blank
        has_responded = True if p['converted'] else (random.random() > 0.35)
        rows.append({
            "Id": sf_id("00v"),
            "CreatedDate": sf_datetime(responded_dt),
            "SystemModstamp": sf_datetime(responded_dt),
            "LeadId": p['lead_id'],              # ← real lead ID from registry
            "ContactId": p['contact_id'] or "",
            "CampaignId": p['campaign_id'],
            "Status": "Responded" if has_responded else "Sent",
            "HasResponded": "True" if has_responded else "False",
            "FirstRespondedDate": sf_date(responded_dt) if has_responded else "",
            "IsDeleted": "False",
            "LastModifiedDate": sf_datetime(responded_dt),
            "Type": "Lead",
        })
    with open(fname, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader(); writer.writerows(rows)
    print(f"  Written: sf_campaign_members_raw_{date_str}.csv ({len(rows):,} rows)")

    # Relational integrity check
    lead_ids_in_leads = {p['lead_id'] for p in all_prospects}
    lead_ids_in_opps  = {p['lead_id'] for p in converted}
    task_who_ids      = {t['who_id'] for p in all_prospects for t in p['touches']}
    member_lead_ids   = {p['lead_id'] for p in all_prospects}
    
    orphan_tasks = task_who_ids - lead_ids_in_leads - {p['contact_id'] for p in converted if p['contact_id']}
    orphan_opps  = lead_ids_in_opps - lead_ids_in_leads
    
    print(f"\n  INTEGRITY CHECK:")
    print(f"    Leads:             {len(all_prospects):,}")
    print(f"    Contacts:          {len(converted):,} (converting only)")
    print(f"    Opportunities:     {len(converted):,} (1 per converted lead)")
    print(f"    Tasks:             {sum(len(p['touches']) for p in all_prospects):,}")
    print(f"    Campaign members:  {len(all_prospects):,}")
    print(f"    Orphan task WhoIds: {len(orphan_tasks)} (should be 0)")
    print(f"    Orphan opp LeadIds: {len(orphan_opps)} (should be 0)")
    print(f"    Conversion rate:   {len(converted)/len(all_prospects):.1%}")


# ─────────────────────────────────────────────────────────────────────────────
# SPEND PIPELINE SOURCES
# ─────────────────────────────────────────────────────────────────────────────

def build_google_ads_export(run_dt, props, spend, funnel, out_dir):
    """
    Google Ads UI export format.
    Covers VK4 (Search campaigns) and VK8 (Display campaigns).
    Real Google Ads exports use this exact column structure.
    Campaign naming convention: "{PropertyName} - {CampaignType}"
    """
    fname = os.path.join(out_dir, f"google_ads_export_{run_dt.strftime('%Y%m%d')}.csv")
    rows = []

    # Google Ads report header (matches real UI export)
    report_header = [
        "Google Ads",
        f"Account: NorthStar Residential Group",
        f"Date range: {google_date(run_dt)} - {google_date(run_dt)}",
        "",
    ]

    for pk, prop in sorted(props.items()):
        prop_name = prop["PropertyName"]
        market    = prop["MarketName"]

        # Search campaigns (VK4 — Google Ads)
        search_spend = noise(spend[pk].get(VK_GOOGLE_ADS, 0))
        search_impr  = int(noise(funnel[pk][VK_GOOGLE_ADS].get(SK_IMPRESSIONS, 0)))
        search_clicks= int(noise(funnel[pk][VK_GOOGLE_ADS].get(SK_CLICKS, 0)))
        search_conv  = int(noise(funnel[pk][VK_GOOGLE_ADS].get(SK_LEASES, 0)))
        search_ctr   = round(search_clicks / search_impr, 4) if search_impr > 0 else 0
        search_cpc   = round(search_spend / search_clicks, 2) if search_clicks > 0 else 0
        search_cpa   = round(search_spend / search_conv, 2) if search_conv > 0 else 0

        rows.append({
            "Campaign": f"{prop_name} - Brand",
            "Campaign ID": str(random.randint(1000000000, 9999999999)),
            "Campaign type": "Search",
            "Campaign status": "Enabled",
            "Ad group": f"{prop_name} Brand - Exact",
            "Ad group status": "Enabled",
            "Day": google_date(run_dt),
            "Impressions": search_impr,
            "Clicks": search_clicks,
            "CTR": f"{search_ctr:.2%}",
            "Avg. CPC": f"${search_cpc:.2f}",
            "Cost": f"${search_spend:.2f}",
            "Conversions": search_conv,
            "Cost / conv.": f"${search_cpa:.2f}",
            "Conv. rate": f"{round(search_conv/search_clicks,4):.2%}" if search_clicks > 0 else "0.00%",
            "Search impr. share": f"{random.uniform(0.55, 0.85):.2%}",
            "Quality Score": str(random.randint(6, 10)),
            "Market": market,
            "Property ID": str(pk),
        })

        rows.append({
            "Campaign": f"{prop_name} - Non-Brand",
            "Campaign ID": str(random.randint(1000000000, 9999999999)),
            "Campaign type": "Search",
            "Campaign status": "Enabled",
            "Ad group": f"Apartments {prop['City']} - Broad",
            "Ad group status": "Enabled",
            "Day": google_date(run_dt),
            "Impressions": int(search_impr * random.uniform(0.3, 0.6)),
            "Clicks": int(search_clicks * random.uniform(0.15, 0.35)),
            "CTR": f"{random.uniform(0.02, 0.06):.2%}",
            "Avg. CPC": f"${random.uniform(1.5, 4.5):.2f}",
            "Cost": f"${search_spend * random.uniform(0.2, 0.4):.2f}",
            "Conversions": max(0, int(search_conv * random.uniform(0.1, 0.3))),
            "Cost / conv.": f"${random.uniform(80, 200):.2f}",
            "Conv. rate": f"{random.uniform(0.005, 0.02):.2%}",
            "Search impr. share": f"{random.uniform(0.10, 0.30):.2%}",
            "Quality Score": str(random.randint(4, 7)),
            "Market": market,
            "Property ID": str(pk),
        })

        # Display campaigns (VK8 — Google Display Network)
        disp_spend  = noise(spend[pk].get(VK_GOOGLE_DISP, 0))
        disp_impr   = int(noise(funnel[pk][VK_GOOGLE_DISP].get(SK_IMPRESSIONS, 0)))
        disp_clicks = int(noise(funnel[pk][VK_GOOGLE_DISP].get(SK_CLICKS, 0)))
        disp_ctr    = round(disp_clicks / disp_impr, 5) if disp_impr > 0 else 0

        rows.append({
            "Campaign": f"{prop_name} - Remarketing",
            "Campaign ID": str(random.randint(1000000000, 9999999999)),
            "Campaign type": "Display",
            "Campaign status": "Enabled",
            "Ad group": "Site Visitors - 30D",
            "Ad group status": "Enabled",
            "Day": google_date(run_dt),
            "Impressions": disp_impr,
            "Clicks": disp_clicks,
            "CTR": f"{disp_ctr:.2%}",
            "Avg. CPC": f"${disp_spend/disp_clicks:.2f}" if disp_clicks > 0 else "$0.00",
            "Cost": f"${disp_spend:.2f}",
            "Conversions": 0,
            "Cost / conv.": "$0.00",
            "Conv. rate": "0.00%",
            "Search impr. share": "--",
            "Quality Score": "--",
            "Market": market,
            "Property ID": str(pk),
        })

    with open(fname, "w", newline="") as f:
        # Write Google-style report headers
        for line in report_header:
            f.write(line + "\n")
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"  Written: {fname} ({len(rows)} rows)")
    return fname


def build_bing_ads_export(run_dt, props, spend, funnel, out_dir):
    """
    Microsoft Advertising (Bing Ads) report format.
    Downloaded from Microsoft Advertising UI → Reports → Performance Reports.
    VK5 only.
    """
    fname = os.path.join(out_dir, f"bing_ads_export_{run_dt.strftime('%Y%m%d')}.csv")
    rows = []

    for pk, prop in sorted(props.items()):
        prop_name = prop["PropertyName"]
        bing_spend  = noise(spend[pk].get(VK_BING, 0))
        bing_impr   = int(noise(funnel[pk][VK_BING].get(SK_IMPRESSIONS, 0)))
        bing_clicks = int(noise(funnel[pk][VK_BING].get(SK_CLICKS, 0)))
        bing_conv   = int(noise(funnel[pk][VK_BING].get(SK_LEASES, 0)))

        rows.append({
            "Account name": "NorthStar Residential Group",
            "Account number": "C01-" + str(random.randint(100000, 999999)),
            "Campaign name": f"{prop_name} | Search | Brand",
            "Campaign ID": str(random.randint(100000000, 999999999)),
            "Ad group": f"Brand - Exact Match",
            "Ad group ID": str(random.randint(10000000, 99999999)),
            "Time period": run_dt.strftime("%m/%d/%Y"),
            "Impressions": bing_impr,
            "Clicks": bing_clicks,
            "CTR (%)": f"{round(bing_clicks/bing_impr*100, 2):.2f}" if bing_impr > 0 else "0.00",
            "Avg. CPC": f"{bing_spend/bing_clicks:.2f}" if bing_clicks > 0 else "0.00",
            "Spend": f"{bing_spend:.2f}",
            "Conversions": bing_conv,
            "Revenue": f"{bing_conv * random.uniform(900, 1400):.2f}",
            "Avg. position": f"{random.uniform(1.1, 2.8):.1f}",
            "Quality score": str(random.randint(5, 9)),
            "Keyword": f"apartments {prop['City'].lower()}",
            "Match type": random.choice(["Exact", "Phrase", "Broad"]),
            "Device type": random.choice(["Computer", "Smartphone", "Tablet"]),
            "Network": "Microsoft sites",
            "Property_ID": str(pk),
        })

    with open(fname, "w", newline="") as f:
        # Bing reports have a report info header
        f.write(f"Report name: Campaign Performance Report\n")
        f.write(f"Report time: {run_dt.strftime('%m/%d/%Y')}\n")
        f.write(f"Request time: {sf_datetime(run_dt)}\n")
        f.write(f"Format: Csv\n\n")
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"  Written: {fname} ({len(rows)} rows)")
    return fname


def build_meta_ads_export(run_dt, props, spend, funnel, out_dir):
    """
    Meta Ads Manager export format.
    Covers VK6 (Facebook) and VK7 (Instagram) — both come from one Meta account.
    Exported via Ads Manager → Reports → Export.
    """
    fname = os.path.join(out_dir, f"meta_ads_export_{run_dt.strftime('%Y%m%d')}.csv")
    rows = []

    for pk, prop in sorted(props.items()):
        prop_name = prop["PropertyName"]

        for vk, platform, placement in [
            (VK_FACEBOOK,  "Facebook", "Facebook Feed"),
            (VK_INSTAGRAM, "Instagram", "Instagram Feed"),
        ]:
            plat_spend  = noise(spend[pk].get(vk, 0))
            plat_impr   = int(noise(funnel[pk][vk].get(SK_IMPRESSIONS, 0)))
            plat_clicks = int(noise(funnel[pk][vk].get(SK_CLICKS, 0)))
            plat_reach  = int(plat_impr * random.uniform(0.55, 0.75))
            plat_freq   = round(plat_impr / plat_reach, 2) if plat_reach > 0 else 1.0
            cpm         = round(plat_spend / plat_impr * 1000, 2) if plat_impr > 0 else 0
            cpc         = round(plat_spend / plat_clicks, 2) if plat_clicks > 0 else 0
            ctr         = round(plat_clicks / plat_impr * 100, 4) if plat_impr > 0 else 0

            rows.append({
                "Campaign name": f"MAA - {prop['State']} - Awareness - {prop_name[:20]}",
                "Campaign ID": str(random.randint(10000000000, 99999999999)),
                "Ad set name": f"{prop_name} - {platform} - Lookalike 2%",
                "Ad set ID": str(random.randint(10000000000, 99999999999)),
                "Ad name": f"Lifestyle Creative - Apr 2026",
                "Ad ID": str(random.randint(10000000000, 99999999999)),
                "Day": meta_date(run_dt),
                "Account name": "NorthStar Residential Group",
                "Account ID": "act_" + str(random.randint(100000000, 999999999)),
                "Platform": platform,
                "Placement": placement,
                "Objective": "REACH",
                "Buying type": "Auction",
                "Amount spent (USD)": f"{plat_spend:.2f}",
                "Reach": plat_reach,
                "Impressions": plat_impr,
                "Frequency": f"{plat_freq:.2f}",
                "Clicks (all)": plat_clicks,
                "Link clicks": int(plat_clicks * random.uniform(0.6, 0.8)),
                "CTR (all) (%)": f"{ctr:.4f}",
                "CPM (cost per 1,000 impressions)": f"{cpm:.2f}",
                "CPC (cost per link click)": f"{cpc:.2f}",
                "Post engagements": int(plat_clicks * random.uniform(1.5, 3.0)),
                "Video plays": int(plat_impr * random.uniform(0.1, 0.3)) if platform == "Instagram" else 0,
                "Results": int(noise(funnel[pk][vk].get(SK_LEADS, 0))),
                "Result indicator": "Leads",
                "Cost per result": f"{round(plat_spend / max(1, int(noise(funnel[pk][vk].get(SK_LEADS, 0)))), 2):.2f}",
                "Property_ID": str(pk),
            })

    with open(fname, "w", newline="") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"  Written: {fname} ({len(rows)} rows)")
    return fname


def build_zillow_export(run_dt, props, spend, funnel, leasing, out_dir):
    """
    Zillow Rental Manager performance report format.
    VK1. Zillow bills on a flat subscription + lead fees model.
    Weekly reporting period — we simulate a daily extract.
    """
    fname = os.path.join(out_dir, f"zillow_leads_{run_dt.strftime('%Y%m%d')}.csv")
    rows = []

    for pk, prop in sorted(props.items()):
        zil_spend   = noise(spend[pk].get(VK_ZILLOW, 0))
        zil_leads   = int(noise(funnel[pk][VK_ZILLOW].get(SK_LEADS, 0)))
        zil_visits  = int(noise(funnel[pk][VK_ZILLOW].get(SK_VISITS, 0)))
        zil_impr    = int(noise(funnel[pk][VK_ZILLOW].get(SK_IMPRESSIONS, 0)))
        phone_leads = int(zil_leads * random.uniform(0.15, 0.30))
        email_leads = zil_leads - phone_leads

        rows.append({
            "Property": prop["PropertyName"],
            "Property ID": f"ZIL{pk:05d}",
            "Address": prop["StreetAddress"],
            "City": prop["City"],
            "State": prop["State"],
            "Zip": prop["Zip"],
            "Report Date": run_dt.strftime("%m/%d/%Y"),
            "Listing Status": "Active",
            "Listing Type": "For Rent",
            "Unique Visitors": int(zil_visits * random.uniform(1.8, 2.5)),
            "Page Views": int(zil_impr * random.uniform(0.08, 0.15)),
            "Total Leads": zil_leads,
            "Email Leads": email_leads,
            "Phone Calls": phone_leads,
            "Tours Requested": int(zil_leads * random.uniform(0.10, 0.20)),
            "Applications": int(zil_leads * random.uniform(0.05, 0.12)),
            "Monthly Cost": f"${zil_spend:.2f}",
            "Cost Per Lead": f"${round(zil_spend / zil_leads, 2):.2f}" if zil_leads > 0 else "$0.00",
            "Avg. Price Shown": f"${random.randint(1400, 3200)}",
            "Units Available": prop.get("VacantUnits", 5),
            "Market": prop["MarketName"],
            "Property_Key": str(pk),
        })

    with open(fname, "w", newline="") as f:
        # Zillow exports have a title row
        f.write(f"Zillow Rental Manager — Performance Report\n")
        f.write(f"Generated: {run_dt.strftime('%B %d, %Y at %I:%M %p')} PT\n\n")
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"  Written: {fname} ({len(rows)} rows)")
    return fname


def build_apartments_com_export(run_dt, props, spend, funnel, out_dir):
    """
    CoStar / Apartments.com partner performance report.
    VK2. Downloaded from Apartments.com Partner Portal.
    CoStar acquired Apartments.com — reports carry CoStar branding.
    """
    fname = os.path.join(out_dir, f"apartments_com_{run_dt.strftime('%Y%m%d')}.csv")
    rows = []

    for pk, prop in sorted(props.items()):
        apts_spend  = noise(spend[pk].get(VK_APARTMENTS, 0))
        apts_leads  = int(noise(funnel[pk][VK_APARTMENTS].get(SK_LEADS, 0)))
        apts_visits = int(noise(funnel[pk][VK_APARTMENTS].get(SK_VISITS, 0)))
        apts_impr   = int(noise(funnel[pk][VK_APARTMENTS].get(SK_IMPRESSIONS, 0)))
        apts_clicks = int(noise(funnel[pk][VK_APARTMENTS].get(SK_CLICKS, 0)))

        rows.append({
            "Report Date": run_dt.strftime("%Y-%m-%d"),
            "Property Name": prop["PropertyName"],
            "CoStar Property ID": f"CS{pk:07d}",
            "Community ID": f"APT{pk:06d}",
            "Address": prop["StreetAddress"],
            "City": prop["City"],
            "State": prop["State"],
            "Zip Code": prop["Zip"],
            "Market": prop["MarketName"],
            "Listing Package": random.choice(["Platinum", "Gold", "Silver"]),
            "Listing Status": "Active",
            "Total Impressions": apts_impr,
            "Detail Page Views": apts_clicks,
            "Contact Requests": apts_leads,
            "Email Contacts": int(apts_leads * random.uniform(0.55, 0.70)),
            "Phone Contacts": int(apts_leads * random.uniform(0.20, 0.35)),
            "Chat Contacts": int(apts_leads * random.uniform(0.05, 0.15)),
            "Tour Requests": int(apts_leads * random.uniform(0.08, 0.18)),
            "Virtual Tour Views": int(apts_visits * random.uniform(0.05, 0.15)),
            "Saved Properties": int(apts_impr * random.uniform(0.002, 0.008)),
            "Monthly Spend": f"{apts_spend:.2f}",
            "Cost Per Contact": f"{round(apts_spend/apts_leads, 2):.2f}" if apts_leads > 0 else "0.00",
            "ILS Rank": str(random.randint(1, 8)),
            "Property_Key": str(pk),
        })

    with open(fname, "w", newline="") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"  Written: {fname} ({len(rows)} rows)")
    return fname


def build_apartment_list_export(run_dt, props, spend, funnel, out_dir):
    """
    Apartment List partner portal performance export.
    VK3. Apartment List uses a pay-per-lease model — cost is per signed lease,
    not per lead. This creates a different cost structure than Zillow/Apts.com.
    """
    fname = os.path.join(out_dir, f"apartment_list_{run_dt.strftime('%Y%m%d')}.csv")
    rows = []

    for pk, prop in sorted(props.items()):
        al_spend   = noise(spend[pk].get(VK_APTLIST, 0))
        al_leads   = int(noise(funnel[pk][VK_APTLIST].get(SK_LEADS, 0)))
        al_visits  = int(noise(funnel[pk][VK_APTLIST].get(SK_VISITS, 0)))
        al_leases  = int(noise(funnel[pk][VK_APTLIST].get(SK_LEASES, 0)))
        al_impr    = int(noise(funnel[pk][VK_APTLIST].get(SK_IMPRESSIONS, 0)))

        rows.append({
            "date": run_dt.strftime("%Y-%m-%d"),
            "property_name": prop["PropertyName"],
            "property_id": f"AL{pk:06d}",
            "street_address": prop["StreetAddress"],
            "city": prop["City"],
            "state": prop["State"],
            "zip": prop["Zip"],
            "market": prop["MarketName"],
            "listing_status": "active",
            "pricing_model": "pay_per_lease",
            "profile_views": al_impr,
            "renter_interest": al_leads,
            "tour_requests": int(al_leads * random.uniform(0.15, 0.30)),
            "applications_started": int(al_leads * random.uniform(0.08, 0.18)),
            "lease_signings_attributed": al_leases,
            "estimated_spend_usd": f"{al_spend:.4f}",
            "cost_per_lease": f"{round(al_spend / al_leases, 2):.2f}" if al_leases > 0 else "0.00",
            "renter_match_score_avg": f"{random.uniform(72, 94):.1f}",
            "response_rate_pct": f"{random.uniform(0.55, 0.92):.2f}",
            "avg_response_hours": f"{random.uniform(0.5, 8.0):.1f}",
            "property_key": str(pk),
        })

    with open(fname, "w", newline="") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"  Written: {fname} ({len(rows)} rows)")
    return fname


def build_display_dsp_export(run_dt, props, spend, funnel, out_dir):
    """
    Programmatic DSP consolidated report.
    Covers VK9 (StackAdapt) and VK10 (TradeDesk).
    Both DSPs use similar OpenRTB-based reporting schemas.
    StackAdapt and TradeDesk are in separate sections of the same file.
    """
    fname = os.path.join(out_dir, f"display_dsp_{run_dt.strftime('%Y%m%d')}.csv")
    rows = []

    for pk, prop in sorted(props.items()):
        prop_name = prop["PropertyName"]

        for vk, dsp_name, dsp_id_prefix in [
            (VK_STACKADAPT, "StackAdapt", "SA"),
            (VK_TRADEDESK,  "TradeDesk",  "TD"),
        ]:
            dsp_spend  = noise(spend[pk].get(vk, 0))
            dsp_impr   = int(noise(funnel[pk][vk].get(SK_IMPRESSIONS, 0)))
            dsp_clicks = int(noise(funnel[pk][vk].get(SK_CLICKS, 0)))
            viewable   = int(dsp_impr * random.uniform(0.55, 0.75))
            cpm        = round(dsp_spend / dsp_impr * 1000, 4) if dsp_impr > 0 else 0
            ctr        = round(dsp_clicks / dsp_impr, 6) if dsp_impr > 0 else 0

            rows.append({
                "DSP": dsp_name,
                "Date": run_dt.strftime("%Y-%m-%d"),
                "Advertiser": "NorthStar Residential Group",
                "Campaign": f"NSR | {prop['State']} | Prospecting | Q2-2026",
                "Campaign ID": f"{dsp_id_prefix}{pk:05d}001",
                "Line Item": f"{prop_name[:25]} | Display | Prospecting",
                "Line Item ID": f"{dsp_id_prefix}{pk:05d}LI1",
                "Creative Size": random.choice(["300x250", "728x90", "160x600", "320x50"]),
                "Audience Segment": random.choice([
                    "In-Market Renters", "Apartment Searchers", "Lookalike - Past Leases",
                    "Geo: 5mi Radius", "Household Income $50K+"
                ]),
                "Impressions": dsp_impr,
                "Viewable Impressions": viewable,
                "Viewability Rate": f"{round(viewable/dsp_impr,4):.4f}" if dsp_impr > 0 else "0.0000",
                "Clicks": dsp_clicks,
                "CTR": f"{ctr:.6f}",
                "Spend (USD)": f"{dsp_spend:.4f}",
                "CPM": f"{cpm:.4f}",
                "CPC": f"{round(dsp_spend/dsp_clicks,4):.4f}" if dsp_clicks > 0 else "0.0000",
                "Post-Click Conversions": 0,
                "Post-View Conversions": int(dsp_impr * random.uniform(0.0001, 0.0005)),
                "Frequency": f"{random.uniform(1.2, 3.8):.2f}",
                "Unique Reach": int(dsp_impr / random.uniform(1.2, 3.8)),
                "Win Rate": f"{random.uniform(0.08, 0.22):.4f}",
                "Bid Requests": int(dsp_impr / random.uniform(0.08, 0.22)),
                "Property_Key": str(pk),
            })

    with open(fname, "w", newline="") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"  Written: {fname} ({len(rows)} rows)")
    return fname


# ─────────────────────────────────────────────────────────────────────────────
# OPS PIPELINE SOURCE
# ─────────────────────────────────────────────────────────────────────────────


def build_yardi_ops_export(run_dt, props, ops, out_dir):
    """
    Yardi Voyager flat file extract — centralized instance, all 120 properties.
    Yardi exports via scheduled Crystal Reports or custom SQL views.
    This is the standard 'Occupancy and Leasing Summary' report format.
    """
    fname = os.path.join(out_dir, f"yardi_ops_export_{run_dt.strftime('%Y%m%d')}.csv")
    rows = []

    for pk, prop in sorted(props.items()):
        o = ops.get(pk, {})
        if not o:
            continue

        occ    = int(noise(int(o.get("OccupiedUnits", 0)), pct=0.02))
        vac    = int(noise(int(o.get("VacantUnits", 0)), pct=0.05))
        avail  = int(noise(int(o.get("AvailableUnits", 0)), pct=0.05))
        movein = int(noise(int(o.get("MoveIns", 0)), pct=0.10))
        moveout= int(noise(int(o.get("MoveOuts", 0)), pct=0.10))
        exp60  = float(o.get("LeaseExpirations_Next60D", 0))
        sched60= float(o.get("ScheduledMoveIns_Next60D", 0))
        total  = occ + vac
        occ_pct= round(occ / total, 4) if total > 0 else 0

        # Yardi property codes are typically short alphanumeric identifiers
        prop_code = prop["PropertyName"][:4].upper().replace(" ", "") + str(pk).zfill(3)

        rows.append({
            "PropertyCode": prop_code,
            "PropertyName": prop["PropertyName"],
            "PropertyID": str(pk),
            "MarketName": prop["MarketName"],
            "RegionName": {1:"East",2:"Central",3:"West",4:"South"}.get(int(prop["RegionKey"]),""),
            "State": prop["State"],
            "City": prop["City"],
            "AsOfDate": yardi_date(run_dt),
            "ExtractTimestamp": run_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "UnitCount": int(prop["TotalUnits"]),
            "OccupiedUnits": occ,
            "VacantUnits": vac,
            "AvailableUnits": avail,
            "OccupancyPct": f"{occ_pct:.4f}",
            "VacancyPct": f"{round(vac/total,4):.4f}" if total > 0 else "0.0000",
            "MoveIns_Today": movein,
            "MoveOuts_Today": moveout,
            "NetAbsorption_Today": movein - moveout,
            "LeaseExpirations_Today": int(o.get("LeaseExpirations", 0)),
            "ScheduledMoveIns_Today": int(o.get("ScheduledMoveIns", 0)),
            "LeaseExpirations_Next60D": f"{exp60:.1f}",
            "ScheduledMoveIns_Next60D": f"{sched60:.1f}",
            "CoverageRatio_60D": f"{round(sched60/exp60,4):.4f}" if exp60 > 0 else "0.0000",
            "NoticeUnits": int(vac * random.uniform(0.15, 0.35)),
            "DownUnits": int(vac * random.uniform(0.05, 0.20)),
            "ModelUnits": random.randint(0, 2),
            "EmployeeUnits": random.randint(0, 1),
            "Concessions_Active": random.choice(["Y", "Y", "Y", "N"]),
            "AvgRent_Occupied": f"{random.uniform(1350, 2800):.2f}",
            "AvgRent_Asking": f"{random.uniform(1400, 2900):.2f}",
            "IsActive": "1",
        })

    with open(fname, "w", newline="") as f:
        # Yardi exports typically have a report header block
        f.write(f"Yardi Voyager — Occupancy and Leasing Summary\n")
        f.write(f"Client: NorthStar Residential Group\n")
        f.write(f"Database: NORTHSTAR_PROD\n")
        f.write(f"Report Run: {run_dt.strftime('%m/%d/%Y %I:%M:%S %p')}\n")
        f.write(f"As Of Date: {yardi_date(run_dt)}\n\n")
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"  Written: {fname} ({len(rows)} rows, {len(props)} properties)")
    return fname


# ─────────────────────────────────────────────────────────────────────────────
# CRM PIPELINE SOURCE  (Salesforce Bulk API format)
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="NorthStar MAA — Mock Pipeline Source File Generator"
    )
    parser.add_argument(
        "--date", required=True,
        help="Run date in YYYY-MM-DD format (e.g. 2026-04-11)"
    )
    parser.add_argument(
        "--output", default="./mock_sources",
        help="Output directory for generated source files"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    args = parser.parse_args()

    random.seed(args.seed)

    run_dt = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    os.makedirs(args.output, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  NorthStar Mock Source Generator")
    print(f"  Run date : {args.date}")
    print(f"  Output   : {args.output}")
    print(f"  Seed     : {args.seed}")
    print(f"{'='*60}\n")

    # Load calibration baseline from gold data
    props, spend, funnel, ops, leasing, monthly_leases, monthly_leads = load_gold_baseline(args.date)

    print("\n-- SPEND PIPELINE SOURCES --")
    build_google_ads_export(run_dt, props, spend, funnel, args.output)
    build_bing_ads_export(run_dt, props, spend, funnel, args.output)
    build_meta_ads_export(run_dt, props, spend, funnel, args.output)
    build_zillow_export(run_dt, props, spend, funnel, leasing, args.output)
    build_apartments_com_export(run_dt, props, spend, funnel, args.output)
    build_apartment_list_export(run_dt, props, spend, funnel, args.output)
    build_display_dsp_export(run_dt, props, spend, funnel, args.output)

    print("\n-- OPS PIPELINE SOURCE --")
    build_yardi_ops_export(run_dt, props, ops, args.output)

    print("\n-- CRM PIPELINE SOURCES --")
    registry = build_prospect_registry(run_dt, props, monthly_leases, monthly_leads)
    write_crm_files(run_dt, registry, args.output)

    # Count total files written
    files = os.listdir(args.output)
    run_files = [f for f in files if run_dt.strftime('%Y%m%d') in f]

    print(f"\n{'='*60}")
    print(f"  COMPLETE — {len(run_files)} files written to {args.output}/")
    print(f"{'='*60}")
    for f in sorted(run_files):
        size = os.path.getsize(os.path.join(args.output, f))
        print(f"  {f:<45s}  {size:>10,} bytes")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()