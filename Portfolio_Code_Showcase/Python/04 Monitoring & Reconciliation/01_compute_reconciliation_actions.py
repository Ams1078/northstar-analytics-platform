"""
compute_reconciliation_actions.py
=================================
Reads bronze CSV files from Azure Blob Storage, compares against canonical
truth in Azure SQL, and writes one row per (date, source, action_type) to
pipeline.reconciliation_actions.

This is NOT a pipeline. It does not run on a timer. It does not reconstruct
silver. It is an analytical script you invoke manually or on demand.

USAGE:
    # Single date
    python compute_reconciliation_actions.py --date 2026-04-22

    # Date range (loops single-date logic for each day)
    python compute_reconciliation_actions.py --start 2026-04-01 --end 2026-05-03

    # Dry-run (compute but don't write)
    python compute_reconciliation_actions.py --date 2026-04-22 --dry-run

ENVIRONMENT:
    Reads SQL connection from same env vars as pipeline_utils.py
    (SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, optional SQL_DRIVER).
    Auth to blob uses DefaultAzureCredential() — managed identity in Azure,
    az-cli login locally.

DESIGN NOTES:
    - Idempotent per date: re-running for the same date deletes ALL of that
      date's rows first, then re-inserts the fresh action set. This handles
      state changes correctly — when a source flips from missing to present
      between runs, stale rows from the prior state get cleaned up.
    - Severity is hardcoded by ActionType (no thresholds, no scoring).
    - MISSING_BRONZE is logged when a source folder doesn't exist for a date.
    - Spend bronze comparison vs canonical is the primary cross-source check.
      CRM and Ops bronze checks are intra-file (dedup, dirty data, orphans).
"""

import argparse
import csv
import datetime
import io
import logging
import os
import re
import sys
from collections import defaultdict, Counter
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv
from sqlalchemy import text

# Reuse existing pipeline infrastructure — engine builder, retry, lookups
from pipeline_utils import (
    get_engine,
    connect_with_retry,
    load_property_lookup,
    load_vendor_lookup,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("reconcile")

# ─── Config ──────────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = os.environ.get("BRONZE_STORAGE_ACCOUNT", "maatruthlake")
BRONZE_CONTAINER = os.environ.get("BRONZE_CONTAINER", "bronze")

# PipelineKey values match pipeline_utils.py constants
PIPELINE_KEY_CRM = 2
PIPELINE_KEY_SPEND = 3
PIPELINE_KEY_OPS = 4

# ─── Source registry ─────────────────────────────────────────────────────────
# Maps SourceSystem string → (bronze subfolder, expected filename pattern,
# pipeline_key, canonical_vendor_keys).
#
# canonical_vendor_keys is a tuple of VendorKey values that the bronze file
# represents in canonical truth. Most files are single-vendor, but some bronze
# formats (Google Ads especially) aggregate multiple campaign types under one
# export — Search campaigns map to VK4 in canonical, while Remarketing/Display
# campaigns in the same file map to VK8. Comparing bronze sum to canonical VK4
# alone produced a structural ~50% gap that was a model/grain mismatch, NOT
# a vendor data quality issue. Spend is financially reliable; reconciliation
# must compare at comparable grain.
#
# When the tuple is None, the file is multi-vendor in a way that can't be
# resolved without per-row platform disambiguation (Meta = FB+IG; Display DSP
# = StackAdapt+TradeDesk). Those produce SPEND_DIVERGENCE_UNRESOLVED.

SPEND_SOURCES = {
    "spend_google_ads":     ("spend", "google_ads_export_{yyyymmdd}.csv",     PIPELINE_KEY_SPEND, (4, 8)),  # VK4 Search + VK8 Display
    "spend_bing_ads":       ("spend", "bing_ads_export_{yyyymmdd}.csv",       PIPELINE_KEY_SPEND, (5,)),
    "spend_meta_ads":       ("spend", "meta_ads_export_{yyyymmdd}.csv",       PIPELINE_KEY_SPEND, None),    # VK6 + VK7, unresolved
    "spend_zillow":         ("spend", "zillow_leads_{yyyymmdd}.csv",          PIPELINE_KEY_SPEND, (1,)),
    "spend_apartments_com": ("spend", "apartments_com_{yyyymmdd}.csv",        PIPELINE_KEY_SPEND, (2,)),
    "spend_apartment_list": ("spend", "apartment_list_{yyyymmdd}.csv",        PIPELINE_KEY_SPEND, (3,)),
    "spend_display_dsp":    ("spend", "display_dsp_{yyyymmdd}.csv",           PIPELINE_KEY_SPEND, None),    # VK9 + VK10, unresolved
}

CRM_SOURCES = {
    "crm_sf_leads":             ("crm", "sf_leads_raw_{yyyymmdd}.csv",             PIPELINE_KEY_CRM),
    "crm_sf_contacts":          ("crm", "sf_contacts_raw_{yyyymmdd}.csv",          PIPELINE_KEY_CRM),
    "crm_sf_opportunities":     ("crm", "sf_opportunities_raw_{yyyymmdd}.csv",     PIPELINE_KEY_CRM),
    "crm_sf_tasks":             ("crm", "sf_tasks_raw_{yyyymmdd}.csv",             PIPELINE_KEY_CRM),
    "crm_sf_campaigns":         ("crm", "sf_campaigns_raw_{yyyymmdd}.csv",         PIPELINE_KEY_CRM),
    "crm_sf_campaign_members":  ("crm", "sf_campaign_members_raw_{yyyymmdd}.csv",  PIPELINE_KEY_CRM),
}

OPS_SOURCES = {
    "ops_yardi": ("ops", "yardi_ops_export_{yyyymmdd}.csv", PIPELINE_KEY_OPS),
}

ALL_SOURCES = {**SPEND_SOURCES, **CRM_SOURCES, **OPS_SOURCES}

# ─── Severity map (hardcoded, no scoring) ────────────────────────────────────
SEVERITY = {
    "SPEND_DIVERGENCE":             "med",
    "SPEND_DIVERGENCE_UNRESOLVED":  "med",   # multi-vendor files we can't anchor to one VendorKey
    "DEDUP_EMAIL":                  "med",
    "DEDUP_PHONE":                  "med",
    "OCCUPANCY_VARIANCE":           "med",
    "DIRTY_PHONE":                  "low",
    "DIRTY_EMAIL":                  "low",
    "DIRTY_NAME":                   "low",
    "MISSING_PROPERTY":             "high",
    "MISSING_VENDOR":               "high",
    "MISSING_EMAIL_AND_PHONE":      "high",
    "ORPHAN_TASK":                  "high",
    "ORPHAN_OPP":                   "high",
    "MISSING_BRONZE":               "high",
    "MISSING_FOLDER":               "high",  # entire source folder missing for the date
}

# Severity escalation thresholds for SPEND_DIVERGENCE.
# Default severity for SPEND_DIVERGENCE is 'med'. Three escalation paths to 'high':
#
#   1. HARD DOLLAR OVERRIDE — ABS(delta) > $5,000 alone, regardless of percent.
#      Catches the "big budget blind spot": $10K of unexplained spend on a
#      $200K canonical = 5% (under the AND rule below it would stay med, but
#      $10K of operational variance is high-severity in any context).
#
#   2. NO CANONICAL ANCHOR — bronze reports spend but canonical sum is $0.
#      delta_pct is None in this case (can't divide by zero). This is NOT
#      "small percent" — it's "we have no canonical to compare against at
#      all." Different from a percent that's small. Treated as high when
#      bronze dollars are material.
#
#   3. STANDARD AND RULE — both ABS(delta) > $50 AND ABS(delta_pct) > 5%.
#      Either dimension alone isn't enough; both must be material.
#      Catches structural divergences like the multi-VK conflation pattern
#      while letting per-property vendor noise stay at 'med'.
#
# All other action types use the default SEVERITY map unchanged.
SPEND_HIGH_SEVERITY_DOLLAR_DELTA = 50.00     # base threshold for AND rule
SPEND_HIGH_SEVERITY_PCT_DELTA    = 0.05      # 5% threshold for AND rule
SPEND_HARD_DOLLAR_OVERRIDE       = 5000.00   # absolute override regardless of pct


def _escalate_spend_severity(action_type: str,
                             delta_amount: Optional[float],
                             delta_pct: Optional[float]) -> str:
    """
    Determine severity for a SPEND_DIVERGENCE row, applying three escalation
    paths to 'high'. See SPEND_HIGH_SEVERITY_* constants for rationale.
    """
    if action_type != "SPEND_DIVERGENCE":
        return SEVERITY[action_type]
    if delta_amount is None:
        return SEVERITY[action_type]

    # Path 1 — hard dollar override (large absolute delta regardless of pct)
    if abs(delta_amount) > SPEND_HARD_DOLLAR_OVERRIDE:
        return "high"

    # Path 2 — no canonical anchor (delta_pct is None because canonical = $0)
    # Treat absolute dollar delta alone as the escalation criterion.
    if delta_pct is None:
        return ("high"
                if abs(delta_amount) > SPEND_HIGH_SEVERITY_DOLLAR_DELTA
                else "med")

    # Path 3 — standard AND rule (both dimensions must be material)
    if (abs(delta_amount) > SPEND_HIGH_SEVERITY_DOLLAR_DELTA
            and abs(delta_pct) > SPEND_HIGH_SEVERITY_PCT_DELTA):
        return "high"
    return "med"


# ─── Dirty-data detectors (regex-light, fast) ────────────────────────────────
TEST_EMAIL_PATTERNS = [
    re.compile(r"^test@", re.I),
    re.compile(r"@example\.com$", re.I),
    re.compile(r"^noreply@", re.I),
    re.compile(r"^fake@", re.I),
    re.compile(r"@mailinator\.com$", re.I),
]

FAKE_PHONE_PATTERNS = {"5550000000", "0000000000", "1234567890"}

TEST_NAMES = {"john doe", "test user", "n/a", "na", "x", "."}


def normalize_phone(raw: Optional[str]) -> Optional[str]:
    """Strip everything but digits. Drop leading 1 if 11-char US format."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if digits else None


def is_dirty_phone(raw: Optional[str]) -> bool:
    norm = normalize_phone(raw)
    if not norm:
        return True  # null phone gets caught by MISSING_EMAIL_AND_PHONE if also no email
    if len(norm) != 10:
        return True
    if norm in FAKE_PHONE_PATTERNS:
        return True
    return False


def is_dirty_email(raw: Optional[str]) -> bool:
    if not raw:
        return False  # missing handled separately
    s = raw.strip().lower()
    if "@" not in s:
        return True
    return any(p.search(s) for p in TEST_EMAIL_PATTERNS)


def is_dirty_name(first: Optional[str], last: Optional[str]) -> bool:
    name = f"{(first or '').strip()} {(last or '').strip()}".strip().lower()
    if not name:
        return True
    if len(name) <= 1:
        return True
    if len(set(name.replace(" ", ""))) == 1:  # all same char
        return True
    if name in TEST_NAMES:
        return True
    return False


# ─── Blob helpers ────────────────────────────────────────────────────────────
def get_container_client() -> ContainerClient:
    """Build a ContainerClient for the bronze container using managed identity."""
    account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
    cred = DefaultAzureCredential()
    svc = BlobServiceClient(account_url=account_url, credential=cred)
    return svc.get_container_client(BRONZE_CONTAINER)


def list_bronze_files(container: ContainerClient, source_subfolder: str,
                      run_date: datetime.date) -> list:
    """List all blob names under bronze/{source}/{YYYY-MM-DD}/."""
    prefix = f"{source_subfolder}/{run_date.isoformat()}/"
    return [b.name for b in container.list_blobs(name_starts_with=prefix)
            if not b.name.endswith("/") and not b.name.split("/")[-1].startswith("_")]


def folder_exists(container: ContainerClient, source_subfolder: str,
                  run_date: datetime.date) -> bool:
    """
    Check if bronze/{source}/{date}/ contains any blobs at all.
    Used to short-circuit per-file checks when the entire folder is missing —
    one MISSING_FOLDER action is more useful than 6 MISSING_BRONZE actions.
    """
    prefix = f"{source_subfolder}/{run_date.isoformat()}/"
    # list_blobs with results_per_page=1 lets us bail after one hit
    iterator = container.list_blobs(name_starts_with=prefix, results_per_page=1)
    try:
        next(iter(iterator))
        return True
    except StopIteration:
        return False


def read_bronze_csv(container: ContainerClient, blob_name: str,
                    skip_preamble: bool = True) -> list:
    """
    Download blob and parse as CSV. Returns list of dict rows.
    skip_preamble handles vendor reports with header lines before the CSV
    (Google Ads, Bing Ads, Yardi, Zillow all have these).
    """
    blob_client = container.get_blob_client(blob_name)
    raw = blob_client.download_blob().readall().decode("utf-8", errors="replace")

    if skip_preamble:
        # Find the first line that looks like a CSV header (contains a comma
        # and doesn't look like "Key: Value" metadata)
        lines = raw.splitlines()
        start_idx = 0
        for i, line in enumerate(lines):
            if "," in line and not re.match(r"^[A-Za-z ]+:", line.strip()):
                # Heuristic: header lines have multiple commas
                if line.count(",") >= 2:
                    start_idx = i
                    break
        raw = "\n".join(lines[start_idx:])

    reader = csv.DictReader(io.StringIO(raw))
    return list(reader)


# ─── Action computation per source family ────────────────────────────────────
def compute_spend_actions(engine, container: ContainerClient,
                          source_system: str, run_date: datetime.date,
                          subfolder: str, filename_template: str,
                          vendor_keys: Optional[tuple],
                          property_lookup: dict,
                          vendor_lookup: dict) -> list:
    """
    For a Spend source, compare bronze totals vs canonical (DS=1) at
    (DateKey, PropertyKey, VendorKey-set) grain. vendor_keys is a tuple of
    one or more VendorKey values that the bronze file represents in canonical
    truth. Multi-VK tuples (e.g. Google Ads = (4, 8)) sum across both VKs to
    match bronze grain. None means unresolvable (Meta, Display DSP).
    """
    actions = []
    yyyymmdd = run_date.strftime("%Y%m%d")
    expected_filename = filename_template.format(yyyymmdd=yyyymmdd)
    blob_path = f"{subfolder}/{run_date.isoformat()}/{expected_filename}"

    # Try to read the file — if not found, log MISSING_BRONZE for this specific source
    try:
        rows = read_bronze_csv(container, blob_path, skip_preamble=True)
    except Exception as e:
        log.warning("[%s] Bronze file not found at %s: %s", source_system, blob_path, e)
        actions.append(_action(
            source_system, run_date, "MISSING_BRONZE", 1,
            description=f"Expected file {expected_filename} not present in bronze",
            bronze_path=blob_path,
        ))
        return actions

    bronze_total = len(rows)
    if bronze_total == 0:
        actions.append(_action(
            source_system, run_date, "MISSING_BRONZE", 1,
            description=f"File {expected_filename} present but empty",
            bronze_path=blob_path, bronze_rows_total=0,
        ))
        return actions

    # MISSING_PROPERTY: bronze rows whose property_id doesn't resolve
    # The property column varies by source — try common ones
    prop_columns = ["Property ID", "Property_ID", "Property_Key", "property_id",
                    "property_key", "PropertyId", "Community ID", "PropertyID"]
    prop_col = next((c for c in prop_columns if rows and c in rows[0]), None)

    # For sample-id formatting, use the first VK in the tuple (or 0 for unresolved)
    primary_vk = vendor_keys[0] if vendor_keys else 0

    missing_prop_count = 0
    sample_missing_prop = None
    if prop_col:
        for r in rows:
            raw = (r.get(prop_col) or "").strip()
            if not raw:
                continue
            try:
                pk = int(re.sub(r"\D", "", raw))
                if pk not in property_lookup:
                    missing_prop_count += 1
                    if sample_missing_prop is None:
                        sample_missing_prop = f"{int(run_date.strftime('%Y%m%d'))}-P{raw}-V{primary_vk}"
            except (ValueError, TypeError):
                missing_prop_count += 1
                if sample_missing_prop is None:
                    sample_missing_prop = f"{int(run_date.strftime('%Y%m%d'))}-P{raw}-V{primary_vk}"

    if missing_prop_count > 0:
        actions.append(_action(
            source_system, run_date, "MISSING_PROPERTY", missing_prop_count,
            description=f"Bronze property_id not in dim_property",
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            sample_record_id=sample_missing_prop,
        ))

    # MISSING_VENDOR: only relevant for files where vendor isn't fixed by source
    # (single-vendor files always resolve, so this is mostly a placeholder for
    # multi-vendor files like display_dsp and meta_ads). For now: if vendor_keys
    # is None in the registry AND we can't infer it from row data, log it.
    if vendor_keys is None:
        # display_dsp has a "DSP" column ("StackAdapt"|"TradeDesk")
        # meta_ads has a "Platform" column ("Facebook"|"Instagram")
        vendor_col = None
        for candidate in ["DSP", "Platform"]:
            if rows and candidate in rows[0]:
                vendor_col = candidate
                break
        if vendor_col:
            unresolved = sum(1 for r in rows if not (r.get(vendor_col) or "").strip())
            if unresolved > 0:
                actions.append(_action(
                    source_system, run_date, "MISSING_VENDOR", unresolved,
                    description=f"Rows missing {vendor_col} for vendor disambiguation",
                    bronze_path=blob_path, bronze_rows_total=bronze_total,
                    sample_record_id=f"{int(run_date.strftime('%Y%m%d'))}-noVendorCol",
                ))

    # SPEND_DIVERGENCE / SPEND_DIVERGENCE_UNRESOLVED:
    # _compute_spend_divergence returns a list of action dicts with keys
    # {action_type, count, description, sample, delta_amount, delta_pct}.
    # May be empty if perfectly aligned within materiality threshold.
    div_results = _compute_spend_divergence(
        engine, run_date, source_system, vendor_keys, rows, prop_col,
        property_lookup
    )
    for d in div_results:
        actions.append(_action(
            source_system, run_date, d["action_type"], d["count"],
            description=d["description"],
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            sample_record_id=d["sample"],
            delta_amount=d.get("delta_amount"),
            delta_pct=d.get("delta_pct"),
        ))

    return actions


def _compute_spend_divergence(engine, run_date, source_system, vendor_keys,
                              rows, prop_col, property_lookup):
    """
    Compare bronze spend to canonical at comparable grain.

    Spend is treated as financially reliable — vendors do not under/over-report
    spend by 50%. When bronze and canonical disagree at scale, the cause is
    almost always a model/grain mismatch, not a data quality issue. Two patterns:

      1. Multi-vendor bronze file → vendor_keys is a tuple of >1 VKs.
         Example: spend_google_ads bronze includes Search (VK4) + Display (VK8)
         campaigns under one Google Ads export, mirroring the real Google Ads UI.
         Canonical stores VK4 and VK8 separately. Compare bronze sum against
         canonical SUM(VK4, VK8).

      2. Unresolvable multi-vendor → vendor_keys is None. Meta = FB+IG, Display
         DSP = StackAdapt+TradeDesk. Without per-row platform disambiguation,
         can't anchor to canonical. Emit SPEND_DIVERGENCE_UNRESOLVED.

    Materiality threshold:
        Flag a property as divergent only when:
            ABS(delta) > $1.00  OR  ABS(delta_pct) > 1%
        Rationale: ±$0.01 floating-point fuzz isn't material; ±8% noise on
        $50 spend = ±$4 IS material reconciliation work in production.

    Returns a list of action dicts. SPEND_DIVERGENCE rows include:
        action_type, count, description, sample,
        delta_amount, delta_pct
    """
    date_key = int(run_date.strftime("%Y%m%d"))

    # Reasons we can't compute divergence — emit UNRESOLVED so the dashboard
    # doesn't read silence as "all clean"
    if not vendor_keys:
        return [{
            "action_type": "SPEND_DIVERGENCE_UNRESOLVED",
            "count": len(rows),
            "description": "Multi-vendor file — bronze cannot be anchored to a single VendorKey for canonical comparison",
            "sample": f"{date_key}-multivendor",
            "delta_amount": None,
            "delta_pct": None,
        }]
    if not prop_col:
        return [{
            "action_type": "SPEND_DIVERGENCE_UNRESOLVED",
            "count": len(rows),
            "description": "No property identifier column found in bronze file",
            "sample": f"{date_key}-noprop",
            "delta_amount": None,
            "delta_pct": None,
        }]

    # Find spend column — varies by vendor
    spend_columns = ["Cost", "Spend", "Spend (USD)", "Amount spent (USD)",
                     "Monthly Cost", "Monthly Spend", "estimated_spend_usd"]
    spend_col = next((c for c in spend_columns if rows and c in rows[0]), None)
    if not spend_col:
        return [{
            "action_type": "SPEND_DIVERGENCE_UNRESOLVED",
            "count": len(rows),
            "description": "No spend column found in bronze file",
            "sample": f"{date_key}-{'_'.join(f'V{v}' for v in vendor_keys)}-nospend",
            "delta_amount": None,
            "delta_pct": None,
        }]

    # Aggregate bronze spend by PropertyKey
    bronze_by_prop = defaultdict(float)
    for r in rows:
        raw_pk = (r.get(prop_col) or "").strip()
        try:
            pk = int(re.sub(r"\D", "", raw_pk))
        except (ValueError, TypeError):
            continue
        if pk not in property_lookup:
            continue

        raw_spend = (r.get(spend_col) or "0").strip()
        raw_spend = raw_spend.replace("$", "").replace(",", "")
        try:
            bronze_by_prop[pk] += float(raw_spend)
        except (ValueError, TypeError):
            continue

    if not bronze_by_prop:
        return []  # all rows had unresolvable properties — handled by MISSING_PROPERTY

    # Query canonical for this date across ALL VendorKeys this bronze file represents.
    # For Google Ads (VK4, VK8), this sums Search + Display per property to match
    # the bronze file's grain.
    vk_placeholders = ",".join(f":vk{i}" for i in range(len(vendor_keys)))
    sql = f"""
        SELECT PropertyKey, SUM(Spend) AS canonical_spend
        FROM   dbo.fact_marketing_spend_daily
        WHERE  DataSource = 1
          AND  DateKey    = :dk
          AND  VendorKey  IN ({vk_placeholders})
        GROUP BY PropertyKey
    """
    params = {"dk": date_key}
    for i, vk in enumerate(vendor_keys):
        params[f"vk{i}"] = vk

    with connect_with_retry(engine) as conn:
        result = conn.execute(text(sql), params).fetchall()

    canonical_by_prop = {row[0]: float(row[1]) for row in result}

    # Materiality thresholds — divergence must be financially meaningful
    MIN_DOLLAR_DELTA = 1.00   # absolute dollar threshold
    MIN_PCT_DELTA    = 0.01   # 1% relative threshold

    divergent = 0
    sample = None
    sum_bronze_divergent    = 0.0
    sum_canonical_divergent = 0.0
    vk_label = "+".join(f"V{v}" for v in vendor_keys)

    for pk, bronze_spend in bronze_by_prop.items():
        canon_spend = canonical_by_prop.get(pk, 0.0)
        delta = bronze_spend - canon_spend
        # Compute delta_pct guarding against canon=0 (treat as material if bronze>0)
        if canon_spend > 0:
            delta_pct = abs(delta) / canon_spend
        else:
            delta_pct = float("inf") if bronze_spend > 0 else 0.0

        is_material = (abs(delta) > MIN_DOLLAR_DELTA) or (delta_pct > MIN_PCT_DELTA)
        if is_material:
            divergent += 1
            sum_bronze_divergent    += bronze_spend
            sum_canonical_divergent += canon_spend
            if sample is None:
                sample = f"{date_key}-P{pk}-{vk_label}"

    if divergent == 0:
        return []

    # Aggregate Delta for executive readability.
    # When canonical sum is zero (no canonical rows for this date/VK at all),
    # delta_pct is mathematically undefined — set to None and emit a distinct
    # description so the dashboard reader sees "no canonical data" rather
    # than "spend differs at comparable grain." These are operationally
    # different situations.
    aggregate_delta = sum_bronze_divergent - sum_canonical_divergent
    if sum_canonical_divergent > 0:
        aggregate_delta_pct = aggregate_delta / sum_canonical_divergent
        description = (
            f"Spend does not reconcile at comparable grain "
            f"(bronze {vk_label} vs canonical {vk_label}, "
            f"materiality > $1 or > 1%)"
        )
    else:
        aggregate_delta_pct = None
        description = (
            f"Spend has no canonical anchor — canonical sum for {vk_label} = $0 "
            f"on this date. Bronze reports ${aggregate_delta:,.2f} with nothing "
            f"to compare against. Likely missing canonical data for this date."
        )

    return [{
        "action_type": "SPEND_DIVERGENCE",
        "count": divergent,
        "description": description,
        "sample": sample,
        "delta_amount": round(aggregate_delta, 4),
        "delta_pct": round(aggregate_delta_pct, 6) if aggregate_delta_pct is not None else None,
    }]


def compute_crm_actions(container: ContainerClient,
                        source_system: str, run_date: datetime.date,
                        subfolder: str, filename_template: str,
                        property_lookup: dict,
                        crm_context: dict) -> list:
    """
    CRM checks are intra-file (no canonical comparison — there's no DS=2
    yet to compare against). Detects dedup, dirty-data, orphan-id issues.
    crm_context is shared across CRM sources for the same date so orphan
    checks can join across files.
    """
    actions = []
    yyyymmdd = run_date.strftime("%Y%m%d")
    expected_filename = filename_template.format(yyyymmdd=yyyymmdd)
    blob_path = f"{subfolder}/{run_date.isoformat()}/{expected_filename}"

    try:
        rows = read_bronze_csv(container, blob_path, skip_preamble=False)
    except Exception as e:
        log.warning("[%s] Bronze file not found at %s: %s", source_system, blob_path, e)
        actions.append(_action(
            source_system, run_date, "MISSING_BRONZE", 1,
            description=f"Expected file {expected_filename} not present in bronze",
            bronze_path=blob_path,
        ))
        return actions

    bronze_total = len(rows)
    if bronze_total == 0:
        actions.append(_action(
            source_system, run_date, "MISSING_BRONZE", 1,
            description=f"File {expected_filename} present but empty",
            bronze_path=blob_path, bronze_rows_total=0,
        ))
        return actions

    # Dispatch by source — leads has the most checks, others are leaner
    if source_system == "crm_sf_leads":
        actions.extend(_check_sf_leads(rows, blob_path, source_system, run_date,
                                        property_lookup, crm_context))
    elif source_system == "crm_sf_contacts":
        # Track contact IDs for orphan checks; light dirty-data on contacts
        crm_context.setdefault("contact_ids", set())
        crm_context["contact_ids"].update(
            (r.get("Id") or "").strip() for r in rows if r.get("Id")
        )
    elif source_system == "crm_sf_tasks":
        actions.extend(_check_sf_tasks(rows, blob_path, source_system, run_date,
                                        crm_context))
    elif source_system == "crm_sf_opportunities":
        actions.extend(_check_sf_opportunities(rows, blob_path, source_system,
                                                run_date, crm_context))
    elif source_system == "crm_sf_campaign_members":
        actions.extend(_check_sf_campaign_members(rows, blob_path, source_system,
                                                   run_date, crm_context))
    # crm_sf_campaigns is reference data — no checks needed

    return actions


def _check_sf_leads(rows, blob_path, source_system, run_date,
                    property_lookup, crm_context):
    """All the meaty CRM checks live here."""
    actions = []
    bronze_total = len(rows)

    # Track lead IDs for downstream orphan checks
    lead_ids = set()
    for r in rows:
        lid = (r.get("Id") or "").strip()
        if lid:
            lead_ids.add(lid)
    crm_context["lead_ids"] = lead_ids

    # DEDUP_EMAIL
    email_to_ids = defaultdict(list)
    for r in rows:
        email = (r.get("Email") or "").strip().lower()
        lid = (r.get("Id") or "").strip()
        if email and lid:
            email_to_ids[email].append(lid)
    dup_emails = sum(len(ids) - 1 for ids in email_to_ids.values() if len(ids) > 1)
    if dup_emails > 0:
        # Sample = first Lead Id from any duplicate group
        sample_id = next((ids[0] for ids in email_to_ids.values() if len(ids) > 1), None)
        actions.append(_action(
            source_system, run_date, "DEDUP_EMAIL", dup_emails,
            description=f"Duplicate emails within sf_leads_raw",
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            bronze_rows_after_action=bronze_total - dup_emails,
            sample_record_id=sample_id,
        ))

    # DEDUP_PHONE (normalized)
    phone_to_ids = defaultdict(list)
    for r in rows:
        norm = normalize_phone(r.get("Phone"))
        lid = (r.get("Id") or "").strip()
        if norm and len(norm) == 10 and lid:
            phone_to_ids[norm].append(lid)
    dup_phones = sum(len(ids) - 1 for ids in phone_to_ids.values() if len(ids) > 1)
    if dup_phones > 0:
        sample_id = next((ids[0] for ids in phone_to_ids.values() if len(ids) > 1), None)
        actions.append(_action(
            source_system, run_date, "DEDUP_PHONE", dup_phones,
            description=f"Duplicate normalized phones within sf_leads_raw",
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            bronze_rows_after_action=bronze_total - dup_phones,
            sample_record_id=sample_id,
        ))

    # DIRTY_PHONE / DIRTY_EMAIL / DIRTY_NAME / MISSING_EMAIL_AND_PHONE
    dirty_phone = dirty_email = dirty_name = missing_both = 0
    samples = {}
    for r in rows:
        phone = r.get("Phone")
        email = r.get("Email")
        first = r.get("FirstName")
        last = r.get("LastName")
        lid = (r.get("Id") or "").strip()

        has_phone = bool(normalize_phone(phone))
        has_email = bool((email or "").strip())

        if not has_phone and not has_email:
            missing_both += 1
            samples.setdefault("MISSING_EMAIL_AND_PHONE", lid)
        if phone and is_dirty_phone(phone):
            dirty_phone += 1
            samples.setdefault("DIRTY_PHONE", lid)
        if email and is_dirty_email(email):
            dirty_email += 1
            samples.setdefault("DIRTY_EMAIL", lid)
        if is_dirty_name(first, last):
            dirty_name += 1
            samples.setdefault("DIRTY_NAME", lid)

    for action_type, count in [
        ("DIRTY_PHONE", dirty_phone),
        ("DIRTY_EMAIL", dirty_email),
        ("DIRTY_NAME", dirty_name),
        ("MISSING_EMAIL_AND_PHONE", missing_both),
    ]:
        if count > 0:
            actions.append(_action(
                source_system, run_date, action_type, count,
                description=f"{action_type} detected in sf_leads_raw",
                bronze_path=blob_path, bronze_rows_total=bronze_total,
                sample_record_id=samples.get(action_type),
            ))

    # MISSING_PROPERTY
    missing_prop = 0
    sample_mp = None
    for r in rows:
        prop_code = (r.get("Property__c") or "").strip()
        # Property codes look like PROP00001 — extract the digits
        m = re.search(r"\d+", prop_code)
        if not m:
            missing_prop += 1
            sample_mp = sample_mp or (r.get("Id") or "").strip()
            continue
        pk = int(m.group())
        if pk not in property_lookup:
            missing_prop += 1
            sample_mp = sample_mp or (r.get("Id") or "").strip()

    if missing_prop > 0:
        actions.append(_action(
            source_system, run_date, "MISSING_PROPERTY", missing_prop,
            description="Property__c does not resolve to dim_property",
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            sample_record_id=sample_mp,
        ))

    return actions


def _check_sf_tasks(rows, blob_path, source_system, run_date, crm_context):
    """ORPHAN_TASK: WhoId not in {lead_ids ∪ contact_ids} for the day."""
    actions = []
    bronze_total = len(rows)
    valid_ids = (crm_context.get("lead_ids", set())
                 | crm_context.get("contact_ids", set()))

    if not valid_ids:
        # Leads/contacts not loaded yet — skip orphan check, log informationally
        log.info("[%s] No lead_ids or contact_ids in context; skipping ORPHAN_TASK",
                 source_system)
        return actions

    orphans = 0
    sample = None
    for r in rows:
        whoid = (r.get("WhoId") or "").strip()
        if whoid and whoid not in valid_ids:
            orphans += 1
            if sample is None:
                sample = (r.get("Id") or "").strip()

    if orphans > 0:
        actions.append(_action(
            source_system, run_date, "ORPHAN_TASK", orphans,
            description="Task.WhoId not in leads or contacts for the day",
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            sample_record_id=sample,
        ))
    return actions


def _check_sf_opportunities(rows, blob_path, source_system, run_date, crm_context):
    """ORPHAN_OPP: LeadId__c not in lead_ids for the day."""
    actions = []
    bronze_total = len(rows)
    lead_ids = crm_context.get("lead_ids", set())

    if not lead_ids:
        log.info("[%s] No lead_ids in context; skipping ORPHAN_OPP", source_system)
        return actions

    orphans = 0
    sample = None
    for r in rows:
        leadid = (r.get("LeadId__c") or "").strip()
        if leadid and leadid not in lead_ids:
            orphans += 1
            if sample is None:
                sample = (r.get("Id") or "").strip()

    if orphans > 0:
        actions.append(_action(
            source_system, run_date, "ORPHAN_OPP", orphans,
            description="Opportunity.LeadId__c not in sf_leads_raw for the day",
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            sample_record_id=sample,
        ))
    return actions


def _check_sf_campaign_members(rows, blob_path, source_system, run_date, crm_context):
    """LeadId in campaign_members should match sf_leads — track but don't error."""
    # Reuse ORPHAN_OPP-style logic on LeadId column
    actions = []
    bronze_total = len(rows)
    lead_ids = crm_context.get("lead_ids", set())
    if not lead_ids:
        return actions

    orphans = 0
    sample = None
    for r in rows:
        lid = (r.get("LeadId") or "").strip()
        if lid and lid not in lead_ids:
            orphans += 1
            if sample is None:
                sample = (r.get("Id") or "").strip()

    if orphans > 0:
        # Reuse ORPHAN_OPP semantically — same severity, same meaning
        actions.append(_action(
            source_system, run_date, "ORPHAN_OPP", orphans,
            description="CampaignMember.LeadId not in sf_leads_raw for the day",
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            sample_record_id=sample,
        ))
    return actions


def compute_ops_actions(engine, container: ContainerClient,
                        source_system: str, run_date: datetime.date,
                        subfolder: str, filename_template: str,
                        property_lookup: dict) -> list:
    """Yardi ops: MISSING_PROPERTY (PropertyCode resolution) + OCCUPANCY_VARIANCE."""
    actions = []
    yyyymmdd = run_date.strftime("%Y%m%d")
    expected_filename = filename_template.format(yyyymmdd=yyyymmdd)
    blob_path = f"{subfolder}/{run_date.isoformat()}/{expected_filename}"

    try:
        rows = read_bronze_csv(container, blob_path, skip_preamble=True)
    except Exception as e:
        log.warning("[%s] Bronze file not found at %s: %s", source_system, blob_path, e)
        actions.append(_action(
            source_system, run_date, "MISSING_BRONZE", 1,
            description=f"Expected file {expected_filename} not present in bronze",
            bronze_path=blob_path,
        ))
        return actions

    bronze_total = len(rows)
    if bronze_total == 0:
        actions.append(_action(
            source_system, run_date, "MISSING_BRONZE", 1,
            description=f"File {expected_filename} present but empty",
            bronze_path=blob_path, bronze_rows_total=0,
        ))
        return actions

    # MISSING_PROPERTY — PropertyID column should resolve
    missing = 0
    sample_mp = None
    date_key = int(run_date.strftime("%Y%m%d"))
    for r in rows:
        pid_raw = (r.get("PropertyID") or "").strip()
        try:
            pk = int(re.sub(r"\D", "", pid_raw))
            if pk not in property_lookup:
                missing += 1
                if sample_mp is None:
                    sample_mp = f"{date_key}-P{pk}"
        except (ValueError, TypeError):
            missing += 1
            if sample_mp is None:
                sample_mp = f"{date_key}-P{pid_raw}"

    if missing > 0:
        actions.append(_action(
            source_system, run_date, "MISSING_PROPERTY", missing,
            description="Yardi PropertyID not in dim_property",
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            sample_record_id=sample_mp,
        ))

    # OCCUPANCY_VARIANCE: bronze occupancy_pct vs canonical OccupiedUnits/TotalUnits
    variance_count, sample_var = _compute_occupancy_variance(
        engine, run_date, rows, property_lookup
    )
    if variance_count > 0:
        actions.append(_action(
            source_system, run_date, "OCCUPANCY_VARIANCE", variance_count,
            description="Bronze occupancy differs from canonical by > 1%",
            bronze_path=blob_path, bronze_rows_total=bronze_total,
            sample_record_id=sample_var,
        ))

    return actions


def _compute_occupancy_variance(engine, run_date, rows, property_lookup):
    """Compare bronze OccupancyPct to canonical OccupiedUnits/TotalUnits."""
    date_key = int(run_date.strftime("%Y%m%d"))

    sql = """
        SELECT PropertyKey,
               CAST(OccupiedUnits AS FLOAT) /
                 NULLIF(CAST(OccupiedUnits + VacantUnits AS FLOAT), 0) AS pct
        FROM   dbo.fact_property_ops_daily
        WHERE  DataSource = 1
          AND  DateKey    = :dk
    """
    with connect_with_retry(engine) as conn:
        result = conn.execute(text(sql), {"dk": date_key}).fetchall()

    canonical_pct = {row[0]: row[1] for row in result if row[1] is not None}
    if not canonical_pct:
        return 0, None

    variance = 0
    sample = None
    for r in rows:
        pid_raw = (r.get("PropertyID") or "").strip()
        try:
            pk = int(re.sub(r"\D", "", pid_raw))
        except (ValueError, TypeError):
            continue
        if pk not in canonical_pct:
            continue

        bronze_pct_raw = (r.get("OccupancyPct") or "").strip()
        try:
            bronze_pct = float(bronze_pct_raw)
        except (ValueError, TypeError):
            continue

        if abs(bronze_pct - canonical_pct[pk]) > 0.01:
            variance += 1
            if sample is None:
                sample = f"{date_key}-P{pk}"

    return variance, sample


# ─── Action dict builder ─────────────────────────────────────────────────────
# Family-level SourceSystem values used by MISSING_FOLDER rows.
# These don't appear in ALL_SOURCES (which is keyed by per-file source),
# so we map them explicitly for PipelineKey resolution.
FAMILY_PIPELINE_KEYS = {
    "spend_family": PIPELINE_KEY_SPEND,
    "crm_family":   PIPELINE_KEY_CRM,
    "ops_family":   PIPELINE_KEY_OPS,
}


def _action(source_system, run_date, action_type, count,
            description=None, bronze_path=None,
            bronze_rows_total=None, bronze_rows_after_action=None,
            sample_record_id=None,
            delta_amount=None, delta_pct=None):
    """Build a reconciliation_actions row dict, ready for INSERT.

    delta_amount and delta_pct are populated only for SPEND_DIVERGENCE rows;
    they capture the financial materiality of the divergence (positive means
    bronze > canonical). NULL for non-spend action types.
    """
    # Resolve PipelineKey: per-file source from ALL_SOURCES, family-level
    # source from FAMILY_PIPELINE_KEYS, otherwise None.
    if source_system in FAMILY_PIPELINE_KEYS:
        pipeline_key = FAMILY_PIPELINE_KEYS[source_system]
    else:
        registry_entry = ALL_SOURCES.get(source_system)
        pipeline_key = registry_entry[2] if registry_entry else None

    # Resolve severity, escalating SPEND_DIVERGENCE to 'high' when the delta
    # is both materially large in dollars AND meaningfully large as a percent.
    severity = _escalate_spend_severity(action_type, delta_amount, delta_pct)

    return {
        "DateKey":               int(run_date.strftime("%Y%m%d")),
        "BronzeDate":            run_date,
        "SourceSystem":          source_system,
        "ActionType":            action_type,
        "ActionCount":           count,
        "Severity":              severity,
        "Description":           description,
        "BronzeRowsTotal":       bronze_rows_total,
        "BronzeRowsAfterAction": bronze_rows_after_action,
        "DeltaAmount":           delta_amount,
        "DeltaPct":              delta_pct,
        "BronzePath":            bronze_path,
        "SampleRecordId":        sample_record_id,
        "RunId":                 None,
        "PipelineKey":           pipeline_key,
    }


# ─── Persistence ─────────────────────────────────────────────────────────────
def replace_actions_for_date(engine, run_date: datetime.date, actions: list,
                             dry_run: bool = False):
    """
    Idempotent write: delete ALL existing rows for this date, then bulk
    insert the new actions. Re-running for the same date is safe — every
    row gets recomputed and any stale rows from a prior run (e.g., a
    MISSING_FOLDER row from before bronze was uploaded) get cleaned up
    automatically. See the comment block at the DELETE call for why
    whole-date scope is correct rather than per-source scope.
    """
    if not actions:
        log.info("No actions to write for %s", run_date)
        return

    if dry_run:
        log.info("DRY RUN — would write %d actions for %s:", len(actions), run_date)
        for a in actions:
            delta_str = ""
            if a.get("DeltaAmount") is not None:
                d_amt = a["DeltaAmount"]
                d_pct = a["DeltaPct"]
                if d_pct is not None:
                    delta_str = f" | delta=${d_amt:+,.2f} ({d_pct:+.2%})"
                else:
                    delta_str = f" | delta=${d_amt:+,.2f}"
            log.info("  %s | %s | %s | count=%d | severity=%s%s",
                     a["BronzeDate"], a["SourceSystem"], a["ActionType"],
                     a["ActionCount"], a["Severity"], delta_str)
        return

    # Delete ALL existing rows for this date so re-runs are fully idempotent
    # even when source systems change between runs.
    #
    # Earlier versions scoped the DELETE by (date, source_system), but that
    # left stale rows behind in this scenario:
    #
    #   Run 1 (folder missing):  writes ops_family | MISSING_FOLDER
    #   Run 2 (folder uploaded): writes ops_yardi | OCCUPANCY_VARIANCE
    #                            but ops_family is no longer in actions[],
    #                            so its DELETE never fires → stale row sticks
    #
    # The narrower scope was meant to allow partial re-runs of just one source
    # without disturbing others, but in practice the populator always runs the
    # FULL set of checks for a date — there's no per-source invocation. So
    # whole-date DELETE is correct for current behavior and prevents the
    # state-change bug.
    #
    # If we ever add per-source CLI invocation (e.g. --source spend), revisit
    # this and scope DELETE by whatever was actually computed in this run.
    date_key = int(run_date.strftime("%Y%m%d"))
    affected_sources = sorted({a["SourceSystem"] for a in actions})

    delete_sql = """
        DELETE FROM pipeline.reconciliation_actions
        WHERE DateKey = :dk
    """
    insert_sql = """
        INSERT INTO pipeline.reconciliation_actions
            (DateKey, BronzeDate, SourceSystem, ActionType, ActionCount, Severity,
             Description, BronzeRowsTotal, BronzeRowsAfterAction,
             DeltaAmount, DeltaPct,
             BronzePath, SampleRecordId, RunId, PipelineKey)
        VALUES
            (:DateKey, :BronzeDate, :SourceSystem, :ActionType, :ActionCount, :Severity,
             :Description, :BronzeRowsTotal, :BronzeRowsAfterAction,
             :DeltaAmount, :DeltaPct,
             :BronzePath, :SampleRecordId, :RunId, :PipelineKey)
    """

    with engine.begin() as conn:
        conn.execute(text(delete_sql), {"dk": date_key})
        conn.execute(text(insert_sql), actions)

    log.info("Wrote %d actions for %s across %d sources",
             len(actions), run_date, len(affected_sources))


# ─── Orchestration for a single date ─────────────────────────────────────────
def reconcile_date(engine, container: ContainerClient,
                   run_date: datetime.date,
                   property_lookup: dict, vendor_lookup: dict,
                   dry_run: bool = False):
    """Compute and persist all reconciliation actions for one date."""
    log.info("─" * 70)
    log.info("Reconciling %s", run_date.isoformat())
    log.info("─" * 70)

    all_actions = []
    date_key = int(run_date.strftime("%Y%m%d"))

    # ─── Folder-level MISSING_FOLDER check (per source family) ──────────
    # If a family's folder is empty or missing, emit ONE row and skip the
    # per-file checks. Avoids 6 MISSING_BRONZE rows when the real signal is
    # "the entire CRM source didn't deliver today."
    spend_folder_exists = folder_exists(container, "spend", run_date)
    crm_folder_exists   = folder_exists(container, "crm",   run_date)
    ops_folder_exists   = folder_exists(container, "ops",   run_date)

    if not spend_folder_exists:
        all_actions.append(_action(
            "spend_family", run_date, "MISSING_FOLDER", 1,
            description="bronze/spend/{date}/ does not exist or is empty",
            bronze_path=f"spend/{run_date.isoformat()}/",
            sample_record_id=f"{date_key}-spend-folder",
        ))
        log.warning("[%s] bronze/spend/ folder missing — skipping per-file Spend checks",
                    run_date)

    if not crm_folder_exists:
        all_actions.append(_action(
            "crm_family", run_date, "MISSING_FOLDER", 1,
            description="bronze/crm/{date}/ does not exist or is empty",
            bronze_path=f"crm/{run_date.isoformat()}/",
            sample_record_id=f"{date_key}-crm-folder",
        ))
        log.warning("[%s] bronze/crm/ folder missing — skipping per-file CRM checks",
                    run_date)

    if not ops_folder_exists:
        all_actions.append(_action(
            "ops_family", run_date, "MISSING_FOLDER", 1,
            description="bronze/ops/{date}/ does not exist or is empty",
            bronze_path=f"ops/{run_date.isoformat()}/",
            sample_record_id=f"{date_key}-ops-folder",
        ))
        log.warning("[%s] bronze/ops/ folder missing — skipping per-file Ops checks",
                    run_date)

    # ─── Per-file checks (only when folder exists) ──────────────────────
    if spend_folder_exists:
        for source_system, (subfolder, filename, _, vendor_keys) in SPEND_SOURCES.items():
            actions = compute_spend_actions(
                engine, container, source_system, run_date, subfolder, filename,
                vendor_keys, property_lookup, vendor_lookup,
            )
            all_actions.extend(actions)

    if crm_folder_exists:
        # CRM sources need shared context across files for orphan checks.
        # Order matters: leads first (populates lead_ids), contacts second
        # (populates contact_ids), then tasks/opps/campaign_members can check.
        crm_context = {}
        crm_order = ["crm_sf_leads", "crm_sf_contacts", "crm_sf_campaigns",
                     "crm_sf_opportunities", "crm_sf_tasks", "crm_sf_campaign_members"]
        for source_system in crm_order:
            subfolder, filename, _ = CRM_SOURCES[source_system]
            actions = compute_crm_actions(
                container, source_system, run_date, subfolder, filename,
                property_lookup, crm_context,
            )
            all_actions.extend(actions)

    if ops_folder_exists:
        for source_system, (subfolder, filename, _) in OPS_SOURCES.items():
            actions = compute_ops_actions(
                engine, container, source_system, run_date, subfolder, filename,
                property_lookup,
            )
            all_actions.extend(actions)

    # Summary
    by_severity = Counter(a["Severity"] for a in all_actions)
    log.info("[%s] Actions: %d total | high=%d med=%d low=%d",
             run_date, len(all_actions),
             by_severity["high"], by_severity["med"], by_severity["low"])

    replace_actions_for_date(engine, run_date, all_actions, dry_run=dry_run)


# ─── CLI ─────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="Compute and log reconciliation actions per source-day."
    )
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--date", help="Single date YYYY-MM-DD")
    grp.add_argument("--start", help="Start date for range mode YYYY-MM-DD "
                                     "(use with --end)")
    p.add_argument("--end", help="End date for range mode YYYY-MM-DD")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute but don't write to SQL")
    return p.parse_args()


def main():
    args = parse_args()

    if args.start:
        if not args.end:
            print("--start requires --end", file=sys.stderr)
            sys.exit(2)
        start = datetime.date.fromisoformat(args.start)
        end = datetime.date.fromisoformat(args.end)
        if end < start:
            print("--end must be >= --start", file=sys.stderr)
            sys.exit(2)
        dates = [start + datetime.timedelta(days=i)
                 for i in range((end - start).days + 1)]
    else:
        dates = [datetime.date.fromisoformat(args.date)]

    log.info("Reconciliation run: %d date(s) from %s to %s | dry_run=%s",
             len(dates), dates[0], dates[-1], args.dry_run)

    engine = get_engine()
    container = get_container_client()

    # Lookups loaded once and reused across all dates
    property_lookup = load_property_lookup(engine)
    vendor_lookup = load_vendor_lookup(engine)

    for d in dates:
        try:
            reconcile_date(engine, container, d, property_lookup, vendor_lookup,
                           dry_run=args.dry_run)
        except Exception as e:
            log.error("Failed reconciliation for %s: %s", d, e, exc_info=True)
            # Continue with next date — one bad day shouldn't kill the backfill

    engine.dispose()
    log.info("Reconciliation complete.")


if __name__ == "__main__":
    main()
