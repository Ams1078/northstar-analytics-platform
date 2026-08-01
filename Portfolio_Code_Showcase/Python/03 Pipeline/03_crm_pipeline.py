"""
crm_pipeline.py
===============
NorthStar MAA — CRM Pipeline (DataSource = 2)

Reads 6 Salesforce CSV source files, runs all 12 pipeline steps,
and writes to fact_leasing_daily and fact_prospect_journey.

SOURCE FILES
    Root folder is read from SOURCE_PATH (default ./mock_sources).
    Each run_date reads from a dated subfolder per the ETL architecture:

        {SOURCE_PATH}/{YYYY-MM-DD}/sf_leads_raw_{YYYYMMDD}.csv
        {SOURCE_PATH}/{YYYY-MM-DD}/sf_contacts_raw_{YYYYMMDD}.csv
        {SOURCE_PATH}/{YYYY-MM-DD}/sf_opportunities_raw_{YYYYMMDD}.csv
        {SOURCE_PATH}/{YYYY-MM-DD}/sf_tasks_raw_{YYYYMMDD}.csv
        {SOURCE_PATH}/{YYYY-MM-DD}/sf_campaigns_raw_{YYYYMMDD}.csv
        {SOURCE_PATH}/{YYYY-MM-DD}/sf_campaign_members_raw_{YYYYMMDD}.csv

    Legacy flat layout (all files directly under SOURCE_PATH) is still
    supported as a fallback for backward compatibility.

TARGET TABLES:
    dbo.fact_leasing_daily      — DataSource = 2
    dbo.fact_prospect_journey   — DataSource = 2

PIPELINE TABLES WRITTEN:
    pipeline.pipeline_runs
    pipeline.pipeline_flags
    pipeline.silver_quarantine
    pipeline.silver_processed
    pipeline.silver_prospects
    pipeline.campaign_vendor_lookup  (upserted from sf_campaigns_raw)

USAGE:
    python crm_pipeline.py
    python crm_pipeline.py --date 2026-04-11
    python crm_pipeline.py --date 2026-04-11 --dry-run

Requires .env file with SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD.
"""

import argparse
import csv
import datetime
import hashlib
import logging
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

from sqlalchemy import text

from pipeline_utils import (
    PIPELINE_CRM,
    FLAG_DIRTY_EMAIL, FLAG_DIRTY_PHONE, FLAG_DIRTY_NAME,
    FLAG_DIRTY_DATE, FLAG_DIRTY_NULL, FLAG_DIRTY_KEY,
    FLAG_DEDUP_AUTO, FLAG_DEDUP_PENDING,
    FLAG_ATTR_ORGANIC, FLAG_ATTR_CONFLICT,
    OUTCOME_LOADED, OUTCOME_FLAGGED_LOADED, OUTCOME_QUARANTINED,
    OUTCOME_DEDUP_SUPPRESSED, OUTCOME_ATTR_CONFLICT,
    get_engine, test_connection, get_source_path,
    get_watermark, update_watermark,
    start_run, finish_run, fail_run,
    write_flag, write_flags_batch,
    write_quarantine,
    write_processed, write_processed_batch,
    load_property_lookup, load_vendor_lookup,
    load_campaign_lookup, load_date_lookup,
    resolve_prospect_key, update_prospect_last_seen,
    resolve_prospect_keys_batch,
    date_str_to_key, normalize_sf_datetime,
    resolve_vendor_from_utm,
    check_attr_conflict,
)

log = logging.getLogger("crm_pipeline")

# ── Constants ─────────────────────────────────────────────────────────────────
DATASOURCE       = 2
ATTRIBUTION_DAYS = 7       # Locked: 7-day attribution window
DECAY_LAMBDA     = 0.1     # Locked: e^(-0.1 × DaysBeforeLease)

# Task.Type → FunnelStageKey (deterministic per crosswalk v1.2.1)
TASK_TYPE_TO_FUNNEL = {
    "Lead":  4,   # Lead inquiry
    "Tour":  3,   # Property tour / visit
    "Lease": 5,   # Lease signed
}

# Dirty data detection patterns
DIRTY_EMAIL_PATTERNS = {
    "test@", "noreply@", "donotcontact@", "@test.com", "@example.com",
    "@mailinator.com", "@guerrillamail.com", "fake@", "no@no",
}
DIRTY_PHONE_PATTERNS = {
    "5550000000", "0000000000", "1234567890", "1111111111",
    "2222222222", "9999999999",
}
DIRTY_NAME_PATTERNS = {
    "john doe", "test user", "test test", "aaaa", "xxxx",
    "n/a", "na", "none", "unknown",
}


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — FILE LOADING
# ══════════════════════════════════════════════════════════════════════════════

def resolve_source_dir(run_date: datetime.date) -> Path:
    """
    Resolve which directory to read source files from for a given run_date.

    Per the ETL architecture the canonical layout is a dated subfolder:
        {SOURCE_PATH}/{YYYY-MM-DD}/

    For backward compatibility we also support the legacy flat layout where
    all dated files live directly under SOURCE_PATH. The dated folder is
    preferred when it exists; the flat root is used only as a fallback.
    """
    root      = Path(get_source_path())
    dated_dir = root / run_date.strftime("%Y-%m-%d")

    if dated_dir.is_dir():
        log.info("Source layout: dated folder → %s", dated_dir)
        return dated_dir

    # Fallback to legacy flat layout — only accepted if root exists
    if root.is_dir():
        log.warning(
            "Source layout: flat fallback → %s "
            "(dated folder %s not found — consider regenerating sources "
            "into the dated layout)", root, dated_dir,
        )
        return root

    raise FileNotFoundError(
        f"No source directory found. Looked for:\n"
        f"  {dated_dir}  (preferred dated layout)\n"
        f"  {root}       (legacy flat layout)\n"
        f"Set SOURCE_PATH env var or run the mock source generator "
        f"for {run_date}."
    )


def load_source_files(run_date: datetime.date) -> dict:
    """
    Load all 6 SF source CSV files for the run date.
    Returns dict of {file_key: list_of_row_dicts}.
    Raises FileNotFoundError if any required file is missing.
    """
    date_str   = run_date.strftime("%Y%m%d")
    source_dir = resolve_source_dir(run_date)

    file_map = {
        "leads":      f"sf_leads_raw_{date_str}.csv",
        "contacts":   f"sf_contacts_raw_{date_str}.csv",
        "opps":       f"sf_opportunities_raw_{date_str}.csv",
        "tasks":      f"sf_tasks_raw_{date_str}.csv",
        "campaigns":  f"sf_campaigns_raw_{date_str}.csv",
        "members":    f"sf_campaign_members_raw_{date_str}.csv",
    }

    data = {}
    for key, fname in file_map.items():
        fpath = source_dir / fname
        if not fpath.exists():
            raise FileNotFoundError(
                f"Required source file not found: {fpath}\n"
                f"Run mock_source_generator.py --date {run_date} to generate it."
            )
        # utf-8-sig strips any BOM Excel may have added.
        # errors="replace" keeps us going when a row contains legacy
        # Windows-1252 bytes (em dash 0x97, smart quotes 0x91-0x94,
        # ellipsis 0x85, etc.) — each bad byte becomes '?' rather than
        # crashing the whole run. Real fix is regenerating the source
        # files as true UTF-8; this is a safety net, not the cure.
        with open(fpath, encoding="utf-8-sig", errors="replace", newline="") as f:
            rows = list(csv.DictReader(f))
        # Filter deleted records
        rows = [r for r in rows if r.get("IsDeleted", "False").strip() == "False"]
        data[key] = rows
        log.info("Loaded %s: %d rows", fname, len(rows))

    return data


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — CAMPAIGN LOOKUP UPSERT
# ══════════════════════════════════════════════════════════════════════════════

def upsert_campaign_lookup(engine, campaigns: list,
                            vendor_lookup: dict,
                            run_date: datetime.date) -> None:
    """
    Upsert campaign_vendor_lookup from sf_campaigns_raw.
    Maps VendorName__c and ChannelType__c to VendorKey/ChannelKey
    using the vendor_lookup table.

    All valid rows are collected first and then written in a single
    transaction. This is significantly faster than per-row begin/commit
    and keeps the upsert atomic — either all campaigns for this run
    land, or none do.
    """
    # Build VendorName → VendorKey reverse map
    vendor_name_map = {
        v["VendorName"].lower(): vk
        for vk, v in vendor_lookup.items()
    }

    batch = []
    for row in campaigns:
        campaign_id  = row.get("Id", "").strip()
        vendor_name  = row.get("VendorName__c", "").strip()
        channel_type = row.get("ChannelType__c", "").strip()
        campaign_name = row.get("Name", "").strip()

        if not campaign_id or not vendor_name:
            continue

        vendor_key = vendor_name_map.get(vendor_name.lower())
        if vendor_key is None:
            log.warning("Campaign %s has unknown vendor '%s' — skipping",
                        campaign_id, vendor_name)
            continue

        channel_key = vendor_lookup[vendor_key]["ChannelKey"]

        batch.append({
            "cid":   campaign_id,
            "vk":    vendor_key,
            "ck":    channel_key,
            "cname": campaign_name[:199],
            "vname": vendor_name[:99],
            "ctype": channel_type[:49],
        })

    if not batch:
        log.info("Campaign lookup upserted: 0 rows (no valid campaigns)")
        return

    sql = """
        MERGE pipeline.campaign_vendor_lookup AS target
        USING (VALUES (:cid, :vk, :ck, :cname, :vname, :ctype))
            AS source (CampaignId, VendorKey, ChannelKey,
                       CampaignName, VendorName, ChannelType)
        ON target.CampaignId = source.CampaignId
        WHEN MATCHED THEN UPDATE SET
            target.VendorKey    = source.VendorKey,
            target.ChannelKey   = source.ChannelKey,
            target.CampaignName = source.CampaignName,
            target.VendorName   = source.VendorName,
            target.ChannelType  = source.ChannelType,
            target.UpdatedAt    = SYSDATETIME()
        WHEN NOT MATCHED THEN INSERT
            (CampaignId, VendorKey, ChannelKey, CampaignName,
             VendorName, ChannelType, IsActive)
        VALUES
            (source.CampaignId, source.VendorKey, source.ChannelKey,
             source.CampaignName, source.VendorName, source.ChannelType, 1);
    """
    with engine.begin() as conn:
        conn.execute(text(sql), batch)

    log.info("Campaign lookup upserted: %d rows", len(batch))


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — PROPERTY KEY RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def resolve_property_key(prop_code: str) -> Optional[int]:
    """
    Convert Salesforce Property__c code to PropertyKey integer.
    Format: 'PROP00001' → 1, 'PROP00120' → 120.
    Returns None if code is invalid.
    """
    if not prop_code or not prop_code.startswith("PROP"):
        return None
    try:
        return int(prop_code[4:])
    except ValueError:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — DIRTY DATA DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def check_dirty(row: dict, run_date: datetime.date) -> list:
    """
    Run all dirty data checks on a lead row.
    Returns list of (flag_type, flag_field, original_value) tuples.
    Empty list = clean record.
    """
    flags = []

    email = (row.get("Email") or "").strip().lower()
    phone = (row.get("Phone") or "").strip()
    first = (row.get("FirstName") or "").strip()
    last  = (row.get("LastName") or "").strip()
    name  = f"{first} {last}".strip().lower()
    prop  = row.get("Property__c", "").strip()

    # DIRTY_NULL — no contact info at all
    if not email and not phone and not first and not last:
        flags.append((FLAG_DIRTY_NULL, "Email/Phone/Name", "all_null"))
        return flags  # No point checking further

    # DIRTY_EMAIL
    if email:
        if any(pattern in email for pattern in DIRTY_EMAIL_PATTERNS):
            flags.append((FLAG_DIRTY_EMAIL, "Email", row.get("Email", "")))
    
    # DIRTY_PHONE — normalize to digits only
    phone_digits = "".join(c for c in phone if c.isdigit())
    if phone_digits:
        if len(phone_digits) < 10 or phone_digits in DIRTY_PHONE_PATTERNS:
            flags.append((FLAG_DIRTY_PHONE, "Phone", phone))

    # DIRTY_NAME
    if name.strip():
        if (len(name.strip()) <= 1 or
            name in DIRTY_NAME_PATTERNS or
            len(set(name.replace(" ", ""))) <= 1):
            flags.append((FLAG_DIRTY_NAME, "FirstName/LastName", name))

    # DIRTY_KEY — unresolvable PropertyKey
    if not prop or resolve_property_key(prop) is None:
        flags.append((FLAG_DIRTY_KEY, "Property__c", prop))

    # DIRTY_DATE — future CreatedDate
    created_str = (row.get("CreatedDate") or "")[:10]
    if created_str:
        try:
            created = datetime.date.fromisoformat(created_str)
            if created > run_date:
                flags.append((FLAG_DIRTY_DATE, "CreatedDate", created_str))
        except ValueError:
            flags.append((FLAG_DIRTY_DATE, "CreatedDate", created_str))

    return flags


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — BUILD PROSPECT REGISTRY FROM SOURCE FILES
# ══════════════════════════════════════════════════════════════════════════════

def build_registry(data: dict,
                   run_date: datetime.date,
                   vendor_lookup: dict,
                   campaign_lookup: dict,
                   date_lookup: dict) -> dict:
    """
    Build an in-memory prospect registry keyed by lead_id.
    Enriches each lead with:
      - resolved PropertyKey
      - resolved VendorKey / ChannelKey
      - dirty data flags
      - matched opportunity (if converted)
      - matched tasks (touch sequence)
      - campaign member record

    Returns {lead_id: prospect_dict}
    """
    leads   = {r["Id"]: r for r in data["leads"]}
    opps    = {r["LeadId__c"]: r for r in data["opps"] if r.get("LeadId__c")}
    members = {r["LeadId"]: r for r in data["members"] if r.get("LeadId")}

    # Build task index: lead_id → sorted list of tasks
    task_index = defaultdict(list)
    for t in data["tasks"]:
        who_id = t.get("WhoId", "").strip()
        if who_id:
            task_index[who_id].append(t)
    # Sort each prospect's tasks by ActivityDate
    for who_id in task_index:
        task_index[who_id].sort(key=lambda t: t.get("ActivityDate", ""))

    registry = {}
    for lead_id, lead in leads.items():

        # Property resolution
        prop_code  = lead.get("Property__c", "").strip()
        prop_key   = resolve_property_key(prop_code)

        # Vendor resolution
        vk, ck, resolution = resolve_vendor_from_utm(
            utm_source    = lead.get("UTM_Source__c"),
            utm_medium    = lead.get("UTM_Medium__c"),
            campaign_id   = lead.get("Campaign__c"),
            campaign_lookup = campaign_lookup,
            vendor_lookup   = vendor_lookup,
        )

        # Dirty data flags
        dirty_flags = check_dirty(lead, run_date)
        is_dirty    = bool(dirty_flags)

        # Opportunity match (converted leads only)
        opp = opps.get(lead_id)

        # Lease date from opportunity
        lease_date_str = None
        lease_date_key = None
        lease_value    = None
        if opp:
            lease_date_str = (opp.get("LeaseStartDate__c") or
                              opp.get("CloseDate") or "")[:10]
            lease_date_key = date_str_to_key(lease_date_str)
            monthly_rent   = opp.get("MonthlyRent__c")
            if monthly_rent:
                try:
                    lease_value = round(float(monthly_rent) * 12, 2)
                except (ValueError, TypeError):
                    lease_value = None

        # Tasks for this prospect
        tasks = task_index.get(lead_id, [])

        # Campaign member
        member = members.get(lead_id)

        # Created date → DateKey
        created_str = (lead.get("CreatedDate") or "")[:10]
        created_key = date_str_to_key(created_str.replace("-", ""))

        registry[lead_id] = {
            "lead_id":        lead_id,
            "lead":           lead,
            "prop_code":      prop_code,
            "prop_key":       prop_key,
            "vendor_key":     vk,
            "channel_key":    ck,
            "resolution":     resolution,
            "dirty_flags":    dirty_flags,
            "is_dirty":       is_dirty,
            "is_converted":   lead.get("IsConverted", "False").strip() == "True",
            "opp":            opp,
            "lease_date_str": lease_date_str,
            "lease_date_key": lease_date_key,
            "lease_value":    lease_value,
            "tasks":          tasks,
            "member":         member,
            "created_str":    created_str,
            "created_key":    created_key,
        }

    log.info("Registry built: %d prospects (%d converted)",
             len(registry),
             sum(1 for p in registry.values() if p["is_converted"]))
    return registry


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5b — DEDUPLICATION
# ══════════════════════════════════════════════════════════════════════════════
#
# Matching rules (per NorthStar_CRM_ETL_Pipeline_Documentation, Step 8):
#   EXACT  — same email  + same property + within 7 days  → auto-merge
#   PHONE  — same phone  + same property + within 14 days → auto-merge
#   FUZZY  — same first+last + same property + within 14 days → DEDUP_PENDING
#
# Completeness scoring (master-wins, suppressed-loses):
#   email present: +3  |  phone present: +2  |  full name: +2
#   vendor resolved: +2  |  channel resolved: +1
#
# Auto-merged duplicates are marked suppressed=True on the prospect dict so
# the main processing loop can skip them for gold writes while still logging
# them to silver_processed as OUTCOME_DEDUP_SUPPRESSED with the master key.
# ══════════════════════════════════════════════════════════════════════════════

EXACT_WINDOW_DAYS = 7
PHONE_WINDOW_DAYS = 14
FUZZY_WINDOW_DAYS = 14


def _completeness_score(prospect: dict) -> int:
    """Record completeness score used to pick the master in auto-merges."""
    lead = prospect["lead"]
    score = 0
    if (lead.get("Email") or "").strip():
        score += 3
    if (lead.get("Phone") or "").strip() or (lead.get("MobilePhone") or "").strip():
        score += 2
    first = (lead.get("FirstName") or "").strip()
    last  = (lead.get("LastName")  or "").strip()
    if first and last:
        score += 2
    if prospect.get("vendor_key") is not None:
        score += 2
    if prospect.get("channel_key") is not None:
        score += 1
    return score


def _parse_created(prospect: dict) -> Optional[datetime.date]:
    """Parse CreatedDate for windowing. None if unparseable."""
    created_str = (prospect["lead"].get("CreatedDate") or "")[:10]
    if not created_str:
        return None
    try:
        return datetime.date.fromisoformat(created_str)
    except ValueError:
        return None


def dedup_registry(registry: dict) -> list:
    """
    Scan the prospect registry for duplicates per the documented rules.

    Returns a list of dedup_event dicts:
        {
          "master_lead_id":      str,
          "suppressed_lead_id":  str,
          "master_prospect_key": Optional[int],  # filled in after resolve
          "match_type":          "EXACT" | "PHONE" | "FUZZY",
          "flag_type":           FLAG_DEDUP_AUTO | FLAG_DEDUP_PENDING,
          "field":               "Email" | "Phone" | "Name",
          "value":               str,
        }

    Side effects on the registry:
      - For AUTO matches (email, phone): the suppressed prospect dict gets
        `suppressed = True` and `master_lead_id = <master>`. The main loop
        uses these to route the record to OUTCOME_DEDUP_SUPPRESSED instead
        of writing it to gold.
      - For PENDING matches (fuzzy name): NEITHER record is suppressed — both
        proceed to gold. The flag is informational and drives the manual
        review queue.
    """
    # Separator so "John" + "Smithson" can't collide with "Johns" + "Mithson"
    SEP = "\x1f"

    exact_index: dict = {}   # (email, prop) -> [(created, lead_id), ...]
    phone_index: dict = {}   # (phone10, prop) -> [(created, lead_id), ...]
    fuzzy_index: dict = {}   # (first, last, prop) -> [(created, lead_id), ...]

    def _push(idx: dict, key: tuple, created: datetime.date, lid: str) -> None:
        idx.setdefault(key, []).append((created, lid))

    # Build indexes from all non-suppressed prospects with a resolvable property
    for lid, p in registry.items():
        if p.get("prop_key") is None:
            continue
        created = _parse_created(p)
        if created is None:
            continue

        lead  = p["lead"]
        email = (lead.get("Email") or "").strip().lower()
        phone_raw = (lead.get("Phone") or lead.get("MobilePhone") or "")
        phone = "".join(c for c in phone_raw if c.isdigit())
        first = (lead.get("FirstName") or "").strip().lower()
        last  = (lead.get("LastName")  or "").strip().lower()

        if email:
            _push(exact_index, (email, p["prop_key"]), created, lid)
        if phone and len(phone) >= 10:
            # Normalize to last 10 digits (country-code tolerant)
            _push(phone_index, (phone[-10:], p["prop_key"]), created, lid)
        if first and last:
            _push(fuzzy_index, (first + SEP + last, p["prop_key"]),
                  created, lid)

    events: list = []
    already_suppressed: set = set()

    def _pick_master(candidates: list) -> str:
        """Highest completeness wins; ties break on earliest CreatedDate, then lead_id."""
        def _key(lid):
            p = registry[lid]
            return (
                -_completeness_score(p),
                _parse_created(p) or datetime.date.max,
                lid,
            )
        return sorted(candidates, key=_key)[0]

    def _within(dates: list, window_days: int) -> bool:
        """All dates fall within window_days of each other."""
        if len(dates) < 2:
            return True
        return (max(dates) - min(dates)).days <= window_days

    # ── EXACT: email + property + 7-day window ────────────────────────────────
    for (email, _prop), rows in exact_index.items():
        if len(rows) < 2:
            continue
        dates = [d for d, _ in rows]
        lids  = [lid for _, lid in rows]
        if not _within(dates, EXACT_WINDOW_DAYS):
            continue

        master = _pick_master(lids)
        for lid in lids:
            if lid == master or lid in already_suppressed:
                continue
            registry[lid]["suppressed"]     = True
            registry[lid]["master_lead_id"] = master
            already_suppressed.add(lid)
            events.append({
                "master_lead_id":     master,
                "suppressed_lead_id": lid,
                "master_prospect_key": None,
                "match_type":         "EXACT",
                "flag_type":          FLAG_DEDUP_AUTO,
                "field":              "Email",
                "value":              email,
            })

    # ── PHONE: phone + property + 14-day window ───────────────────────────────
    for (phone, _prop), rows in phone_index.items():
        # Remove already-suppressed rows — once a record is merged via email,
        # it shouldn't also be merged via phone under a different master
        rows = [(d, lid) for d, lid in rows if lid not in already_suppressed]
        if len(rows) < 2:
            continue
        dates = [d for d, _ in rows]
        lids  = [lid for _, lid in rows]
        if not _within(dates, PHONE_WINDOW_DAYS):
            continue

        master = _pick_master(lids)
        for lid in lids:
            if lid == master or lid in already_suppressed:
                continue
            registry[lid]["suppressed"]     = True
            registry[lid]["master_lead_id"] = master
            already_suppressed.add(lid)
            events.append({
                "master_lead_id":     master,
                "suppressed_lead_id": lid,
                "master_prospect_key": None,
                "match_type":         "PHONE",
                "flag_type":          FLAG_DEDUP_AUTO,
                "field":              "Phone",
                "value":              phone,
            })

    # ── FUZZY: name + property + 14-day window → pending, not suppressed ──────
    for (name_key, _prop), rows in fuzzy_index.items():
        if len(rows) < 2:
            continue
        # Skip groups already fully resolved by email/phone
        rows = [(d, lid) for d, lid in rows if lid not in already_suppressed]
        if len(rows) < 2:
            continue
        dates = [d for d, _ in rows]
        lids  = [lid for _, lid in rows]
        if not _within(dates, FUZZY_WINDOW_DAYS):
            continue

        # Name is informational, not authoritative — flag every pair
        # but do not suppress. Picks the highest-complete as "master"
        # just so the queue has a consistent pointer.
        master = _pick_master(lids)
        display_name = name_key.replace(SEP, " ")
        for lid in lids:
            if lid == master:
                continue
            events.append({
                "master_lead_id":     master,
                "suppressed_lead_id": lid,   # flagged, not suppressed
                "master_prospect_key": None,
                "match_type":         "FUZZY",
                "flag_type":          FLAG_DEDUP_PENDING,
                "field":              "Name",
                "value":              display_name,
            })

    n_auto    = sum(1 for e in events if e["flag_type"] == FLAG_DEDUP_AUTO)
    n_pending = sum(1 for e in events if e["flag_type"] == FLAG_DEDUP_PENDING)
    log.info("Dedup complete: %d auto-merged, %d pending review "
             "(total events: %d)", n_auto, n_pending, len(events))
    return events


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — ATTRIBUTION COMPUTATION
# ══════════════════════════════════════════════════════════════════════════════

def compute_attribution(tasks: list,
                         lease_date_key: int,
                         vendor_lookup: dict,
                         campaign_lookup: dict) -> tuple:
    """
    For a converted prospect, compute time-decay attribution credit
    across all touches within the 7-day attribution window.

    Returns a tuple: (touches, reason)
        touches: list of touch dicts with AttributedCredit values summing to 1.0
                 (empty list if no attribution could be computed)
        reason:  diagnostic code for why touches is empty, one of:
                   "OK"             — touches were produced
                   "NO_TASKS"       — prospect had zero tasks attached
                   "NO_LEASE_DATE"  — lease_date_key was missing/zero
                   "BAD_DATES"      — tasks present but none had parseable
                                      ActivityDate values
                   "OUT_OF_WINDOW"  — tasks present and parseable, but none
                                      fell within the 7-day attribution window

    Uses journey-shape-preserving touch selection for >3 touches.
    Attribution formula: e^(-DECAY_LAMBDA × DaysBeforeLease)
    Normalized so all credits sum to exactly 1.0.
    """
    if not lease_date_key:
        return [], "NO_LEASE_DATE"
    if not tasks:
        return [], "NO_TASKS"

    lease_date = datetime.date(
        int(str(lease_date_key)[:4]),
        int(str(lease_date_key)[4:6]),
        int(str(lease_date_key)[6:8]),
    )

    # Filter to tasks within attribution window — track date-parse outcomes
    # so the caller can distinguish "all dates were garbage" from "dates
    # were fine but none were close enough to the lease".
    windowed = []
    n_parsed = 0
    for t in tasks:
        act_str = (t.get("ActivityDate") or "")[:10]
        if not act_str:
            continue
        try:
            act_date = datetime.date.fromisoformat(act_str)
        except ValueError:
            continue
        n_parsed += 1
        days_before = (lease_date - act_date).days
        if 0 <= days_before <= ATTRIBUTION_DAYS:
            windowed.append((t, days_before))

    if not windowed:
        # Differentiate: did parsing fail for every task, or did parsing
        # succeed but every task fell outside the window?
        return [], ("BAD_DATES" if n_parsed == 0 else "OUT_OF_WINDOW")

    # Journey-shape-preserving selection for >3 touches
    if len(windowed) > 3:
        # Initiator = earliest, Converter = latest, Influencer = midpoint
        windowed_sorted = sorted(windowed, key=lambda x: x[0].get("ActivityDate", ""))
        initiator  = windowed_sorted[0]
        converter  = windowed_sorted[-1]
        mid_idx    = len(windowed_sorted) // 2
        influencer = windowed_sorted[mid_idx]
        windowed   = [initiator, influencer, converter]
        # Deduplicate if mid == first or last
        seen = set()
        unique = []
        for item in windowed:
            tid = item[0].get("Id", "")
            if tid not in seen:
                seen.add(tid)
                unique.append(item)
        windowed = unique

    # Compute raw weights: e^(-λ × days)
    raw_weights = [math.exp(-DECAY_LAMBDA * days) for _, days in windowed]
    total = sum(raw_weights)

    touches = []
    touch_number = 1
    total_touches = len(windowed)

    for (task, days_before), raw_w in zip(windowed, raw_weights):
        credit      = round(raw_w / total, 6) if total > 0 else 0.0
        task_type   = task.get("Type", "Lead").strip()
        funnel_key  = TASK_TYPE_TO_FUNNEL.get(task_type, 4)

        # Resolve vendor/channel for this specific touch via TouchChannel__c
        # TouchChannel__c is the channel label written by the generator
        # For attribution we use the lead's vendor (all touches share lead vendor)
        # In production this could be touch-level vendor resolution
        touch_channel = task.get("TouchChannel__c", "")

        is_direct   = 1 if touch_number == total_touches else 0
        is_assisted = 1 if touch_number < total_touches else 0

        touches.append({
            "task_id":         task.get("Id", ""),
            "activity_date":   task.get("ActivityDate", "")[:10],
            "task_type":       task_type,
            "funnel_stage_key": funnel_key,
            "touch_number":    touch_number,
            "total_touches":   total_touches,
            "days_before_lease": days_before,
            "attributed_credit": credit,
            "is_direct_credit":  is_direct,
            "is_assisted_credit": is_assisted,
            "touch_channel":   touch_channel,
        })
        touch_number += 1

    return touches, "OK"


# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — GOLD WRITES
# ══════════════════════════════════════════════════════════════════════════════

def upsert_fact_leasing_daily(engine, run_date: datetime.date,
                               property_summary: dict) -> int:
    """
    Upsert fact_leasing_daily for DataSource=2.
    property_summary: {property_key: {leads, new_leases, visits,
                                      attributed_leases, unattributed_leases}}
    Returns number of rows upserted.

    Rows are batched into a single executemany — one transaction for
    all 120 properties — instead of per-row begin/commit.
    """
    if not property_summary:
        log.info("fact_leasing_daily: no properties to upsert for %s", run_date)
        return 0

    date_key = int(run_date.strftime("%Y%m%d"))

    sql = """
        MERGE dbo.fact_leasing_daily AS target
        USING (VALUES (
            :date_key, :prop_key, :leads, :new_leases, :visits,
            :attr_leases, :unattr_leases, :datasource
        )) AS source (
            DateKey, PropertyKey, Leads, NewLeases, Visits,
            AttributedNewLeases, UnattributedLeases, DataSource
        )
        ON  target.DateKey      = source.DateKey
        AND target.PropertyKey  = source.PropertyKey
        AND target.DataSource   = source.DataSource
        WHEN MATCHED THEN UPDATE SET
            target.Leads                = source.Leads,
            target.NewLeases            = source.NewLeases,
            target.Visits               = source.Visits,
            target.AttributedNewLeases  = source.AttributedNewLeases,
            target.UnattributedLeases   = source.UnattributedLeases
        WHEN NOT MATCHED THEN INSERT (
            DateKey, PropertyKey, Leads, NewLeases, Visits,
            AttributedNewLeases, UnattributedLeases, DataSource
        ) VALUES (
            source.DateKey, source.PropertyKey, source.Leads,
            source.NewLeases, source.Visits,
            source.AttributedNewLeases, source.UnattributedLeases,
            source.DataSource
        );
    """

    batch = []
    for prop_key, summary in property_summary.items():
        attr   = summary.get("attributed_leases", 0)
        total  = summary.get("new_leases", 0)
        unattr = max(0, total - attr)
        batch.append({
            "date_key":     date_key,
            "prop_key":     prop_key,
            "leads":        summary.get("leads", 0),
            "new_leases":   total,
            "visits":       summary.get("visits", 0),
            "attr_leases":  attr,
            "unattr_leases": unattr,
            "datasource":   DATASOURCE,
        })

    with engine.begin() as conn:
        conn.execute(text(sql), batch)

    log.info("fact_leasing_daily upserted: %d property rows for %s",
             len(batch), run_date)
    return len(batch)


def upsert_fact_prospect_journey(engine, run_date: datetime.date,
                                  journey_rows: list) -> int:
    """
    Upsert fact_prospect_journey rows for DataSource=2.
    journey_rows: list of dicts with all required fact columns.
    Returns number of rows upserted.

    Rows are written in chunked executemany batches. This is the
    largest volume write in the CRM pipeline — a converting prospect
    with 3 touches × ~40 leases/day × 120 properties can produce
    several thousand journey rows — so per-row begin/commit is
    prohibitively slow. Chunk size is tuned to stay under typical
    ODBC parameter limits.
    """
    if not journey_rows:
        return 0

    sql = """
        MERGE dbo.fact_prospect_journey AS target
        USING (VALUES (
            :prospect_key, :prop_key, :date_key, :vendor_key, :channel_key,
            :funnel_key, :touch_num, :total_touches, :days_before,
            :lease_date_key, :converted, :attr_credit,
            :is_direct, :is_assisted, :lease_value, :datasource
        )) AS source (
            ProspectKey, PropertyKey, DateKey, VendorKey, ChannelKey,
            FunnelStageKey, TouchNumber, TotalTouches, DaysBeforeLease,
            LeaseDateKey, Converted, AttributedCredit,
            IsDirectCredit, IsAssistedCredit, LeaseValueAnnual, DataSource
        )
        ON  target.ProspectKey  = source.ProspectKey
        AND target.TouchNumber  = source.TouchNumber
        AND target.DataSource   = source.DataSource
        WHEN MATCHED THEN UPDATE SET
            target.AttributedCredit  = source.AttributedCredit,
            target.IsDirectCredit    = source.IsDirectCredit,
            target.IsAssistedCredit  = source.IsAssistedCredit,
            target.LeaseValueAnnual  = source.LeaseValueAnnual
        WHEN NOT MATCHED THEN INSERT (
            ProspectKey, PropertyKey, DateKey, VendorKey, ChannelKey,
            FunnelStageKey, TouchNumber, TotalTouches, DaysBeforeLease,
            LeaseDateKey, Converted, AttributedCredit,
            IsDirectCredit, IsAssistedCredit, LeaseValueAnnual, DataSource
        ) VALUES (
            source.ProspectKey, source.PropertyKey, source.DateKey,
            source.VendorKey, source.ChannelKey,
            source.FunnelStageKey, source.TouchNumber, source.TotalTouches,
            source.DaysBeforeLease, source.LeaseDateKey, source.Converted,
            source.AttributedCredit, source.IsDirectCredit,
            source.IsAssistedCredit, source.LeaseValueAnnual, source.DataSource
        );
    """

    # pyodbc caps a single statement at 2,100 parameters.
    # 16 named params × 125 rows = 2,000 — safely under the ceiling.
    CHUNK = 125
    total = 0
    for i in range(0, len(journey_rows), CHUNK):
        chunk = journey_rows[i:i + CHUNK]
        with engine.begin() as conn:
            conn.execute(text(sql), chunk)
        total += len(chunk)

    log.info("fact_prospect_journey upserted: %d touch rows (%d batches)",
             total, (total + CHUNK - 1) // CHUNK)
    return total


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_crm_pipeline(run_date: datetime.date, dry_run: bool = False) -> None:
    """
    Execute the full 12-step CRM pipeline for the given run_date.
    """
    log.info("=" * 60)
    log.info("CRM PIPELINE — %s%s", run_date, "  [DRY RUN]" if dry_run else "")
    log.info("=" * 60)

    # Pre-declare so the except block can safely reference them even if
    # a failure in step 1/2/3 means they never got assigned. Without this,
    # an early failure would raise NameError in the except handler and
    # mask the real exception.
    engine: Optional[object] = None
    run_id: Optional[int]    = None

    # Counters
    rows_extracted    = 0
    rows_cleansed     = 0
    rows_loaded       = 0
    rows_flagged      = 0
    rows_quarantined  = 0
    rows_attr_conflict = 0

    try:
        # ── Step 1: Connect ───────────────────────────────────────────────────
        engine = get_engine()
        test_connection(engine)

        # ── Step 2: Read watermark ────────────────────────────────────────────
        watermark = get_watermark(engine, PIPELINE_CRM)

        # ── Step 3: Start run log ─────────────────────────────────────────────
        run_id = start_run(engine, PIPELINE_CRM, run_date, watermark_used=watermark)

        # ── Step 4: Load source files ─────────────────────────────────────────
        data = load_source_files(run_date)
        rows_extracted = sum(len(v) for v in data.values())
        log.info("Total rows extracted: %d", rows_extracted)

        # ── Step 5: Load lookups ──────────────────────────────────────────────
        property_lookup  = load_property_lookup(engine)
        vendor_lookup    = load_vendor_lookup(engine)
        date_lookup      = load_date_lookup(engine)

        # ── Step 6: Upsert campaign lookup from today's sf_campaigns_raw ──────
        upsert_campaign_lookup(engine, data["campaigns"],
                               vendor_lookup, run_date)
        # Reload after upsert so new campaigns are available for attribution
        campaign_lookup = load_campaign_lookup(engine)

        # ── Step 7: Build prospect registry ───────────────────────────────────
        registry = build_registry(
            data, run_date, vendor_lookup, campaign_lookup, date_lookup
        )

        # ── Step 7b: Dedup scan ──────────────────────────────────────────────
        # Produces AUTO merges (email, phone) that suppress records from gold,
        # and PENDING events (fuzzy name) that flag but do not suppress.
        dedup_events         = dedup_registry(registry)
        rows_dedup_suppressed = 0
        rows_dedup_pending    = 0

        # Per-reason buckets for converted leads that produced zero journey
        # rows. Populated by the attribution branch in the main loop;
        # surfaced in the summary and used to make low-coverage warnings
        # actionable. Keys: NO_TASKS, OUT_OF_WINDOW, BAD_DATES, NO_LEASE_DATE
        unattrib_reasons: dict = {}

        # ── Step 8: Process each prospect — cleanse, flag, quarantine ─────────
        property_summary = defaultdict(lambda: {
            "leads": 0, "new_leases": 0, "visits": 0,
            "attributed_leases": 0
        })
        journey_rows   = []
        processed_recs = []
        flags_batch    = []

        # ── Step 7c: Bulk-resolve ProspectKeys for every prospect at once ─────
        # Was: 2 × 2703 sequential round-trips to Azure (~30 min, fragile
        #      against any network hiccup — three consecutive runs died here
        #      before this change).
        # Now: 3 round-trips total via temp-table-join bulk SELECT/INSERT/UPDATE
        #      in pipeline_utils.resolve_prospect_keys_batch (~1 second).
        #
        # Resolves for ALL prospects whose PropertyKey is valid — the main
        # loop can then just dict-lookup instead of round-tripping per row.
        # The IsMaster=1 → IsMaster=0 flip for suppressed records still runs
        # as a separate batched UPDATE below, just as before.
        #
        # IMPORTANT — quarantine alignment:
        # We must not insert silver_prospects rows for leads the main loop
        # will later quarantine for bad/missing PropertyKey. silver_prospects
        # has no FK to dim_property (verified against schema_prep), so bad
        # inserts would succeed silently and poison future runs. Pre-filter
        # here using the same two criteria the main loop uses at Step 8:
        #   (a) prop_key is None  → unparseable Property__c source value
        #   (b) prop_key not in property_lookup → orphan key not in dim_property
        # Records matching either are skipped here and routed through the
        # main loop's quarantine path as normal. The count is logged for
        # parity with the main loop's rows_quarantined metric.
        skipped_for_bad_property = sum(
            1 for p in registry.values()
            if p.get("prop_key") is None
               or p.get("prop_key") not in property_lookup
        )
        if skipped_for_bad_property:
            log.info(
                "silver_prospects: skipping %d prospects with bad PropertyKey "
                "(will be quarantined in main loop)",
                skipped_for_bad_property,
            )

        if not dry_run:
            batch_input = [
                {
                    "sfid":        lid,
                    "prop_key":    p.get("prop_key"),
                    "vendor_key":  p.get("vendor_key"),
                    "channel_key": p.get("channel_key"),
                }
                for lid, p in registry.items()
                if p.get("prop_key") is not None
                   and p.get("prop_key") in property_lookup
            ]
            sfid_to_key = resolve_prospect_keys_batch(
                engine, batch_input, run_date,
            )
            for lid, p in registry.items():
                pk = sfid_to_key.get(lid)
                if pk is not None:
                    p["prospect_key"] = pk
            log.info("silver_prospects: resolved %d ProspectKeys in batch",
                     len(sfid_to_key))

            # Flip silver_prospects.IsMaster=0 and set MasterProspectKey on
            # each DEDUP_AUTO-suppressed record. The schema's silver_prospects
            # table holds IsMaster and MasterProspectKey columns for this
            # purpose (pipeline_schema_prep section 3.7). Run after the batch
            # resolver so every suppressed record has a ProspectKey to link
            # back to its master.
            flip_sql = text("""
                UPDATE pipeline.silver_prospects
                SET    IsMaster          = 0,
                       MasterProspectKey = :master_key,
                       UpdatedAt         = SYSDATETIME()
                WHERE  SourceSalesforceId = :sfid
                  AND  (IsMaster = 1 OR MasterProspectKey IS NULL)
            """)
            flip_batch = []
            for ev in dedup_events:
                if ev["flag_type"] != FLAG_DEDUP_AUTO:
                    continue
                master = registry.get(ev["master_lead_id"], {})
                mk = master.get("prospect_key")
                if mk is None:
                    continue
                flip_batch.append({
                    "master_key": mk,
                    "sfid":       ev["suppressed_lead_id"],
                })
            if flip_batch:
                with engine.begin() as conn:
                    conn.execute(flip_sql, flip_batch)
                log.info("silver_prospects: flipped %d records to "
                         "IsMaster=0 with MasterProspectKey pointer",
                         len(flip_batch))
        else:
            # Dry run — deterministic hashes, no DB touch. Skip prospects
            # that will be quarantined in the main loop so dry-run behavior
            # matches live-run behavior: "prospect_key is None" signals
            # "will be quarantined" in both modes.
            for lid, p in registry.items():
                if (p.get("prop_key") is None
                        or p.get("prop_key") not in property_lookup):
                    continue
                p["prospect_key"] = int(
                    int(hashlib.md5(lid.encode()).hexdigest(), 16)
                    % 2_147_483_647
                )

        for lead_id, prospect in registry.items():
            prop_key   = prospect["prop_key"]
            vendor_key = prospect["vendor_key"]
            channel_key = prospect["channel_key"]

            # ── Suppressed by dedup (EXACT email or PHONE match) ─────────────
            # ProspectKey was pre-resolved above so the dedup flag emission
            # can populate SuppressedKey correctly (= the suppressed record's
            # own ProspectKey, per pipeline_flags schema line 405-406).
            # Routed to silver_processed as DEDUP_SUPPRESSED with the master's
            # Salesforce Id in DuplicateOf. Does NOT go to gold.
            if prospect.get("suppressed"):
                processed_recs.append({
                    "run_id":          run_id,
                    "run_date":        run_date,
                    "pipeline_key":    PIPELINE_CRM,
                    "source_object":   "Lead",
                    "source_record_id": lead_id,
                    "outcome":         OUTCOME_DEDUP_SUPPRESSED,
                    "prospect_key":    prospect.get("prospect_key"),
                    "property_key":    prop_key,
                    "vendor_key":      vendor_key,
                    "flag_summary":    FLAG_DEDUP_AUTO,
                    "gold_table":      None,
                    "duplicate_of":    prospect.get("master_lead_id"),
                })
                rows_dedup_suppressed += 1
                continue

            # ── Quarantine: unresolvable PropertyKey ──────────────────────────
            if prop_key is None:
                write_quarantine(
                    engine, run_id, run_date, PIPELINE_CRM,
                    source_object="Lead",
                    quarantine_reason=FLAG_DIRTY_KEY,
                    raw_data=prospect["lead"],
                    source_record_id=lead_id,
                    notes=f"Property__c={prospect['prop_code']} not resolvable",
                )
                processed_recs.append({
                    "run_id": run_id, "run_date": run_date,
                    "pipeline_key": PIPELINE_CRM,
                    "source_object": "Lead",
                    "source_record_id": lead_id,
                    "outcome": OUTCOME_QUARANTINED,
                    "property_key": None,
                    "vendor_key": vendor_key,
                    "flag_summary": FLAG_DIRTY_KEY,
                    "gold_table": None,
                })
                rows_quarantined += 1
                continue

            # ── Validate PropertyKey exists in dim_property ───────────────────
            if prop_key not in property_lookup:
                write_quarantine(
                    engine, run_id, run_date, PIPELINE_CRM,
                    source_object="Lead",
                    quarantine_reason=FLAG_DIRTY_KEY,
                    raw_data=prospect["lead"],
                    source_record_id=lead_id,
                    notes=f"PropertyKey={prop_key} not in dim_property",
                )
                rows_quarantined += 1
                continue

            # ── Dirty data flags ──────────────────────────────────────────────
            flag_types = []
            for flag_type, flag_field, orig_val in prospect["dirty_flags"]:
                flags_batch.append({
                    "run_id":          run_id,
                    "run_date":        run_date,
                    "pipeline_key":    PIPELINE_CRM,
                    "source_object":   "Lead",
                    "source_record_id": lead_id,
                    "flag_type":       flag_type,
                    "flag_field":      flag_field,
                    "original_value":  orig_val,
                    "resolved_value":  None,
                })
                flag_types.append(flag_type)
                rows_flagged += 1

            # ── Flag organic resolution ───────────────────────────────────────
            if vendor_key == 12:
                flags_batch.append({
                    "run_id":          run_id,
                    "run_date":        run_date,
                    "pipeline_key":    PIPELINE_CRM,
                    "source_object":   "Lead",
                    "source_record_id": lead_id,
                    "flag_type":       FLAG_ATTR_ORGANIC,
                    "flag_field":      "UTM_Source__c/Campaign__c",
                    "original_value":  prospect["lead"].get("Campaign__c"),
                    "resolved_value":  "VendorKey=12 (Organic/Direct)",
                })
                flag_types.append(FLAG_ATTR_ORGANIC)
                rows_flagged += 1

            # ── Read pre-resolved ProspectKey ─────────────────────────────────
            # Step 7c populated prospect["prospect_key"] for every registry
            # entry using a single batch SELECT+INSERT against silver_prospects.
            # Main-loop round-trips per prospect are eliminated — we just read
            # the dict. This is what collapses the connection-open window from
            # ~30 minutes down to seconds.
            prospect_key = prospect.get("prospect_key")
            if prospect_key is None:
                # Only reachable if a prospect had no PropertyKey (filtered
                # out of the batch input). Fall back to a deterministic hash
                # so downstream code has something valid to use; this record
                # will also be routed to quarantine in the next block.
                prospect_key = int(
                    int(hashlib.md5(lead_id.encode()).hexdigest(), 16)
                    % 2_147_483_647
                )
                prospect["prospect_key"] = prospect_key

            # ── Count leads per property ──────────────────────────────────────
            property_summary[prop_key]["leads"] += 1

            # ── Count new leases (converted prospects) ────────────────────────
            is_converted = prospect["is_converted"]
            if is_converted and prospect["opp"]:
                property_summary[prop_key]["new_leases"] += 1

                # Count visits from Tour-type tasks
                tours = sum(1 for t in prospect["tasks"]
                            if t.get("Type", "") == "Tour")
                property_summary[prop_key]["visits"] += tours

                # ── Attribution computation ───────────────────────────────────
                lease_date_key = prospect["lease_date_key"]
                if lease_date_key:
                    touches, reason = compute_attribution(
                        prospect["tasks"], lease_date_key,
                        vendor_lookup, campaign_lookup,
                    )

                    if touches:
                        property_summary[prop_key]["attributed_leases"] += 1
                        created_key = prospect["created_key"] or int(
                            run_date.strftime("%Y%m%d"))

                        for touch in touches:
                            journey_rows.append({
                                "prospect_key":  prospect_key,
                                "prop_key":      prop_key,
                                "date_key":      created_key,
                                "vendor_key":    vendor_key,
                                "channel_key":   channel_key,
                                "funnel_key":    touch["funnel_stage_key"],
                                "touch_num":     touch["touch_number"],
                                "total_touches": touch["total_touches"],
                                "days_before":   touch["days_before_lease"],
                                "lease_date_key": lease_date_key,
                                "converted":     1,
                                "attr_credit":   touch["attributed_credit"],
                                "is_direct":     touch["is_direct_credit"],
                                "is_assisted":   touch["is_assisted_credit"],
                                "lease_value":   prospect["lease_value"],
                                "datasource":    DATASOURCE,
                            })
                    else:
                        # Converted but no touches in attribution window —
                        # tally by reason and log a per-record warning so
                        # the cause is recoverable from the log alone.
                        # Each bucket maps to a different upstream fix:
                        #   NO_TASKS       — task generator never created
                        #                    rows for this lead
                        #   OUT_OF_WINDOW  — task dates exist but don't
                        #                    align with LeaseStartDate__c
                        #   BAD_DATES      — task ActivityDate values are
                        #                    blank or malformed
                        unattrib_reasons[reason] = (
                            unattrib_reasons.get(reason, 0) + 1
                        )
                        log.warning(
                            "Lead %s converted on %s but no journey written: "
                            "reason=%s, tasks_attached=%d",
                            lead_id, lease_date_key, reason,
                            len(prospect.get("tasks") or []),
                        )
                else:
                    # Converted but with no lease_date_key on the opp —
                    # different bucket because the upstream fix is in the
                    # opportunities file, not the tasks file.
                    unattrib_reasons["NO_LEASE_DATE"] = (
                        unattrib_reasons.get("NO_LEASE_DATE", 0) + 1
                    )
                    log.warning(
                        "Lead %s converted but opportunity has no "
                        "LeaseStartDate__c — no journey written", lead_id,
                    )

            # ── Record outcome in silver_processed ───────────────────────────
            outcome = (OUTCOME_FLAGGED_LOADED if flag_types
                       else OUTCOME_LOADED)
            processed_recs.append({
                "run_id":          run_id,
                "run_date":        run_date,
                "pipeline_key":    PIPELINE_CRM,
                "source_object":   "Lead",
                "source_record_id": lead_id,
                "outcome":         outcome,
                "prospect_key":    prospect_key,
                "property_key":    prop_key,
                "vendor_key":      vendor_key,
                "flag_summary":    ",".join(flag_types) if flag_types else None,
                "gold_table":      "fact_leasing_daily,fact_prospect_journey",
                "duplicate_of":    None,
            })
            rows_cleansed += 1

        # ── Emit dedup flags ────────────────────────────────────────────────
        # Must run after the main loop. All participants (masters +
        # suppressed) have ProspectKeys pre-resolved in Step 7c.
        #
        # Semantics per pipeline_schema_prep section 3.4:
        #   ProspectKey    = resolved key on the flagged record itself
        #                    (i.e. the suppressed record for DEDUP_AUTO,
        #                     the pending record for DEDUP_PENDING)
        #   SuppressedKey  = the record SUPPRESSED by dedup — populated only
        #                    for DEDUP_AUTO. Same value as ProspectKey on
        #                    these rows; present as a redundant explicit
        #                    pointer for review queries.
        #   ResolvedValue  = "master_lead_id=<sfid>" so the manual queue
        #                    can pivot from suppressed → master record.
        for ev in dedup_events:
            suppressed = registry.get(ev["suppressed_lead_id"], {})
            suppressed_key = suppressed.get("prospect_key")

            flags_batch.append({
                "run_id":           run_id,
                "run_date":         run_date,
                "pipeline_key":     PIPELINE_CRM,
                "source_object":    "Lead",
                "source_record_id": ev["suppressed_lead_id"],
                "prospect_key":     suppressed_key,
                "flag_type":        ev["flag_type"],
                "flag_field":       ev["field"],
                "original_value":   ev["value"],
                "resolved_value":   f"master_lead_id={ev['master_lead_id']}",
                "suppressed_key":   (suppressed_key
                                     if ev["flag_type"] == FLAG_DEDUP_AUTO
                                     else None),
                "notes":            f"{ev['match_type']} match",
            })
            rows_flagged += 1
            if ev["flag_type"] == FLAG_DEDUP_PENDING:
                rows_dedup_pending += 1

        # ── Step 9: Flush batch writes ────────────────────────────────────────
        if flags_batch and not dry_run:
            write_flags_batch(engine, flags_batch)

        if processed_recs and not dry_run:
            write_processed_batch(engine, processed_recs)

        # ── Step 10: Write gold ───────────────────────────────────────────────
        if not dry_run:
            leasing_rows   = upsert_fact_leasing_daily(
                engine, run_date, property_summary)
            journey_count  = upsert_fact_prospect_journey(
                engine, run_date, journey_rows)
            rows_loaded    = leasing_rows + journey_count
        else:
            log.info("[DRY RUN] Would write %d leasing rows, %d journey rows",
                     len(property_summary), len(journey_rows))
            rows_loaded = 0

        # ── Step 11: ATTR_CONFLICT check ──────────────────────────────────────
        if not dry_run:
            conflicts = check_attr_conflict(engine, run_date)
            if conflicts:
                rows_attr_conflict = len(conflicts)
                for conflict in conflicts:
                    write_flag(
                        engine, run_id, run_date, PIPELINE_CRM,
                        source_object="fact_leasing_daily",
                        flag_type=FLAG_ATTR_CONFLICT,
                        flag_field="AttributedNewLeases",
                        original_value=str(conflict.get("AttributedNewLeases")),
                        resolved_value=str(conflict.get("NewLeases")),
                        notes=(f"PropertyKey={conflict.get('PropertyKey')} "
                               f"DateKey={conflict.get('DateKey')} "
                               f"delta={conflict.get('ConflictDelta')}"),
                    )
                log.error("ATTR_CONFLICT: %d rows quarantined — manual review required",
                          rows_attr_conflict)
            else:
                log.info("ATTR_CONFLICT check passed — no conflicts")

        # ── Step 12: Update watermark ─────────────────────────────────────────
        if not dry_run:
            new_watermark = datetime.datetime.combine(
                run_date, datetime.time(23, 59, 59))
            update_watermark(engine, PIPELINE_CRM, new_watermark)

        # ── Finish run log ────────────────────────────────────────────────────
        if not dry_run:
            finish_run(
                engine, run_id, PIPELINE_CRM,
                rows_extracted   = rows_extracted,
                rows_cleansed    = rows_cleansed,
                rows_loaded      = rows_loaded,
                rows_flagged     = rows_flagged,
                rows_quarantined = rows_quarantined,
                rows_attr_conflict = rows_attr_conflict,
                watermark_new    = datetime.datetime.combine(
                    run_date, datetime.time(23, 59, 59)),
            )

        # ── Summary ───────────────────────────────────────────────────────────
        log.info("")
        log.info("CRM PIPELINE COMPLETE")
        log.info("  Run date:        %s", run_date)
        log.info("  Extracted:       %d", rows_extracted)
        log.info("  Cleansed:        %d", rows_cleansed)
        log.info("  Loaded:          %d", rows_loaded)
        log.info("  Flagged:         %d", rows_flagged)
        log.info("  Quarantined:     %d", rows_quarantined)
        log.info("  Dedup suppressed: %d", rows_dedup_suppressed)
        log.info("  Dedup pending:   %d", rows_dedup_pending)
        log.info("  ATTR_CONFLICT:   %d", rows_attr_conflict)
        log.info("  Journey rows:    %d", len(journey_rows))
        log.info("  Properties:      %d", len(property_summary))
        log.info("")

        # Attribution coverage check
        total_leases = sum(s["new_leases"] for s in property_summary.values())
        attr_leases  = sum(s["attributed_leases"] for s in property_summary.values())
        if total_leases > 0:
            coverage = attr_leases / total_leases
            log.info("  Attribution coverage: %.1f%% (%d / %d leases)",
                     coverage * 100, attr_leases, total_leases)

            # Show the unattributed bucket breakdown whenever there is one,
            # not only on warnings — operators want this visible on every
            # run as a routine signal-quality check.
            unattrib_total = sum(unattrib_reasons.values())
            if unattrib_total > 0:
                log.info("  Unattributed conversions: %d", unattrib_total)
                # Print a stable order so successive runs are diff-able.
                for reason in ("NO_TASKS", "OUT_OF_WINDOW",
                               "BAD_DATES", "NO_LEASE_DATE"):
                    n = unattrib_reasons.get(reason, 0)
                    if n > 0:
                        log.info("    %-15s %d", reason + ":", n)

            if coverage < 0.60:
                # Replace the old generic "investigate dedup failures" text
                # with a concrete, reason-specific next step.
                top_reason = (max(unattrib_reasons, key=unattrib_reasons.get)
                              if unattrib_reasons else None)
                hint = {
                    "NO_TASKS":       "task generator did not emit rows for these leads",
                    "OUT_OF_WINDOW":  "task ActivityDate values do not align with LeaseStartDate__c",
                    "BAD_DATES":      "task ActivityDate values are blank or unparseable",
                    "NO_LEASE_DATE":  "opportunities are missing LeaseStartDate__c",
                }.get(top_reason, "see per-record warnings above")
                log.warning(
                    "  Attribution coverage below 60%% — top unattributed "
                    "reason is %s (%s)", top_reason or "unknown", hint,
                )
            elif coverage > 0.80:
                log.warning("  Attribution coverage above 80%% — investigate duplicate attribution")

        if engine is not None:
            engine.dispose()
            engine = None

    except Exception as e:
        log.exception("CRM pipeline failed: %s", e)
        # Only try to mark the run failed if both engine and run_id made it
        # past Step 3 — otherwise fail_run would raise its own exception
        # and mask the original error.
        if engine is not None and run_id is not None:
            try:
                fail_run(engine, run_id, str(e))
            except Exception as fail_exc:
                log.error("Could not write fail_run to pipeline_runs: %s",
                          fail_exc)
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                pass
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="NorthStar CRM Pipeline")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Run date YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate source files without writing to SQL",
    )
    args = parser.parse_args()

    if args.date:
        try:
            run_date = datetime.date.fromisoformat(args.date)
        except ValueError:
            print(f"Invalid date format: {args.date}. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        run_date = datetime.date.today() - datetime.timedelta(days=1)

    run_crm_pipeline(run_date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
