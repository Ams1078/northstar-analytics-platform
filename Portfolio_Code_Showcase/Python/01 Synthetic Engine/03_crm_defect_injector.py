"""
add_realism_to_crm.py
======================
NorthStar MAA — CRM realism layer (post-generator)

Injects "walk-in" patterns into the synthetic CRM source files so the
overall system attribution coverage matches realistic property operations
(~75-80% rather than the generator's perfect 100%).

WHAT IS A WALK-IN?
    A real-world prospect who signs a lease without ever appearing in CRM:
      - Walked into the leasing office, no web inquiry
      - Was a referral from a current resident
      - Found the property via Google Maps, drive-by, signage
    For these prospects, Yardi has the lease but CRM has zero records.

DESIGN CHOICES (locked in via planning conversation)
    Approach:    drop SF rows entirely (cleanest realism — true walk-ins
                 generate ZERO CRM artifacts, not malformed ones)
    Rate:        22% of converting leads become walk-ins
                 → for 55 converting leads, ~12 walk-ins
                 → cross-source coverage (CRM 43 vs Canonical/Ops 55) ≈ 78%
    Determinism: random.seed(YYYYMMDD-as-int) so same date always produces
                 same walk-in set (reproducible) but different dates produce
                 different sets (varied across the calendar)
    Scope:       overwrites the 6 SF files in place at mock_sources/{date}/
                 (the generator is deterministic too — re-run it to restore)
    Boundary:    only converting leads (IsConverted='True') become walk-ins
                 — non-converting leads can't affect attribution coverage
                 either way, so removing them would be no-op realism noise

WHAT THIS DOES NOT DO (Phase 2, future work)
    - Expired-window leads (lead activity outside 7-day attribution window)
    - Cross-property leads (inquired Property A, signed Property B)
    - Dirty join keys (typo'd email, mismatched phone, name variants)
    - Lease-side data (does not touch Yardi ops files — those are correct)

WHAT THE REALISM SHOWS UP AS
    The realism is NOT visible inside CRM's own metrics — CRM only counts
    what it sees, so its internal attribution stays at 100%. The realism
    appears in cross-source comparison:

        SELECT DataSource, COUNT(DISTINCT lease_id) AS leases
        FROM   dbo.fact_leasing_daily
        WHERE  DateKey = 20260430
        GROUP BY DataSource;
        -- Expected after realism:
        --   DS=1 (canonical):  55
        --   DS=2 (CRM):        ~43

    That gap (12 leases / 22%) IS the walk-in signal.

USAGE
    python add_realism_to_crm.py --date 2026-04-30
    python add_realism_to_crm.py --date 2026-04-30 --walkin-rate 0.15
    python add_realism_to_crm.py --date 2026-04-30 --dry-run
"""

import argparse
import csv
import datetime
import logging
import os
import random
import sys
from pathlib import Path

log = logging.getLogger("realism")

# Default mock_sources root — matches generator's output convention
DEFAULT_SOURCE_ROOT = "./mock_sources"

# The 6 Salesforce raw files and the column on each that links back to a Lead.
# Order is informational only; we process all files in this dict regardless.
#
#   filename pattern       → join column whose value identifies "this row
#                            belongs to lead X"
#
# IMPORTANT: sf_contacts_raw is NOT in this dict. Salesforce contacts do not
# carry a FK back to the originating Lead — the link is forward (sf_leads_raw
# has a ConvertedContactId column pointing to the Contact, not the other
# direction). When a Lead converts, the Contact's only join anchor is Email.
#
# Leaving walk-in contacts in place actually adds REALISM rather than removing
# it: real Salesforce environments routinely have orphan Contact records where
# someone deleted a Lead without cleaning up the converted Contact. CRM's
# integrity checks should flag these as ORPHAN_CONTACT — exactly the kind of
# dirty-data signal the reconciliation control plane is designed to surface.
SF_FILE_JOIN_KEYS = {
    "sf_leads_raw_{ds}.csv":             "Id",          # the lead itself
    "sf_opportunities_raw_{ds}.csv":     "LeadId__c",
    "sf_tasks_raw_{ds}.csv":             "WhoId",
    "sf_campaign_members_raw_{ds}.csv":  "LeadId",
}

# Files we read but never modify
UNTOUCHED_FILES = [
    "sf_campaigns_raw_{ds}.csv",   # campaign dimension table — not lead-keyed
    "sf_contacts_raw_{ds}.csv",    # see note above — no FK to lead, link is
                                    # via Email match; orphan contacts are
                                    # realistic dirty data, not a bug
]


# ══════════════════════════════════════════════════════════════════════════════
# Walk-in selection
# ══════════════════════════════════════════════════════════════════════════════

def pick_walkins(leads_path: Path, walkin_rate: float, seed: int) -> set:
    """
    Read sf_leads_raw and select the walk-in LeadIds deterministically.

    Returns: set of LeadId strings to delete from all 6 SF files.

    Selection rule: only converting leads (IsConverted='True') are eligible.
    Non-converting leads can't affect attribution coverage (they have no
    matching lease), so removing them produces no realism signal.
    """
    converting_leads = []
    with open(leads_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("IsConverted", "").strip().lower() == "true":
                converting_leads.append(row["Id"])

    n_total = len(converting_leads)
    n_walkins = int(round(n_total * walkin_rate))

    if n_walkins == 0:
        log.warning("Walk-in rate %.2f produces 0 walk-ins from %d converting "
                    "leads — no changes will be made", walkin_rate, n_total)
        return set()

    rng = random.Random(seed)
    walkins = set(rng.sample(converting_leads, n_walkins))

    log.info("Selected %d walk-ins from %d converting leads (%.1f%%, seed=%d)",
             n_walkins, n_total, 100.0 * n_walkins / n_total, seed)
    log.info("Walk-in LeadIds: %s", sorted(walkins)[:5] +
             (["..."] if len(walkins) > 5 else []))
    return walkins


# ══════════════════════════════════════════════════════════════════════════════
# File modification
# ══════════════════════════════════════════════════════════════════════════════

def filter_walkins_from_file(path: Path, join_col: str,
                              walkin_ids: set, dry_run: bool) -> tuple[int, int]:
    """
    Read a CSV, drop rows whose join_col value is in walkin_ids, write back.

    Returns: (rows_before, rows_after).

    On dry_run, computes counts without writing the file.
    """
    if not path.exists():
        log.warning("Skipping missing file: %s", path)
        return 0, 0

    # Read all rows
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if join_col not in fieldnames:
            log.error("Column %r not found in %s — file structure may have "
                      "changed. Skipping.", join_col, path.name)
            return 0, 0
        rows = list(reader)

    n_before = len(rows)
    surviving = [r for r in rows if r.get(join_col, "").strip() not in walkin_ids]
    n_after = len(surviving)
    n_dropped = n_before - n_after

    if dry_run:
        log.info("  [DRY RUN] %s: would drop %d of %d rows (join=%s)",
                 path.name, n_dropped, n_before, join_col)
        return n_before, n_after

    # Write back to the same path (overwrite in place per design)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(surviving)

    log.info("  %s: dropped %d of %d rows → %d remaining (join=%s)",
             path.name, n_dropped, n_before, n_after, join_col)
    return n_before, n_after


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def add_realism(run_date: datetime.date, walkin_rate: float,
                source_root: str, dry_run: bool) -> None:
    """
    Apply the walk-in realism layer to a single date's CRM source files.
    """
    log.info("=" * 60)
    log.info("CRM REALISM LAYER — %s%s",
             run_date.isoformat(), "  [DRY RUN]" if dry_run else "")
    log.info("=" * 60)

    ds_compact = run_date.strftime("%Y%m%d")
    source_dir = Path(source_root) / run_date.strftime("%Y-%m-%d")

    if not source_dir.exists():
        log.error("Source directory does not exist: %s", source_dir)
        log.error("Did you run the generator and stage files into the dated "
                  "subfolder first?")
        sys.exit(1)

    log.info("Source directory: %s", source_dir)

    # Step 1: Pick walk-ins (deterministic per date)
    leads_path = source_dir / f"sf_leads_raw_{ds_compact}.csv"
    if not leads_path.exists():
        log.error("Leads file not found: %s", leads_path)
        sys.exit(1)

    seed = int(ds_compact)   # YYYYMMDD as int — deterministic per date
    walkin_ids = pick_walkins(leads_path, walkin_rate, seed)

    if not walkin_ids:
        log.info("No walk-ins selected — exiting without modifying files")
        return

    # Step 2: Drop walk-in rows from each of the 5 lead-keyed files
    log.info("")
    log.info("Modifying SF files:")
    total_dropped = 0
    for file_pattern, join_col in SF_FILE_JOIN_KEYS.items():
        path = source_dir / file_pattern.format(ds=ds_compact)
        n_before, n_after = filter_walkins_from_file(
            path, join_col, walkin_ids, dry_run
        )
        total_dropped += (n_before - n_after)

    # Untouched files: log for transparency
    log.info("")
    log.info("Untouched files (not lead-keyed):")
    for file_pattern in UNTOUCHED_FILES:
        path = source_dir / file_pattern.format(ds=ds_compact)
        if path.exists():
            log.info("  %s: unchanged", path.name)

    # Summary
    log.info("")
    log.info("=" * 60)
    log.info("REALISM LAYER COMPLETE")
    log.info("=" * 60)
    log.info("  Run date:        %s", run_date)
    log.info("  Walk-in rate:    %.1f%%", walkin_rate * 100)
    log.info("  Walk-in count:   %d converting leads", len(walkin_ids))
    log.info("  Total rows dropped (across 5 SF files): %d", total_dropped)
    log.info("  Mode:            %s", "DRY RUN" if dry_run else "LIVE")
    log.info("")
    log.info("Expected effect on cross-source attribution:")
    log.info("  CRM (DS=2) lease count will be lower than Canonical (DS=1)")
    log.info("  by approximately %d leases when CRM pipeline next runs.",
             len(walkin_ids))
    log.info("  CRM's internal attribution coverage will still report 100%%")
    log.info("  because CRM only sees its own (post-realism) leads.")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Inject walk-in realism into synthetic CRM source files."
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Run date YYYY-MM-DD (the dated subfolder under mock_sources/)",
    )
    parser.add_argument(
        "--walkin-rate",
        type=float,
        default=0.22,
        help="Fraction of converting leads to walk-in (default: 0.22 → ~78%% "
             "cross-source attribution coverage)",
    )
    parser.add_argument(
        "--source-root",
        type=str,
        default=os.environ.get("SOURCE_PATH", DEFAULT_SOURCE_ROOT),
        help="Mock sources root directory (default: ./mock_sources or "
             "$SOURCE_PATH)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing the modified files",
    )
    args = parser.parse_args()

    # Validate inputs
    if not 0.0 <= args.walkin_rate <= 0.5:
        print(f"Invalid walk-in rate: {args.walkin_rate}. "
              f"Must be between 0.0 and 0.5 (50%%).")
        sys.exit(1)

    try:
        run_date = datetime.date.fromisoformat(args.date)
    except ValueError:
        print(f"Invalid date format: {args.date}. Use YYYY-MM-DD.")
        sys.exit(1)

    add_realism(run_date, args.walkin_rate, args.source_root, args.dry_run)


if __name__ == "__main__":
    main()
