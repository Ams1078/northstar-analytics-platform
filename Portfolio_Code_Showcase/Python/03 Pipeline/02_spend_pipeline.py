"""
spend_pipeline.py
Spend pipeline orchestrator (DataSource = 3).

Phase 1 (done): dry-run shell that calls the Google Ads parser and prints
what would be written. No SQL writes.

Phase 2 (this revision): live SQL writes to fact_marketing_spend_daily.
Adds run logging via pipeline_runs and watermark updates via
pipeline_watermarks. Funnel writes deferred to a future phase.

Phase 3+ (later sessions): add Bing, Zillow, Apartments.com, Apartment List,
Meta, Display DSP parsers — each registered in PARSER_REGISTRY below.
Add fact_marketing_funnel_daily writes when at least one parser produces
funnel rows.

USAGE:
    python spend_pipeline.py --date 2026-04-22 --dry-run    # parse only
    python spend_pipeline.py --date 2026-04-22              # live write

See SPEND_PIPELINE_DESIGN.md for full architecture.
"""

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

from sqlalchemy import text

from parsers.google_ads import parse as parse_google_ads
from pipeline_utils import (
    get_engine,
    test_connection,
    get_watermark,
    start_run,
    update_watermark,
    finish_run,
    fail_run,
    write_quarantine,
    write_flags_batch,
    clear_audit_for_rerun,
    detect_spend_conflicts,
    FLAG_DIRTY_KEY,
    FLAG_SPEND_CONFLICT,
    QUARANTINE_PARSE_FAILED,
)


# ══════════════════════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════════════════════

PIPELINE_KEY  = 3        # matches pipeline_watermarks seed row
PIPELINE_NAME = "SPEND"
DATASOURCE    = 3        # stamped on every row written to gold tables


# Parser registry — one entry per source file we know how to parse.
# Adding a new parser is a single entry here plus the parser module itself.
# The shell loops over this registry; no per-parser orchestration code.
#
# Each entry maps an internal name to:
#   filename_template — string with {date_key} placeholder
#   parse_fn          — callable matching parse(path) -> (spend_rows, funnel_rows)
PARSER_REGISTRY = {
    "google_ads": {
        "filename_template": "google_ads_export_{date_key}.csv",
        "parse_fn":          parse_google_ads,
    },
    # When ready, future parsers will be added here. Each follows the
    # same contract: parse(path) returns (list_of_spend_rows, list_of_funnel_rows)
    # in the canonical 4-field shape (DateKey, PropertyKey, VendorKey, Spend)
    # for spend rows, and 5-field shape for funnel rows.
    #
    # "bing_ads":      {"filename_template": "bing_ads_export_{date_key}.csv",   "parse_fn": parse_bing_ads},
    # "zillow":        {"filename_template": "zillow_leads_{date_key}.csv",      "parse_fn": parse_zillow},
    # "apartments_com": {"filename_template": "apartments_com_{date_key}.csv",    "parse_fn": parse_apartments_com},
    # "apartment_list": {"filename_template": "apartment_list_{date_key}.csv",    "parse_fn": parse_apartment_list},
    # "meta_ads":      {"filename_template": "meta_ads_export_{date_key}.csv",   "parse_fn": parse_meta_ads},
    # "display_dsp":   {"filename_template": "display_dsp_{date_key}.csv",       "parse_fn": parse_display_dsp},
}


# ══════════════════════════════════════════════════════════════════════════════
# Logging
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Source directory resolution
# ══════════════════════════════════════════════════════════════════════════════

def resolve_source_dir(run_date: date) -> Path:
    """
    Find which directory holds the source files for this run date.
    Same pattern as crm_pipeline: prefer dated subfolder, fall back to
    flat layout for backward compatibility with old test fixtures.
    """
    source_root = Path(os.environ.get("SOURCE_PATH", "./mock_sources"))
    dated_dir   = source_root / run_date.isoformat()

    if dated_dir.is_dir():
        log.info("Source layout: dated folder → %s", dated_dir)
        return dated_dir

    if source_root.is_dir():
        log.warning(
            "Source layout: flat fallback → %s "
            "(dated folder %s not found)", source_root, dated_dir,
        )
        return source_root

    raise FileNotFoundError(
        f"No source directory found. Looked for:\n"
        f"  {dated_dir}  (preferred)\n"
        f"  {source_root}  (legacy flat layout)\n"
        f"Set SOURCE_PATH env var or generate sources for {run_date}."
    )


# ══════════════════════════════════════════════════════════════════════════════
# Row validation (Step 6 of 12-step framework)
# ══════════════════════════════════════════════════════════════════════════════

def validate_spend_rows(spend_rows: list,
                          property_keys: set,
                          vendor_keys: set,
                          parser_name: str,
                          run_id: int,
                          run_date,
                          pipeline_key: int) -> tuple:
    """
    Split spend rows into (good_rows, flag_dicts, quarantine_records).

    Good rows pass all validation checks and proceed to gold-table upsert.
    Bad rows are routed to silver_quarantine; for each bad row we also emit
    a flag describing why it was quarantined.

    Validation checks (Day 1 scope):
      1. PropertyKey must exist in dim_property
      2. VendorKey must exist in dim_vendor

    Out of scope for Day 1 (deferred):
      - Negative spend (defensive code; current sources can't produce this)
      - Duplicate aggregate detection (parser already aggregates; MERGE handles)
      - Numeric type validation (Python types prevent the bad case)

    Returns:
      good_rows: list of dicts ready for gold-table upsert
      flag_dicts: list of dicts for write_flags_batch
      quarantine_records: list of (raw_data, source_record_id, reason) tuples
                          for write_quarantine (called per-record because
                          there's no batch helper today)
    """
    good_rows: list = []
    flag_dicts: list = []
    quarantine_records: list = []

    source_object = f"{parser_name}_export"  # e.g. "google_ads_export"

    for row in spend_rows:
        date_key = row.get("DateKey")
        prop_key = row.get("PropertyKey")
        vendor_key = row.get("VendorKey")

        # Stable record identifier — Spend rows don't have natural IDs because
        # they're aggregates. Construct one from the composite key so
        # SourceRecordId can be queried later for traceability back to the
        # offending record. Format: "{date_key}-P{prop}-V{vendor}".
        source_record_id = f"{date_key}-P{prop_key}-V{vendor_key}"

        # Check 1: PropertyKey exists in dim_property
        if prop_key not in property_keys:
            quarantine_records.append({
                "raw_data": dict(row),
                "source_record_id": source_record_id,
                "reason": FLAG_DIRTY_KEY,
                "notes": f"PropertyKey {prop_key} not in dim_property",
            })
            flag_dicts.append({
                "run_id": run_id,
                "run_date": run_date,
                "pipeline_key": pipeline_key,
                "source_object": source_object,
                "source_record_id": source_record_id,
                "flag_type": FLAG_DIRTY_KEY,
                "flag_field": "PropertyKey",
                "original_value": str(prop_key),
                "notes": f"PropertyKey {prop_key} not in dim_property",
            })
            continue  # don't also flag for VendorKey — first failure stops

        # Check 2: VendorKey exists in dim_vendor
        if vendor_key not in vendor_keys:
            quarantine_records.append({
                "raw_data": dict(row),
                "source_record_id": source_record_id,
                "reason": FLAG_DIRTY_KEY,
                "notes": f"VendorKey {vendor_key} not in dim_vendor",
            })
            flag_dicts.append({
                "run_id": run_id,
                "run_date": run_date,
                "pipeline_key": pipeline_key,
                "source_object": source_object,
                "source_record_id": source_record_id,
                "flag_type": FLAG_DIRTY_KEY,
                "flag_field": "VendorKey",
                "original_value": str(vendor_key),
                "notes": f"VendorKey {vendor_key} not in dim_vendor",
            })
            continue

        good_rows.append(row)

    return good_rows, flag_dicts, quarantine_records


def load_dim_keys(engine) -> tuple:
    """
    Load PropertyKey and VendorKey sets from dim tables.
    Used by validate_spend_rows to check FK existence without round-tripping
    to Azure for every row.
    """
    with engine.connect() as conn:
        prop_rows = conn.execute(
            text("SELECT PropertyKey FROM dbo.dim_property WHERE IsActive = 1")
        ).fetchall()
        vendor_rows = conn.execute(
            text("SELECT VendorKey FROM dbo.dim_vendor")
        ).fetchall()

    property_keys = {r[0] for r in prop_rows}
    vendor_keys = {r[0] for r in vendor_rows}
    return property_keys, vendor_keys


# ══════════════════════════════════════════════════════════════════════════════
# Gold-table upsert
# ══════════════════════════════════════════════════════════════════════════════

def upsert_fact_marketing_spend_daily(engine, spend_rows: list,
                                        batch_size: int = 125) -> int:
    """
    Upsert spend rows into dbo.fact_marketing_spend_daily.

    MERGE key: (DateKey, PropertyKey, VendorKey, DataSource).
    The DataSource component is critical — DataSource=1 rows already exist
    from the canonical pipeline. Without it in the key, this MERGE would
    overwrite canonical truth instead of writing a parallel DataSource=3
    row alongside it.

    Spend is the only updateable column on match. All others are part of
    the key.

    batch_size = 125 keeps us safely under pyodbc's 2,100-parameter ceiling:
    5 columns × 125 rows = 625 parameters per executemany batch.
    """
    if not spend_rows:
        return 0

    sql = text("""
        MERGE dbo.fact_marketing_spend_daily AS tgt
        USING (VALUES (
            :DateKey, :PropertyKey, :VendorKey, :Spend, :DataSource
        )) AS src (
            DateKey, PropertyKey, VendorKey, Spend, DataSource
        )
        ON  tgt.DateKey     = src.DateKey
        AND tgt.PropertyKey = src.PropertyKey
        AND tgt.VendorKey   = src.VendorKey
        AND tgt.DataSource  = src.DataSource
        WHEN MATCHED THEN
            UPDATE SET Spend = src.Spend
        WHEN NOT MATCHED THEN
            INSERT (DateKey, PropertyKey, VendorKey, Spend, DataSource)
            VALUES (
                src.DateKey, src.PropertyKey, src.VendorKey,
                src.Spend, src.DataSource
            );
    """)

    loaded = 0
    with engine.begin() as conn:
        for i in range(0, len(spend_rows), batch_size):
            batch = spend_rows[i:i + batch_size]
            conn.execute(sql, batch)
            loaded += len(batch)
    return loaded


# ══════════════════════════════════════════════════════════════════════════════
# Main pipeline orchestration
# ══════════════════════════════════════════════════════════════════════════════

def run_spend_pipeline(run_date: date, dry_run: bool = False) -> None:
    """
    Phase 1: parse all registered sources and report counts.
    Phase 2 will add SQL writes to gold tables and run logging.
    """
    date_key   = run_date.strftime("%Y%m%d")
    source_dir = resolve_source_dir(run_date)

    log.info("=" * 60)
    log.info("SPEND PIPELINE — %s%s", run_date.isoformat(),
             "  [DRY RUN]" if dry_run else "")
    log.info("=" * 60)
    log.info("Source directory: %s", source_dir)
    log.info("Registered parsers: %s", list(PARSER_REGISTRY.keys()))

    # ── Run all registered parsers ───────────────────────────────────────────
    total_spend_rows  = 0
    total_funnel_rows = 0
    parser_results: dict = {}

    # Collected once during parsing, used by both the dry-run summary and
    # the live-write path. Avoids parsing every source file twice.
    all_spend_rows:  list = []
    all_funnel_rows: list = []

    # Parsers that raised an exception. Each entry is a dict with file + error.
    # Used in the live-write path to write a single quarantine row per failure
    # and to flip the run status to PARTIAL.
    failed_parsers: list = []

    for parser_name, parser_spec in PARSER_REGISTRY.items():
        filename = parser_spec["filename_template"].format(date_key=date_key)
        file_path = source_dir / filename

        if not file_path.exists():
            log.warning("Skipping %s — source file not found: %s",
                        parser_name, file_path)
            parser_results[parser_name] = {"status": "MISSING_FILE"}
            continue

        try:
            log.info("Parsing %s from %s ...", parser_name, file_path.name)
            spend_rows, funnel_rows = parser_spec["parse_fn"](file_path)

            # Stamp DataSource on every row before they reach gold.
            # The parser is data-source-agnostic by design (any pipeline
            # could call it). Adding DataSource here means the parser
            # contract stays simple and the pipeline owns the stamp.
            for row in spend_rows:
                row["DataSource"] = DATASOURCE
            for row in funnel_rows:
                row["DataSource"] = DATASOURCE

            total_spend_rows  += len(spend_rows)
            total_funnel_rows += len(funnel_rows)

            # Save for downstream — used by both dry-run summary and live write
            all_spend_rows.extend(spend_rows)
            all_funnel_rows.extend(funnel_rows)

            parser_results[parser_name] = {
                "status":      "OK",
                "spend_rows":  len(spend_rows),
                "funnel_rows": len(funnel_rows),
                # Cache the actual row lists per parser so the validation
                # phase doesn't have to re-parse the file. This matters for
                # parsers that may become non-deterministic later (timestamps,
                # API calls, retries). Parsing a file should happen exactly
                # once per pipeline run.
                "spend_rows_data":  spend_rows,
                "funnel_rows_data": funnel_rows,
            }
            log.info(
                "  %s: %d spend rows, %d funnel rows",
                parser_name, len(spend_rows), len(funnel_rows),
            )

        except Exception as e:
            log.error("  %s FAILED: %s", parser_name, e)
            parser_results[parser_name] = {
                "status": "PARSE_FAILED",
                "error":  str(e),
            }
            failed_parsers.append({
                "parser_name": parser_name,
                "filename":    filename,
                "file_path":   str(file_path),
                "error":       str(e),
            })

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("")
    log.info("SPEND PIPELINE COMPLETE")
    log.info("  Run date:      %s", run_date)
    log.info("  Parsers run:   %d", len(parser_results))
    log.info("  Spend rows:    %d", total_spend_rows)
    log.info("  Funnel rows:   %d", total_funnel_rows)
    log.info("")
    log.info("Per-parser breakdown:")
    for parser_name, result in parser_results.items():
        log.info("  %-20s %s", parser_name, result)

    if dry_run:
        log.info("")
        log.info("[DRY RUN] No SQL writes performed.")
        log.info("[DRY RUN] Would write %d rows to fact_marketing_spend_daily "
                 "(DataSource=%d)", total_spend_rows, DATASOURCE)
        log.info("[DRY RUN] Would write %d rows to fact_marketing_funnel_daily "
                 "(DataSource=%d)", total_funnel_rows, DATASOURCE)
        return

    # ── LIVE WRITE PATH ──────────────────────────────────────────────────────
    # Reuses the CRM run-log + watermark pattern. Order matters:
    #   1. Connect and verify
    #   2. Read watermark (for run-log audit; not used to filter rows
    #      since this pipeline is full-load per run_date)
    #   3. Open a run row in pipeline_runs (status RUNNING)
    #   4. Load dim keys for FK validation (one-time, in-memory)
    #   5. Validate rows: split into good_rows + flagged/quarantined
    #   6. Write quarantine + flag rows BEFORE the upsert so the audit trail
    #      is in place even if the upsert fails halfway through
    #   7. Upsert good rows to gold
    #   8. Update watermark FIRST, then finish_run — that order means a
    #      crash between them leaves the run as RUNNING (visibly stuck)
    #      rather than as SUCCESS with a stale watermark.
    log.info("")
    log.info("Connecting to Azure SQL...")
    engine = get_engine()
    test_connection(engine)
    watermark = get_watermark(engine, PIPELINE_KEY)
    run_id = start_run(
        engine, PIPELINE_KEY, run_date, watermark_used=watermark,
    )
    log.info("[%s] Run started → RunId=%d", PIPELINE_NAME, run_id)

    try:
        # ── Idempotency: clear previous run's flags + quarantine ─────────────
        # If this date has been run before (manual re-trigger, retry, etc.),
        # remove the old audit rows so the new run produces a clean trail.
        # Gold tables are already idempotent via MERGE; this brings audit
        # tables to the same semantics. Earlier runs' RunId rows in
        # pipeline_runs are NOT touched — that table stays append-only on
        # purpose so re-run history is preserved.
        clear_audit_for_rerun(engine, PIPELINE_KEY, run_date)

        # ── Step 6: Validate rows against dim-table FKs ──────────────────────
        property_keys, vendor_keys = load_dim_keys(engine)
        log.info(
            "Loaded dim keys: %d properties, %d vendors",
            len(property_keys), len(vendor_keys),
        )

        # The parser registry produced rows already grouped by parser name
        # in parser_results. We need to validate per-parser so flags can
        # record the right source_object. Iterate parser_results to find
        # which parser each row came from.
        all_good_rows: list = []
        all_flag_dicts: list = []
        all_quarantine_records: list = []

        # Validate per-parser using the cached row lists from initial parse.
        # No re-parse — parsing a file happens exactly once per pipeline run.
        for parser_name, parser_spec in PARSER_REGISTRY.items():
            result = parser_results.get(parser_name, {})
            if result.get("status") != "OK":
                continue

            spend_rows = result["spend_rows_data"]

            good, flags, quarantines = validate_spend_rows(
                spend_rows,
                property_keys,
                vendor_keys,
                parser_name=parser_name,
                run_id=run_id,
                run_date=run_date,
                pipeline_key=PIPELINE_KEY,
            )
            all_good_rows.extend(good)
            all_flag_dicts.extend(flags)
            all_quarantine_records.extend(quarantines)

        log.info(
            "Validation: %d good rows, %d quarantined rows, %d flags",
            len(all_good_rows),
            len(all_quarantine_records),
            len(all_flag_dicts),
        )

        # ── Write quarantine records FIRST ───────────────────────────────────
        # silver_quarantine has no batch helper today; CRM also writes
        # per-record. Volume is low (a handful of bad rows in production)
        # so this is acceptable. SourceObject is just "spend" since we don't
        # track per-parser at quarantine level (the SourceRecordId encodes
        # date_key and the keys, which is enough for traceability).

        # First: any parser that raised an exception gets ONE quarantine row
        # at the file level (not per-row, since we never got to the rows).
        # No flag is emitted because pipeline_flags is for row-level data
        # quality issues; a parser failure is a file-level event.
        #
        # SourceRecordId is varchar(20) max, so we use a compact identifier
        # rather than the full filename. The full filename + path lives in
        # raw_data + notes where there's no length limit.
        for fp in failed_parsers:
            short_id = f"PF-{fp['parser_name'][:12]}-{date_key}"[:18]
            write_quarantine(
                engine, run_id, run_date, PIPELINE_KEY,
                source_object=fp["parser_name"],
                quarantine_reason=QUARANTINE_PARSE_FAILED,
                raw_data={
                    "parser":   fp["parser_name"],
                    "filename": fp["filename"],
                    "path":     fp["file_path"],
                    "error":    fp["error"],
                },
                source_record_id=short_id,
                notes=(
                    f"Parser {fp['parser_name']} raised on file "
                    f"{fp['filename']}: {fp['error'][:200]}"
                ),
            )
        if failed_parsers:
            log.info("silver_quarantine: wrote %d parser-failure records",
                     len(failed_parsers))

        # Then: per-row quarantines from validation
        for q in all_quarantine_records:
            write_quarantine(
                engine, run_id, run_date, PIPELINE_KEY,
                source_object="spend",
                quarantine_reason=q["reason"],
                raw_data=q["raw_data"],
                source_record_id=q["source_record_id"],
                notes=q["notes"],
            )
        if all_quarantine_records:
            log.info("silver_quarantine: wrote %d row-level records",
                     len(all_quarantine_records))

        # ── Write flags batch ────────────────────────────────────────────────
        write_flags_batch(engine, all_flag_dicts)
        if all_flag_dicts:
            log.info("pipeline_flags: wrote %d flags", len(all_flag_dicts))

        # ── Upsert good rows to gold ─────────────────────────────────────────
        rows_loaded = upsert_fact_marketing_spend_daily(engine, all_good_rows)
        log.info("fact_marketing_spend_daily upserted: %d rows (DataSource=%d)",
                 rows_loaded, DATASOURCE)

        update_watermark(engine, PIPELINE_KEY, run_date)

        # ── Day 2: SPEND_CONFLICT detection vs canonical (DataSource=1) ──────
        # After upsert + watermark, compare what we wrote at DataSource=3 to
        # canonical truth at DataSource=1 for the same date. Any (Property,
        # Vendor) tuple where Spend differs by more than $0.01 gets a
        # SPEND_CONFLICT flag in pipeline_flags.
        #
        # This is run AFTER update_watermark because the watermark says "we
        # successfully wrote this date." Conflict detection is observation,
        # not a retry trigger — even if 100% of rows conflict, the upsert
        # was still correct (rows landed in gold) and the watermark should
        # advance.
        #
        # If canonical hasn't run yet for this date, conflict detection
        # returns an empty list with a warning. That's fine.
        conflict_flags = detect_spend_conflicts(
            engine,
            run_date=run_date,
            run_id=run_id,
            pipeline_key=PIPELINE_KEY,
            source_datasource=DATASOURCE,
            tolerance=0.01,
        )
        if conflict_flags:
            write_flags_batch(engine, conflict_flags)
            log.info("pipeline_flags: wrote %d SPEND_CONFLICT flags",
                     len(conflict_flags))

        # If any parser failed, the run is PARTIAL not SUCCESS — some sources
        # didn't make it to gold even though the rest of the pipeline worked.
        # The quarantine count includes both row-level rejects and the file-
        # level parser failures, so the run-log captures the full picture.
        run_status = "PARTIAL" if failed_parsers else "SUCCESS"
        total_quarantined = len(all_quarantine_records) + len(failed_parsers)
        total_flagged = len(all_flag_dicts) + len(conflict_flags)

        finish_run(
            engine, run_id, PIPELINE_KEY,
            rows_extracted=len(all_spend_rows),
            rows_cleansed=len(all_good_rows),
            rows_loaded=rows_loaded,
            rows_flagged=total_flagged,
            rows_quarantined=total_quarantined,
            rows_attr_conflict=len(conflict_flags),
            status=run_status,
        )

        log.info("")
        log.info(
            "%s — loaded %d spend rows to DataSource=%d "
            "(quarantined %d rows + %d parser failures, "
            "flagged %d dirty + %d spend-conflict)",
            run_status,
            rows_loaded, DATASOURCE,
            len(all_quarantine_records), len(failed_parsers),
            len(all_flag_dicts), len(conflict_flags),
        )

    except Exception as e:
        log.exception("Live write failed: %s", e)
        fail_run(engine, run_id, str(e))
        raise

    finally:
        engine.dispose()


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="NorthStar Spend Pipeline")
    parser.add_argument("--date", required=True, help="Run date YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse only, no SQL writes")
    args = parser.parse_args()

    try:
        run_date = date.fromisoformat(args.date)
    except ValueError:
        log.error("Invalid date format: %r (expected YYYY-MM-DD)", args.date)
        sys.exit(2)

    try:
        run_spend_pipeline(run_date, dry_run=args.dry_run)
    except FileNotFoundError as e:
        log.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
