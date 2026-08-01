"""
ops_pipeline.py
===============
NorthStar MAA — Ops Pipeline (DataSource = 4)

Reads one Yardi Voyager flat-file CSV per day from bronze/ops/ and upserts
property-level operational metrics into dbo.fact_property_ops_daily as
DataSource=4 — sitting alongside DataSource=1 (canonical truth) for the same
date so the reconciliation control plane can compare them.

SOURCE FILE
    Single bronze file per run_date, located at:

        {SOURCE_PATH}/{YYYY-MM-DD}/yardi_ops_export_{YYYYMMDD}.csv

    Yardi exports start with a 5-line report preamble before the actual
    CSV header row (see mock_source_generator.build_yardi_ops_export):

        Yardi Voyager — Occupancy and Leasing Summary
        Client: NorthStar Residential Group
        Database: NORTHSTAR_PROD
        Report Run: MM/DD/YYYY HH:MM:SS AM/PM
        As Of Date: MM/DD/YYYY
        <blank>
        PropertyCode,PropertyName,PropertyID,...   ← real header

    The parser skips down to the first line whose first column is
    "PropertyCode" rather than counting fixed-offset preamble lines, so
    any future change in preamble length doesn't break ingestion.

TARGET TABLE
    dbo.fact_property_ops_daily  — DataSource = 4

PIPELINE TABLES WRITTEN
    pipeline.pipeline_runs          (start_run / finish_run / fail_run)
    pipeline.pipeline_watermarks    (update_watermark on success)

USAGE
    python ops_pipeline.py
    python ops_pipeline.py --date 2026-04-30
    python ops_pipeline.py --date 2026-04-30 --dry-run

Requires .env file with SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD.

Design notes
------------
This pipeline is intentionally simple compared to CRM. There is one source
file, no dedup, no attribution, no flag detection — Yardi is a system of
record, not a CRM full of dirty data. Validation is limited to: file shape,
property FK existence, and numeric coercion. Everything else is a passthrough
upsert.

Reconciliation against canonical (DataSource=1) is NOT done inline here.
The standalone compute_reconciliation_actions.py script handles
OCCUPANCY_VARIANCE and MISSING_PROPERTY checks the next time it runs against
this date's bronze. Keeping the pipeline focused on "land bronze cleanly into
canonical fact tables" means a failure here is a clean failure rather than a
reconciliation-detection bug masquerading as a pipeline bug.
"""

import argparse
import csv
import datetime
import logging
import sys
from pathlib import Path
from typing import Optional

from sqlalchemy import text

from pipeline_utils import (
    get_engine,
    test_connection,
    get_source_path,
    get_watermark,
    update_watermark,
    start_run,
    finish_run,
    fail_run,
    load_property_lookup,
)

# Pipeline key seeded in pipeline.pipeline_watermarks (PipelineKey=4, name='OPS').
# Imported from pipeline_utils when available so the constant lives in one place;
# falls back to hardcoded 4 if the utils module hasn't been updated yet.
try:
    from pipeline_utils import PIPELINE_OPS
except ImportError:
    PIPELINE_OPS = 4

log = logging.getLogger("ops_pipeline")

# ── Constants ─────────────────────────────────────────────────────────────────
DATASOURCE = 4

# Yardi-export columns we care about, mapped to canonical column names.
# The Yardi file has ~30 columns total — we ignore the ones not in this map
# (PropertyCode, MarketName, OccupancyPct, AvgRent_Asking, ConcessionsActive,
# etc. are useful operational context but not part of the canonical fact schema).
YARDI_TO_CANONICAL = {
    "OccupiedUnits":              "OccupiedUnits",
    "VacantUnits":                "VacantUnits",
    "AvailableUnits":             "AvailableUnits",
    "MoveIns_Today":              "MoveIns",
    "MoveOuts_Today":             "MoveOuts",
    "LeaseExpirations_Today":     "LeaseExpirations",
    "ScheduledMoveIns_Today":     "ScheduledMoveIns",
    "LeaseExpirations_Next60D":   "LeaseExpirations_Next60D",
    "ScheduledMoveIns_Next60D":   "ScheduledMoveIns_Next60D",
}

INT_COLUMNS = {
    "OccupiedUnits", "VacantUnits", "AvailableUnits",
    "MoveIns", "MoveOuts", "LeaseExpirations", "ScheduledMoveIns",
}
DECIMAL_COLUMNS = {
    "LeaseExpirations_Next60D", "ScheduledMoveIns_Next60D",
}


# ══════════════════════════════════════════════════════════════════════════════
# Source resolution
# ══════════════════════════════════════════════════════════════════════════════

def resolve_source_dir(run_date: datetime.date) -> Path:
    """
    Resolve which directory to read the Yardi export from for a given run_date.
    Mirrors the CRM/Spend convention: dated subdirectory preferred, root
    fallback for backward compatibility.
    """
    root = Path(get_source_path())
    dated_dir = root / run_date.strftime("%Y-%m-%d")
    if dated_dir.exists():
        return dated_dir
    return root


def resolve_source_file(run_date: datetime.date) -> Path:
    """Locate the single Yardi export for run_date."""
    source_dir = resolve_source_dir(run_date)
    fname = f"yardi_ops_export_{run_date.strftime('%Y%m%d')}.csv"
    path = source_dir / fname

    if not path.exists():
        raise FileNotFoundError(
            f"Missing Yardi ops export: {path}\n"
            f"Expected file produced by mock_source_generator for run_date={run_date}.\n"
            f"Run: python mock_source_generator.py --date {run_date}"
        )
    return path


# ══════════════════════════════════════════════════════════════════════════════
# Yardi parser
# ══════════════════════════════════════════════════════════════════════════════

def _coerce_int(value: str, column: str, row_num: int) -> Optional[int]:
    """Coerce a Yardi cell to int. Empty / whitespace becomes 0 (Yardi's idiom
    for "no movement today")."""
    if value is None or value.strip() == "":
        return 0
    try:
        return int(float(value))   # float() first to tolerate "5.0" style
    except ValueError:
        log.warning(
            "Row %d: non-numeric value %r in column %s — coercing to 0",
            row_num, value, column,
        )
        return 0


def _coerce_decimal(value: str, column: str, row_num: int) -> Optional[float]:
    """Coerce a Yardi cell to float. Empty becomes 0.0."""
    if value is None or value.strip() == "":
        return 0.0
    try:
        return float(value)
    except ValueError:
        log.warning(
            "Row %d: non-numeric decimal %r in column %s — coercing to 0.0",
            row_num, value, column,
        )
        return 0.0


def parse_yardi_export(path: Path,
                        run_date: datetime.date,
                        property_lookup: dict) -> list:
    """
    Parse a Yardi Voyager flat-file export into row dicts ready for upsert.

    Skips the report-header preamble by scanning for the line whose first
    column is "PropertyCode" — robust to preamble length changes.

    Filters rows whose PropertyID is not in property_lookup (active properties
    only). Logs each skipped row at WARNING level so missing-property issues
    are visible in the run log without aborting the pipeline.

    Returns: list of dicts shaped for upsert_fact_property_ops_daily().
    """
    date_key = int(run_date.strftime("%Y%m%d"))
    rows = []
    skipped_unknown_property = 0
    skipped_inactive = 0

    # Yardi exports use cp1252 (Windows-1252) encoding — the report preamble
    # contains an em-dash (0x97 in cp1252) that fails UTF-8 decode. Use
    # cp1252 with errors='replace' to tolerate any other unexpected bytes.
    with open(path, newline="", encoding="cp1252", errors="replace") as f:
        reader = csv.reader(f)
        header = None
        row_num = 0
        for raw in reader:
            row_num += 1
            if not raw:
                continue
            # Detect the real CSV header (first column == "PropertyCode")
            if header is None:
                if raw[0].strip() == "PropertyCode":
                    header = [c.strip() for c in raw]
                continue

            row = dict(zip(header, raw))

            # FK check — must map to an active property
            try:
                pk = int(row.get("PropertyID", "").strip())
            except ValueError:
                log.warning("Row %d: invalid PropertyID %r — skipping",
                            row_num, row.get("PropertyID"))
                skipped_unknown_property += 1
                continue

            if pk not in property_lookup:
                log.warning("Row %d: PropertyID=%d not in dim_property "
                            "(inactive or unknown) — skipping", row_num, pk)
                skipped_inactive += 1
                continue

            # Build the canonical row
            out = {
                "date_key": date_key,
                "prop_key": pk,
                "datasource": DATASOURCE,
            }
            for yardi_col, canonical_col in YARDI_TO_CANONICAL.items():
                raw_val = row.get(yardi_col, "")
                if canonical_col in INT_COLUMNS:
                    out[canonical_col] = _coerce_int(raw_val, yardi_col, row_num)
                else:
                    out[canonical_col] = _coerce_decimal(raw_val, yardi_col, row_num)
            rows.append(out)

    log.info("Parsed Yardi export: %d valid rows | %d skipped (unknown property) "
             "| %d skipped (inactive)",
             len(rows), skipped_unknown_property, skipped_inactive)
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# SQL upsert
# ══════════════════════════════════════════════════════════════════════════════

def upsert_fact_property_ops_daily(engine, ops_rows: list) -> int:
    """
    Upsert ops rows into dbo.fact_property_ops_daily.

    MERGE key: (DateKey, PropertyKey, DataSource).
    The DataSource component is critical — DataSource=1 rows already exist
    from the canonical pipeline. Without it in the key, this MERGE would
    overwrite canonical truth instead of writing a parallel DataSource=4 row.

    Updates all 9 metric columns on match; inserts the full row when not matched.

    batch_size=125 matches the spend pipeline's pyodbc-safe ceiling
    (12 columns × 125 rows = 1500 parameters, well under the 2100 limit).
    """
    if not ops_rows:
        return 0

    sql = text("""
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
    """)

    batch_size = 125
    loaded = 0
    with engine.begin() as conn:
        for i in range(0, len(ops_rows), batch_size):
            batch = ops_rows[i:i + batch_size]
            conn.execute(sql, batch)
            loaded += len(batch)
    return loaded


# ══════════════════════════════════════════════════════════════════════════════
# Main pipeline orchestration
# ══════════════════════════════════════════════════════════════════════════════

def run_ops_pipeline(run_date: datetime.date, dry_run: bool = False) -> None:
    """
    Execute the Ops pipeline for the given run_date.

    Steps:
      1. Connect to SQL.
      2. Load property dim lookup (FK validation).
      3. Open run row in pipeline_runs (status RUNNING).
      4. Resolve and parse the Yardi bronze file.
      5. MERGE into dbo.fact_property_ops_daily as DataSource=4.
      6. Update watermark FIRST, then finish_run — that order means a
         crash between them leaves a stale-watermark RUNNING row visible
         rather than a SUCCESS row with a stale watermark.
    """
    log.info("=" * 60)
    log.info("OPS PIPELINE — %s%s", run_date.isoformat(),
             "  [DRY RUN]" if dry_run else "")
    log.info("=" * 60)

    # ── Step 1: connect ───────────────────────────────────────────────────────
    engine = get_engine()
    test_connection(engine)

    # ── Step 2: load property lookup ──────────────────────────────────────────
    property_lookup = load_property_lookup(engine)
    log.info("Loaded property lookup: %d active properties", len(property_lookup))

    # ── Step 3: open run row ──────────────────────────────────────────────────
    if dry_run:
        run_id = None
        watermark = get_watermark(engine, PIPELINE_OPS)
        log.info("Watermark (read-only in dry-run): %s", watermark)
    else:
        watermark = get_watermark(engine, PIPELINE_OPS)
        run_id = start_run(engine, PIPELINE_OPS, run_date,
                           watermark_used=watermark)
        log.info("Started run: run_id=%s | prior watermark: %s",
                 run_id, watermark)

    try:
        # ── Step 4: parse bronze ──────────────────────────────────────────────
        source_path = resolve_source_file(run_date)
        log.info("Source file: %s", source_path)
        ops_rows = parse_yardi_export(source_path, run_date, property_lookup)

        if not ops_rows:
            log.warning("No valid rows parsed from %s — nothing to upsert",
                        source_path)

        # ── Step 5: MERGE ─────────────────────────────────────────────────────
        if dry_run:
            log.info("DRY RUN — would upsert %d rows to "
                     "dbo.fact_property_ops_daily (DataSource=%d)",
                     len(ops_rows), DATASOURCE)
            rows_loaded = len(ops_rows)
        else:
            rows_loaded = upsert_fact_property_ops_daily(engine, ops_rows)
            log.info("fact_property_ops_daily upserted: %d rows (DataSource=%d)",
                     rows_loaded, DATASOURCE)

        # ── Step 6: watermark, then finish_run ────────────────────────────────
        if not dry_run:
            new_watermark = datetime.datetime.combine(
                run_date, datetime.time(23, 59, 59))
            update_watermark(engine, PIPELINE_OPS, new_watermark)
            log.info("Watermark advanced to: %s", new_watermark)

            finish_run(
                engine,
                run_id,
                PIPELINE_OPS,
                rows_extracted=rows_loaded,
                rows_cleansed=rows_loaded,
                rows_loaded=rows_loaded,
                watermark_new=new_watermark,
            )

        # ── Summary ───────────────────────────────────────────────────────────
        log.info("=" * 60)
        log.info("OPS PIPELINE COMPLETE")
        log.info("=" * 60)
        log.info("  Run date:        %s", run_date)
        log.info("  Rows upserted:   %d", rows_loaded)
        log.info("  DataSource:      %d", DATASOURCE)
        log.info("  Mode:            %s", "DRY RUN" if dry_run else "LIVE")

    except Exception as e:
        log.exception("Ops pipeline FAILED: %s", e)
        if run_id is not None:
            try:
                fail_run(engine, run_id, str(e))
            except Exception as inner:
                log.error("Could not write fail_run to pipeline_runs: %s", inner)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    parser = argparse.ArgumentParser(description="NorthStar Ops Pipeline")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Run date YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate the Yardi export without writing to SQL",
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

    run_ops_pipeline(run_date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
