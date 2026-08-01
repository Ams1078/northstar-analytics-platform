"""
pipeline_utils.py
=================
NorthStar MAA — Shared Pipeline Infrastructure

Imported by all three pipelines:
    from pipeline_utils import get_engine, get_watermark, start_run, finish_run,
                               write_flag, write_quarantine, write_processed, fail_run

USAGE:
    All configuration is read from environment variables (or a .env file).
    Required env vars:
        SQL_SERVER    — Azure SQL server hostname
        SQL_DATABASE  — Database name (maa_marketing_analytics)
        SQL_USERNAME  — SQL login username
        SQL_PASSWORD  — SQL login password

    Optional:
        SQL_DRIVER    — ODBC driver name (default: ODBC Driver 18 for SQL Server)
        SOURCE_PATH   — Root folder for source files (default: ./mock_sources)

PIPELINE KEYS (match DataSource values in fact tables):
    2 = CRM pipeline
    3 = SPEND pipeline
    4 = OPS pipeline
"""

import os
import hashlib
import datetime
import logging
from typing import Optional
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# ── Load .env if present ──────────────────────────────────────────────────────
load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")

# ── Pipeline key constants ────────────────────────────────────────────────────
PIPELINE_CRM   = 2
PIPELINE_SPEND = 3
PIPELINE_OPS   = 4

PIPELINE_NAMES = {
    PIPELINE_CRM:   "CRM",
    PIPELINE_SPEND: "SPEND",
    PIPELINE_OPS:   "OPS",
}


def get_source_path() -> str:
    """
    Return the root folder for pipeline source files.
    Reads SOURCE_PATH env var — defaults to ./mock_sources.
    Each pipeline appends its own subfolder or file pattern.
    """
    return os.environ.get("SOURCE_PATH", "./mock_sources")

# ── FlagType controlled vocabulary (mirrors CHECK constraint in pipeline_flags) ─
FLAG_DIRTY_EMAIL   = "DIRTY_EMAIL"
FLAG_DIRTY_PHONE   = "DIRTY_PHONE"
FLAG_DIRTY_NAME    = "DIRTY_NAME"
FLAG_DIRTY_DATE    = "DIRTY_DATE"
FLAG_DIRTY_NULL    = "DIRTY_NULL"
FLAG_DIRTY_KEY     = "DIRTY_KEY"
FLAG_DEDUP_AUTO    = "DEDUP_AUTO"
FLAG_DEDUP_PENDING = "DEDUP_PENDING"
FLAG_ATTR_ORGANIC  = "ATTR_ORGANIC"
FLAG_ATTR_CONFLICT = "ATTR_CONFLICT"
FLAG_SPEND_CONFLICT = "SPEND_CONFLICT"   # parser-vs-canonical spend divergence

# ── Outcome constants for silver_processed ────────────────────────────────────
OUTCOME_LOADED           = "LOADED"
OUTCOME_FLAGGED_LOADED   = "FLAGGED_LOADED"
OUTCOME_QUARANTINED      = "QUARANTINED"
OUTCOME_DEDUP_SUPPRESSED = "DEDUP_SUPPRESSED"
OUTCOME_ATTR_CONFLICT    = "ATTR_CONFLICT"

# ── Quarantine reason constants ───────────────────────────────────────────────
# These are NOT in pipeline_flags (they're not data-quality flags); they go in
# silver_quarantine.QuarantineReason. The schema's QuarantineReason column has
# no DB-level CHECK constraint, but using documented constants keeps the
# vocabulary stable across pipelines.
#
# For per-row quarantine (e.g. invalid PropertyKey), use the matching FLAG_*
# constant as the QuarantineReason — the pipeline writes a flag AND a quarantine
# row in those cases. PARSE_FAILED is different: it has no per-row flag because
# the failure happens at the file level, not the row level.
QUARANTINE_PARSE_FAILED  = "PARSE_FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════════════════════════

def get_engine():
    """
    Build and return a SQLAlchemy engine from environment variables.
    Uses NullPool so connections are not cached between pipeline steps.
    """
    server   = os.environ["SQL_SERVER"]
    database = os.environ["SQL_DATABASE"]
    username = os.environ["SQL_USERNAME"]
    password = os.environ["SQL_PASSWORD"]
    driver   = os.environ.get("SQL_DRIVER", "ODBC Driver 18 for SQL Server")

    odbc = (
    f"DRIVER={{{driver}}};"
    f"SERVER=tcp:{server},1433;"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=300;"
)
    conn_str = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}"

    engine = create_engine(
        conn_str,
        fast_executemany=True,
        future=True,
        pool_pre_ping=True,
        poolclass=NullPool,
    )
    log.info("Engine created → %s / %s", server, database)
    return engine


# ─── Retry-with-backoff for Azure SQL Serverless wakeup ──────────────────────
#
# Azure SQL Serverless auto-pauses after 1 hour of inactivity. The first
# connection after pause has to wake the database, which can take up to
# ~90 seconds. Despite our Connection Timeout=300, certain wakeup states
# return fast errors (Login timeout expired (0)) instead of slow timeouts —
# so the only reliable defense is RETRY, not longer timeouts.
#
# This helper wraps the engine.connect() handshake with exponential backoff
# on OperationalError. It does NOT retry other exception types — only the
# specific transient-network/transient-auth class returned by Azure SQL
# during paused-database wakeup.
#
# Total worst-case wait: 5 + 15 + 30 + 60 = 110 seconds before giving up.
# In practice most wakeups complete on attempt 2 (within 5s of first failure).

_RETRY_BACKOFF_SECONDS = [5, 15, 30, 60]  # 4 attempts total


def connect_with_retry(engine, max_attempts: int = 4):
    """
    Open a SQLAlchemy connection with retry-on-OperationalError.

    Returns an open connection. CALLER is responsible for closing it
    (use as `with connect_with_retry(engine) as conn:` for auto-close).

    Raises the final OperationalError if all attempts fail.
    """
    from sqlalchemy.exc import OperationalError
    import time as _time

    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            conn = engine.connect()
            if attempt > 1:
                log.info("[CONN] Connected on attempt %d", attempt)
            return conn
        except OperationalError as exc:
            last_exc = exc
            if attempt >= max_attempts:
                log.error("[CONN] Final attempt %d failed: %s", attempt, exc)
                raise
            backoff = _RETRY_BACKOFF_SECONDS[attempt - 1]
            log.warning("[CONN] Attempt %d/%d failed (%s) — retrying in %ds",
                        attempt, max_attempts, type(exc).__name__, backoff)
            _time.sleep(backoff)
    # Should be unreachable but keeps the type checker happy
    raise last_exc  # type: ignore[misc]


def test_connection(engine) -> None:
    """Verify connectivity and log the connected database name. Uses retry."""
    with connect_with_retry(engine) as conn:
        db_name = conn.execute(text("SELECT DB_NAME()")).scalar_one()
    log.info("Connected to database: %s", db_name)


# ══════════════════════════════════════════════════════════════════════════════
# WATERMARKS
# ══════════════════════════════════════════════════════════════════════════════

def get_watermark(engine, pipeline_key: int) -> Optional[datetime.datetime]:
    """
    Read the last successful run timestamp for a pipeline.
    Returns None if this is the first run (NEVER_RUN state).
    """
    sql = """
        SELECT LastSuccessfulRun
        FROM   pipeline.pipeline_watermarks
        WHERE  PipelineKey = :pk
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql), {"pk": pipeline_key}).fetchone()

    if row is None:
        raise ValueError(f"No watermark row found for PipelineKey={pipeline_key}. "
                         "Ensure pipeline_schema_prep.sql was run.")

    watermark = row[0]
    if watermark is None:
        log.info("[%s] Watermark: NEVER_RUN — full load mode",
                 PIPELINE_NAMES[pipeline_key])
    else:
        log.info("[%s] Watermark: %s", PIPELINE_NAMES[pipeline_key], watermark)
    return watermark


def update_watermark(engine, pipeline_key: int,
                     new_watermark: datetime.datetime) -> None:
    """
    Update the watermark after a successful pipeline completion.
    Only called after all steps succeed — never on partial runs.
    """
    sql = """
        UPDATE pipeline.pipeline_watermarks
        SET    LastSuccessfulRun     = :wm,
               LastSuccessfulDateKey = :dk,
               LastRunStatus        = 'SUCCESS',
               UpdatedAt            = SYSDATETIME()
        WHERE  PipelineKey = :pk
    """
    date_key = int(new_watermark.strftime("%Y%m%d"))
    with engine.begin() as conn:
        conn.execute(text(sql), {"wm": new_watermark, "dk": date_key, "pk": pipeline_key})

    log.info("[%s] Watermark updated → %s",
             PIPELINE_NAMES[pipeline_key], new_watermark)


# ══════════════════════════════════════════════════════════════════════════════
# RUN LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def start_run(engine, pipeline_key: int, run_date: datetime.date,
              watermark_used: Optional[datetime.datetime] = None) -> int:
    """
    Insert a new pipeline_runs row with status=RUNNING.
    Returns the RunId for use in all subsequent log writes.
    """
    sql = """
        INSERT INTO pipeline.pipeline_runs
            (PipelineKey, PipelineName, RunDate, RunStartTime, Status, WatermarkUsed)
        OUTPUT INSERTED.RunId
        VALUES
            (:pk, :pname, :rd, SYSDATETIME(), 'RUNNING', :wm)
    """
    with engine.begin() as conn:
        run_id = conn.execute(text(sql), {
            "pk":    pipeline_key,
            "pname": PIPELINE_NAMES[pipeline_key],
            "rd":    run_date,
            "wm":    watermark_used,
        }).scalar_one()

    log.info("[%s] Run started → RunId=%d", PIPELINE_NAMES[pipeline_key], run_id)
    return run_id


def finish_run(engine, run_id: int, pipeline_key: int,
               rows_extracted: int = 0,
               rows_cleansed: int = 0,
               rows_loaded: int = 0,
               rows_flagged: int = 0,
               rows_quarantined: int = 0,
               rows_attr_conflict: int = 0,
               watermark_new: Optional[datetime.datetime] = None,
               status: str = "SUCCESS") -> None:
    """
    Mark a pipeline run as complete (SUCCESS or PARTIAL) and write
    final row counts.

    status is "SUCCESS" by default. Pass "PARTIAL" when some parsers
    failed but the rest of the pipeline completed correctly. PARTIAL
    is documented in the schema as the value to use when partial-load
    is acceptable but worth flagging.

    For full failure, use fail_run() instead — that path captures the
    error message in ErrorMessage and sets status to FAILED.
    """
    if status not in ("SUCCESS", "PARTIAL"):
        raise ValueError(
            f"finish_run: status must be 'SUCCESS' or 'PARTIAL', got {status!r}. "
            "Use fail_run() for FAILED."
        )

    sql = """
        UPDATE pipeline.pipeline_runs
        SET    Status            = :status,
               RunEndTime        = SYSDATETIME(),
               DurationSeconds   = DATEDIFF(SECOND, RunStartTime, SYSDATETIME()),
               RowsExtracted     = :extracted,
               RowsCleansed      = :cleansed,
               RowsLoaded        = :loaded,
               RowsFlagged       = :flagged,
               RowsQuarantined   = :quarantined,
               RowsAttrConflict  = :attr_conflict,
               WatermarkNew      = :wm_new
        WHERE  RunId = :run_id
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {
            "status":       status,
            "extracted":    rows_extracted,
            "cleansed":     rows_cleansed,
            "loaded":       rows_loaded,
            "flagged":      rows_flagged,
            "quarantined":  rows_quarantined,
            "attr_conflict": rows_attr_conflict,
            "wm_new":       watermark_new,
            "run_id":       run_id,
        })

    log.info("[RunId=%d] Run finished — loaded=%d flagged=%d quarantined=%d",
             run_id, rows_loaded, rows_flagged, rows_quarantined)


def fail_run(engine, run_id: int, error_message: str) -> None:
    """
    Mark a pipeline run as FAILED with the error message.
    Called from the except block in each pipeline's main().
    """
    # Truncate to column size limit
    error_message = str(error_message)[:1990]

    sql = """
        UPDATE pipeline.pipeline_runs
        SET    Status          = 'FAILED',
               RunEndTime      = SYSDATETIME(),
               DurationSeconds = DATEDIFF(SECOND, RunStartTime, SYSDATETIME()),
               ErrorMessage    = :err
        WHERE  RunId = :run_id
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"err": error_message, "run_id": run_id})

    log.error("[RunId=%d] Run FAILED: %s", run_id, error_message[:200])


# ══════════════════════════════════════════════════════════════════════════════
# IDEMPOTENCY HELPER
# ══════════════════════════════════════════════════════════════════════════════

def clear_audit_for_rerun(engine, pipeline_key: int,
                           run_date: datetime.date) -> tuple:
    """
    Delete pipeline_flags and silver_quarantine rows for the given
    (PipelineKey, RunDate) so a re-run produces a clean audit trail.

    Why this exists:
    Gold tables use MERGE which is naturally idempotent — re-running the
    same date overwrites existing rows with the new values, no duplication.
    But pipeline_flags and silver_quarantine are append-only by design (each
    INSERT adds a row). Without this cleanup, re-running the same date
    accumulates duplicate flag/quarantine rows from each run.

    This function should be called early in the pipeline run, AFTER the
    new RunId has been created in pipeline_runs (so the new run's flags
    can be written immediately after) but BEFORE any flag/quarantine
    writes happen.

    Returns: (flags_deleted, quarantine_deleted) counts for logging.

    Note: this only affects rows for THIS (PipelineKey, RunDate). Rows from
    other dates and other pipelines are untouched. Run history in
    pipeline_runs is also untouched — that table stays append-only because
    you DO want to know that a re-run happened.
    """
    flags_sql = """
        DELETE FROM pipeline.pipeline_flags
        WHERE PipelineKey = :pk
          AND RunDate = :rd
    """
    quarantine_sql = """
        DELETE FROM pipeline.silver_quarantine
        WHERE PipelineKey = :pk
          AND RunDate = :rd
    """
    with engine.begin() as conn:
        flag_result = conn.execute(
            text(flags_sql), {"pk": pipeline_key, "rd": run_date}
        )
        quarantine_result = conn.execute(
            text(quarantine_sql), {"pk": pipeline_key, "rd": run_date}
        )

    flags_deleted = flag_result.rowcount or 0
    quarantine_deleted = quarantine_result.rowcount or 0

    if flags_deleted or quarantine_deleted:
        log.info(
            "[%s] Cleared previous run for %s: "
            "%d flags + %d quarantine rows deleted",
            PIPELINE_NAMES.get(pipeline_key, f"pk={pipeline_key}"),
            run_date.isoformat(),
            flags_deleted, quarantine_deleted,
        )

    return flags_deleted, quarantine_deleted


# ══════════════════════════════════════════════════════════════════════════════
# FLAG WRITER
# ══════════════════════════════════════════════════════════════════════════════

def write_flag(engine, run_id: int, run_date: datetime.date,
               pipeline_key: int, source_object: str,
               flag_type: str,
               source_record_id: Optional[str] = None,
               prospect_key: Optional[int] = None,
               flag_field: Optional[str] = None,
               original_value: Optional[str] = None,
               resolved_value: Optional[str] = None,
               suppressed_key: Optional[int] = None,
               notes: Optional[str] = None) -> None:
    """
    Write a single data quality flag to pipeline.pipeline_flags.
    flag_type must be one of the 10 controlled vocabulary values.
    """
    sql = """
        INSERT INTO pipeline.pipeline_flags
            (RunId, RunDate, PipelineKey, SourceObject, SourceRecordId,
             ProspectKey, FlagType, FlagField, OriginalValue, ResolvedValue,
             SuppressedKey, Notes, CreatedAt)
        VALUES
            (:run_id, :run_date, :pk, :src_obj, :src_id,
             :prospect_key, :flag_type, :flag_field, :orig_val, :res_val,
             :suppressed_key, :notes, SYSDATETIME())
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {
            "run_id":        run_id,
            "run_date":      run_date,
            "pk":            pipeline_key,
            "src_obj":       source_object,
            "src_id":        source_record_id,
            "prospect_key":  prospect_key,
            "flag_type":     flag_type,
            "flag_field":    flag_field,
            "orig_val":      str(original_value)[:499] if original_value is not None else None,
            "res_val":       str(resolved_value)[:499] if resolved_value is not None else None,
            "suppressed_key": suppressed_key,
            "notes":         str(notes)[:999] if notes is not None else None,
        })


def write_flags_batch(engine, flags: list) -> None:
    """
    Bulk insert a list of flag dicts. Each dict must have the same keys
    as write_flag's parameters (excluding engine).
    More efficient than calling write_flag() in a loop for large batches.
    """
    if not flags:
        return

    sql = """
        INSERT INTO pipeline.pipeline_flags
            (RunId, RunDate, PipelineKey, SourceObject, SourceRecordId,
             ProspectKey, FlagType, FlagField, OriginalValue, ResolvedValue,
             SuppressedKey, Notes, CreatedAt)
        VALUES
            (:run_id, :run_date, :pk, :src_obj, :src_id,
             :prospect_key, :flag_type, :flag_field, :orig_val, :res_val,
             :suppressed_key, :notes, SYSDATETIME())
    """
    rows = [{
        "run_id":         f["run_id"],
        "run_date":       f["run_date"],
        "pk":             f["pipeline_key"],
        "src_obj":        f["source_object"],
        "src_id":         f.get("source_record_id"),
        "prospect_key":   f.get("prospect_key"),
        "flag_type":      f["flag_type"],
        "flag_field":     f.get("flag_field"),
        "orig_val":       str(f["original_value"])[:499] if f.get("original_value") is not None else None,
        "res_val":        str(f["resolved_value"])[:499] if f.get("resolved_value") is not None else None,
        "suppressed_key": f.get("suppressed_key"),
        "notes":          str(f["notes"])[:999] if f.get("notes") is not None else None,
    } for f in flags]

    with engine.begin() as conn:
        conn.execute(text(sql), rows)

    log.info("Wrote %d flags to pipeline_flags", len(flags))


# ══════════════════════════════════════════════════════════════════════════════
# CONFLICT DETECTION (Day 2 — Spend pipeline)
# ══════════════════════════════════════════════════════════════════════════════

def detect_spend_conflicts(engine,
                           run_date: datetime.date,
                           run_id: int,
                           pipeline_key: int,
                           source_datasource: int,
                           canonical_datasource: int = 1,
                           tolerance: float = 0.01) -> list:
    """
    Compare a pipeline's gold writes against canonical truth (DataSource=1)
    for a given run date. Returns a list of flag dicts (one per divergent
    PropertyKey/VendorKey tuple) ready to pass to write_flags_batch().

    Caller is responsible for actually writing the flags. This function
    only DETECTS — it doesn't INSERT. That separation lets the caller log
    the count, decide whether to halt, etc.

    A divergence is flagged when ABS(canonical - source) > tolerance.
    Default tolerance is $0.01 (essentially "any divergence at all").

    The flag uses:
      FlagType        = SPEND_CONFLICT
      OriginalValue   = canonical spend (the "should be")
      ResolvedValue   = source/parser spend (the "is")
      SourceRecordId  = "{date_key}-P{prop}-V{vendor}" (matches Day 1 format)
      Notes           = "Canonical $X vs parser $Y, diff $Z"

    Both directions are flagged: parser-over-canonical and canonical-over-parser.
    The Notes always says "Canonical ... vs parser ..." regardless of direction
    so the dashboard can sort by sign of the divergence if needed.

    If canonical has no rows for the run date, this returns an empty list and
    logs a warning. Missing canonical data is not a failure of the source
    pipeline — canonical may run on a different schedule.
    """
    date_key = int(run_date.strftime("%Y%m%d"))

    # First check: does canonical have ANY rows for this date?
    canonical_check_sql = """
        SELECT COUNT(*) FROM dbo.fact_marketing_spend_daily
        WHERE  DataSource = :canon AND DateKey = :dk
    """
    with engine.begin() as conn:
        canon_count = conn.execute(
            text(canonical_check_sql),
            {"canon": canonical_datasource, "dk": date_key},
        ).scalar()

    if not canon_count:
        log.warning(
            "[CONFLICT] Canonical (DataSource=%d) has no rows for %s — "
            "skipping conflict detection. This is not a pipeline failure; "
            "canonical may not have run yet for this date.",
            canonical_datasource, run_date,
        )
        return []

    # Compare canonical vs source for the date. Inner join means we only
    # detect divergences for tuples present in BOTH — tuples present only in
    # canonical or only in source are NOT flagged here. Those are different
    # findings (missing-from-source / extra-in-source) which we may add later.
    sql = """
        SELECT s1.DateKey, s1.PropertyKey, s1.VendorKey,
               CAST(s1.Spend AS DECIMAL(18,4)) AS canonical_spend,
               CAST(s2.Spend AS DECIMAL(18,4)) AS source_spend,
               CAST(ABS(s1.Spend - s2.Spend) AS DECIMAL(18,4)) AS divergence
        FROM   dbo.fact_marketing_spend_daily s1
        JOIN   dbo.fact_marketing_spend_daily s2
            ON s1.DateKey     = s2.DateKey
           AND s1.PropertyKey = s2.PropertyKey
           AND s1.VendorKey   = s2.VendorKey
        WHERE  s1.DataSource = :canon
          AND  s2.DataSource = :src
          AND  s1.DateKey    = :dk
          AND  ABS(s1.Spend - s2.Spend) > :tol
        ORDER BY s1.PropertyKey, s1.VendorKey
    """
    with engine.begin() as conn:
        result = conn.execute(text(sql), {
            "canon": canonical_datasource,
            "src":   source_datasource,
            "dk":    date_key,
            "tol":   tolerance,
        }).fetchall()

    flags = []
    overflow_count = 0
    for row in result:
        dk_v        = row.DateKey
        prop        = row.PropertyKey
        vendor      = row.VendorKey
        canon_spend = float(row.canonical_spend)
        src_spend   = float(row.source_spend)
        divergence  = float(row.divergence)

        full_record_id = f"{dk_v}-P{prop}-V{vendor}"
        # SourceRecordId column is varchar(18). For this project (120 properties,
        # 12 vendors) the full ID fits — '20260422-P120-V12' is 17 chars.
        # If keys grow large enough that the synthetic ID overflows, the full
        # ID still lives in Notes so nothing is lost. We log a warning when
        # truncation happens so it surfaces in the run log rather than silently.
        if len(full_record_id) > 18:
            overflow_count += 1
        source_record_id = full_record_id[:18]

        flags.append({
            "run_id":           run_id,
            "run_date":         run_date,
            "pipeline_key":     pipeline_key,
            "source_object":    "fact_marketing_spend_daily",
            "source_record_id": source_record_id,
            "flag_type":        FLAG_SPEND_CONFLICT,
            "flag_field":       "Spend",
            "original_value":   f"{canon_spend:.4f}",
            "resolved_value":   f"{src_spend:.4f}",
            "notes": (
                f"Canonical ${canon_spend:.2f} vs parser ${src_spend:.2f}, "
                f"diff ${divergence:.2f} for "
                f"(Date={dk_v}, Property={prop}, Vendor={vendor})"
            ),
        })

    if overflow_count:
        log.warning(
            "[CONFLICT] %d SourceRecordId values exceeded varchar(18) and were "
            "truncated. Full identifier preserved in Notes. Consider widening "
            "pipeline_flags.SourceRecordId column.",
            overflow_count,
        )

    log.info(
        "[CONFLICT] %d divergent (Property,Vendor) tuples found "
        "for %s (DataSource %d vs %d, tolerance $%.2f)",
        len(flags), run_date, canonical_datasource, source_datasource, tolerance,
    )
    return flags


# ══════════════════════════════════════════════════════════════════════════════
# QUARANTINE WRITER
# ══════════════════════════════════════════════════════════════════════════════

def write_quarantine(engine, run_id: int, run_date: datetime.date,
                     pipeline_key: int, source_object: str,
                     quarantine_reason: str,
                     raw_data: dict,
                     source_record_id: Optional[str] = None,
                     notes: Optional[str] = None) -> None:
    """
    Write a record to pipeline.silver_quarantine.
    raw_data is the full source record as a dict — serialized to JSON.
    """
    import json
    raw_json = json.dumps(raw_data, default=str)  # NVARCHAR(MAX) — no truncation

    sql = """
        INSERT INTO pipeline.silver_quarantine
            (RunId, RunDate, PipelineKey, SourceObject, SourceRecordId,
             QuarantineReason, RawData, Notes, CreatedAt)
        VALUES
            (:run_id, :run_date, :pk, :src_obj, :src_id,
             :reason, :raw, :notes, SYSDATETIME())
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {
            "run_id":   run_id,
            "run_date": run_date,
            "pk":       pipeline_key,
            "src_obj":  source_object,
            "src_id":   source_record_id,
            "reason":   quarantine_reason,
            "raw":      raw_json,
            "notes":    str(notes)[:999] if notes is not None else None,
        })


# ══════════════════════════════════════════════════════════════════════════════
# SILVER_PROCESSED WRITER
# ══════════════════════════════════════════════════════════════════════════════

def write_processed(engine, run_id: int, run_date: datetime.date,
                    pipeline_key: int, source_object: str,
                    outcome: str,
                    source_record_id: Optional[str] = None,
                    prospect_key: Optional[int] = None,
                    property_key: Optional[int] = None,
                    vendor_key: Optional[int] = None,
                    duplicate_of: Optional[str] = None,
                    flag_summary: Optional[str] = None,
                    gold_table: Optional[str] = None) -> None:
    """
    Write a single record outcome to pipeline.silver_processed.
    Called for every record processed regardless of outcome.
    """
    sql = """
        INSERT INTO pipeline.silver_processed
            (RunId, RunDate, ProcessedAt, PipelineKey, SourceObject,
             SourceRecordId, Outcome, ProspectKey, PropertyKey, VendorKey,
             DuplicateOf, FlagSummary, GoldTable)
        VALUES
            (:run_id, :run_date, SYSDATETIME(), :pk, :src_obj,
             :src_id, :outcome, :prospect_key, :property_key, :vendor_key,
             :dup_of, :flag_summary, :gold_table)
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {
            "run_id":       run_id,
            "run_date":     run_date,
            "pk":           pipeline_key,
            "src_obj":      source_object,
            "src_id":       source_record_id,
            "outcome":      outcome,
            "prospect_key": prospect_key,
            "property_key": property_key,
            "vendor_key":   vendor_key,
            "dup_of":       duplicate_of,
            "flag_summary": str(flag_summary)[:199] if flag_summary is not None else None,
            "gold_table":   str(gold_table)[:99] if gold_table is not None else None,
        })


def write_processed_batch(engine, records: list) -> None:
    """
    Bulk insert a list of processed record dicts.
    More efficient than write_processed() in a loop for large batches.
    Each dict must have keys matching write_processed() parameters (excluding engine).
    """
    if not records:
        return

    sql = """
        INSERT INTO pipeline.silver_processed
            (RunId, RunDate, ProcessedAt, PipelineKey, SourceObject,
             SourceRecordId, Outcome, ProspectKey, PropertyKey, VendorKey,
             DuplicateOf, FlagSummary, GoldTable)
        VALUES
            (:run_id, :run_date, SYSDATETIME(), :pk, :src_obj,
             :src_id, :outcome, :prospect_key, :property_key, :vendor_key,
             :dup_of, :flag_summary, :gold_table)
    """
    rows = [{
        "run_id":       r["run_id"],
        "run_date":     r["run_date"],
        "pk":           r["pipeline_key"],
        "src_obj":      r["source_object"],
        "src_id":       r.get("source_record_id"),
        "outcome":      r["outcome"],
        "prospect_key": r.get("prospect_key"),
        "property_key": r.get("property_key"),
        "vendor_key":   r.get("vendor_key"),
        "dup_of":       r.get("duplicate_of"),
        "flag_summary": str(r["flag_summary"])[:199] if r.get("flag_summary") is not None else None,
        "gold_table":   str(r["gold_table"])[:99] if r.get("gold_table") is not None else None,
    } for r in records]

    with engine.begin() as conn:
        conn.execute(text(sql), rows)

    log.info("Wrote %d records to silver_processed", len(records))


# ══════════════════════════════════════════════════════════════════════════════
# LOOKUP HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def load_property_lookup(engine) -> dict:
    """
    Returns {PropertyKey: {PropertyName, MarketKey, RegionKey, State, City, TotalUnits}}
    Used by all pipelines for PropertyKey validation and timezone resolution.

    Note: the Azure table stores geographic fields as PropertyState /
    PropertyCity (disambiguating from any other State column on joined
    tables). The query aliases them back to State / City so downstream
    consumers can stay unaware of the Azure-side naming.
    """
    sql = """
        SELECT PropertyKey, PropertyName, MarketKey, RegionKey,
               PropertyState AS State, PropertyCity AS City, TotalUnits
        FROM   dbo.dim_property
        WHERE  IsActive = 1
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    lookup = {r[0]: {
        "PropertyName": r[1],
        "MarketKey":    r[2],
        "RegionKey":    r[3],
        "State":        r[4],
        "City":         r[5],
        "TotalUnits":   r[6],
    } for r in rows}

    log.info("Loaded property lookup: %d active properties", len(lookup))
    return lookup


def load_vendor_lookup(engine) -> dict:
    """
    Returns {VendorKey: {VendorName, ChannelKey}}
    Used for VendorKey validation and UTM mapping.
    """
    sql = "SELECT VendorKey, VendorName, ChannelKey FROM dbo.dim_vendor"
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    lookup = {r[0]: {"VendorName": r[1], "ChannelKey": r[2]} for r in rows}
    log.info("Loaded vendor lookup: %d vendors", len(lookup))
    return lookup


def load_campaign_lookup(engine) -> dict:
    """
    Returns {CampaignId: {VendorKey, ChannelKey, CampaignName}}
    Used by CRM pipeline for campaign → vendor attribution.
    """
    sql = """
        SELECT CampaignId, VendorKey, ChannelKey, CampaignName
        FROM   pipeline.campaign_vendor_lookup
        WHERE  IsActive = 1
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    lookup = {r[0]: {
        "VendorKey":    r[1],
        "ChannelKey":   r[2],
        "CampaignName": r[3],
    } for r in rows}

    log.info("Loaded campaign lookup: %d active campaigns", len(lookup))
    return lookup


def load_date_lookup(engine) -> dict:
    """
    Returns {date_str: DateKey} for fast date → DateKey conversion.
    date_str format: 'YYYY-MM-DD'
    """
    sql = "SELECT Date, DateKey FROM dbo.dim_date"
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    lookup = {str(r[0])[:10]: r[1] for r in rows}
    log.info("Loaded date lookup: %d dates", len(lookup))
    return lookup


# ══════════════════════════════════════════════════════════════════════════════
# IDENTITY HELPERS (CRM pipeline)
# ══════════════════════════════════════════════════════════════════════════════

def hash_prospect_key(salesforce_id: str) -> int:
    """
    Seed ProspectKey from Salesforce Lead.Id on first insert.
    Uses MD5-derived integer (NOT SQL Server ABS(CHECKSUM) — different algorithm).
    Result is deterministic and collision-resistant at demo scale.
    After first insert, always look up by SourceSalesforceId — never recompute.
    The hash is only used to seed the first row; it is NOT recomputed for identity resolution.
    """
    h = int(hashlib.md5(salesforce_id.encode()).hexdigest(), 16)
    return h % 2_147_483_647


def resolve_prospect_key(engine, salesforce_id: str,
                         property_key: int,
                         run_date: datetime.date,
                         vendor_key: Optional[int] = None,
                         channel_key: Optional[int] = None) -> int:
    """
    Two-phase ProspectKey resolution:
      Phase 1 — lookup by SourceSalesforceId in silver_prospects
      Phase 2 — if not found, insert new row with hash-seeded ProspectKey

    Returns ProspectKey (int).
    """
    # Phase 1: lookup
    sql_lookup = """
        SELECT ProspectKey FROM pipeline.silver_prospects
        WHERE  SourceSalesforceId = :sfid
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql_lookup), {"sfid": salesforce_id}).fetchone()

    if row:
        return row[0]

    # Phase 2: first insert — seed from hash
    prospect_key = hash_prospect_key(salesforce_id)

    sql_insert = """
        INSERT INTO pipeline.silver_prospects
            (ProspectKey, SourceSalesforceId, PropertyKey, VendorKey, ChannelKey,
             FirstSeen, LastSeen, IsMaster, DataSource, CreatedAt, UpdatedAt)
        VALUES
            (:pk, :sfid, :prop_key, :vk, :ck,
             :first_seen, :last_seen, 1, 2, SYSDATETIME(), SYSDATETIME())
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(sql_insert), {
                "pk":         prospect_key,
                "sfid":       salesforce_id,
                "prop_key":   property_key,
                "vk":         vendor_key,
                "ck":         channel_key,
                "first_seen": run_date,
                "last_seen":  run_date,
            })
    except Exception:
        # Race condition on first run — re-lookup if insert fails (duplicate hash)
        with engine.connect() as conn:
            row = conn.execute(text(sql_lookup), {"sfid": salesforce_id}).fetchone()
        if row:
            return row[0]
        raise

    return prospect_key


def update_prospect_last_seen(engine, salesforce_id: str,
                               run_date: datetime.date) -> None:
    """Update LastSeen on an existing silver_prospects row."""
    sql = """
        UPDATE pipeline.silver_prospects
        SET    LastSeen  = :last_seen,
               UpdatedAt = SYSDATETIME()
        WHERE  SourceSalesforceId = :sfid
          AND  LastSeen < :last_seen
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"last_seen": run_date, "sfid": salesforce_id})


def resolve_prospect_keys_batch(engine, prospects: list,
                                 run_date: datetime.date) -> dict:
    """
    Bulk identity resolution for many prospects in three round-trips instead
    of 2N. Semantically equivalent to calling resolve_prospect_key() +
    update_prospect_last_seen() for each, but typically 100-1000x faster
    because Azure SQL round-trip latency (~200-400ms from a home network)
    dominates per-row execution time.

    Timing characteristic:
      Per-row approach:   2 × N × 300ms  = ~16 min for N=2703
      This batch:         ~3 × 300ms     = <1s for N=2703

    That collapse of the connection-open window from ~30 minutes to ~1 second
    is what makes the pipeline survive flaky home networks.

    Parameters
    ----------
    engine     : SQLAlchemy engine
    prospects  : list of dicts. Each dict MUST contain:
                   - "sfid"       : str  (Salesforce Lead/Contact Id)
                   - "prop_key"   : int  (PropertyKey, or None)
                   - "vendor_key" : int or None
                   - "channel_key": int or None
    run_date   : date used for FirstSeen on new rows and LastSeen on all

    Returns
    -------
    dict {salesforce_id: ProspectKey}. Only contains entries whose prop_key
    was non-None — callers should have already filtered out prospects that
    couldn't resolve a property.

    Concurrency note
    ----------------
    The function is not guarded against concurrent runs of THIS pipeline
    inserting the same Salesforce Ids. If two daily CRM runs kick off at
    the same time, the INSERT could race. That's the same risk the old
    per-row implementation had (its retry-on-duplicate handler), and the
    CRM pipeline is not designed for concurrent invocation.
    """
    if not prospects:
        return {}

    # Filter out anything without a property key — we don't write those
    # to silver_prospects (they'd violate the FK contract)
    eligible = [p for p in prospects if p.get("prop_key") is not None]
    if not eligible:
        return {}

    sfid_to_prospect = {p["sfid"]: p for p in eligible}
    all_sfids = list(sfid_to_prospect.keys())

    # ── Round trip 1: bulk SELECT which sfids already have ProspectKeys ────
    # Use a temp table rather than a massive IN clause. SQL Server has a
    # hard limit of ~2100 parameters per batch, and IN-lists with 2703
    # items would blow through it. The table-variable + JOIN approach
    # scales to any N and is also faster on the server side.
    sfid_to_key: dict = {}

    with engine.begin() as conn:
        # Create a session-scoped temp table for the sfid batch
        conn.execute(text("""
            CREATE TABLE #sfid_batch (
                SourceSalesforceId VARCHAR(18) NOT NULL PRIMARY KEY
            )
        """))

        # Bulk-load the sfids — one executemany call, many rows
        conn.execute(
            text("INSERT INTO #sfid_batch (SourceSalesforceId) VALUES (:sfid)"),
            [{"sfid": s} for s in all_sfids],
        )

        # Join against silver_prospects to get existing ProspectKeys
        rows = conn.execute(text("""
            SELECT sp.SourceSalesforceId, sp.ProspectKey
            FROM   pipeline.silver_prospects sp
            JOIN   #sfid_batch b
              ON   b.SourceSalesforceId = sp.SourceSalesforceId
        """)).fetchall()

        for sfid, pk in rows:
            sfid_to_key[sfid] = pk

    # ── Round trip 2: bulk INSERT for sfids not yet in silver_prospects ────
    new_sfids = [s for s in all_sfids if s not in sfid_to_key]

    if new_sfids:
        insert_rows = []
        for sfid in new_sfids:
            p = sfid_to_prospect[sfid]
            pk = hash_prospect_key(sfid)
            sfid_to_key[sfid] = pk
            insert_rows.append({
                "pk":         pk,
                "sfid":       sfid,
                "prop_key":   p["prop_key"],
                "vk":         p.get("vendor_key"),
                "ck":         p.get("channel_key"),
                "first_seen": run_date,
                "last_seen":  run_date,
            })

        sql_insert = """
            INSERT INTO pipeline.silver_prospects
                (ProspectKey, SourceSalesforceId, PropertyKey, VendorKey, ChannelKey,
                 FirstSeen, LastSeen, IsMaster, DataSource, CreatedAt, UpdatedAt)
            VALUES
                (:pk, :sfid, :prop_key, :vk, :ck,
                 :first_seen, :last_seen, 1, 2, SYSDATETIME(), SYSDATETIME())
        """
        # Chunk to stay safely under pyodbc's 2100-parameter ceiling.
        # 7 named params × 250 rows = 1,750 params per batch.
        CHUNK = 250
        with engine.begin() as conn:
            for i in range(0, len(insert_rows), CHUNK):
                conn.execute(text(sql_insert), insert_rows[i:i + CHUNK])

    # ── Round trip 3: bulk UPDATE LastSeen for existing rows ───────────────
    # Only touches rows whose LastSeen is older than run_date. Newly-inserted
    # rows already have LastSeen = run_date so they're no-ops in this UPDATE.
    # Using the same temp-table-join pattern as the SELECT.
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE #sfid_update (
                SourceSalesforceId VARCHAR(18) NOT NULL PRIMARY KEY
            )
        """))
        conn.execute(
            text("INSERT INTO #sfid_update (SourceSalesforceId) VALUES (:sfid)"),
            [{"sfid": s} for s in all_sfids],
        )
        conn.execute(text("""
            UPDATE sp
            SET    sp.LastSeen  = :last_seen,
                   sp.UpdatedAt = SYSDATETIME()
            FROM   pipeline.silver_prospects sp
            JOIN   #sfid_update b
              ON   b.SourceSalesforceId = sp.SourceSalesforceId
            WHERE  sp.LastSeen < :last_seen
        """), {"last_seen": run_date})

    return sfid_to_key


# ══════════════════════════════════════════════════════════════════════════════
# DATE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# Timezone offset by US state (UTC offset in hours, standard time)
STATE_UTC_OFFSET = {
    # Eastern
    "CT": -5, "DE": -5, "FL": -5, "GA": -5, "IN": -5, "ME": -5,
    "MD": -5, "MA": -5, "MI": -5, "NH": -5, "NJ": -5, "NY": -5,
    "NC": -5, "OH": -5, "PA": -5, "RI": -5, "SC": -5, "VT": -5,
    "VA": -5, "WV": -5, "DC": -5,
    # Central
    "AL": -6, "AR": -6, "IL": -6, "IA": -6, "KS": -6, "KY": -6,
    "LA": -6, "MN": -6, "MS": -6, "MO": -6, "NE": -6, "ND": -6,
    "OK": -6, "SD": -6, "TN": -6, "TX": -6, "WI": -6,
    # Mountain
    "AZ": -7, "CO": -7, "ID": -7, "MT": -7, "NM": -7, "UT": -7,
    "WY": -7,
    # Pacific
    "CA": -8, "NV": -8, "OR": -8, "WA": -8,
    # Other
    "AK": -9, "HI": -10,
}


def date_str_to_key(date_str: str) -> Optional[int]:
    """
    Convert 'YYYY-MM-DD' string to DateKey integer (YYYYMMDD).
    Returns None if date_str is null/empty.
    """
    if not date_str:
        return None
    try:
        return int(date_str[:10].replace("-", ""))
    except (ValueError, TypeError):
        return None


def normalize_lease_date(date_str: str, state: str) -> Optional[str]:
    """
    LeaseStartDate__c arrives as a DATE (no timezone) in Salesforce.
    Treat as local property time. Returns 'YYYY-MM-DD' string.
    For attribution purposes this is the LeaseDateKey anchor.
    """
    if not date_str:
        return None
    return date_str[:10]  # Already local date — no timezone conversion needed


def normalize_sf_datetime(dt_str: str) -> Optional[datetime.datetime]:
    """
    Parse Salesforce UTC datetime string '2026-04-11T01:23:45.000+0000'
    to a Python datetime (UTC, timezone-naive for SQL insertion).
    """
    if not dt_str:
        return None
    try:
        # Handle both +0000 and Z suffixes
        dt_str = dt_str.replace("+0000", "").replace("Z", "").strip()
        return datetime.datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S")
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════════════════════════
# UTM → VENDOR MAPPING (CRM pipeline)
# ══════════════════════════════════════════════════════════════════════════════

# Maps (utm_source, utm_medium) → VendorKey
# Used when UTM fields are present on a lead record
UTM_TO_VENDOR = {
    ("zillow",          "ils"):         1,
    ("apartments.com",  "ils"):         2,
    ("apartment_list",  "ils"):         3,
    ("google",          "cpc"):         4,
    ("bing",            "cpc"):         5,
    ("facebook",        "paid_social"): 6,
    ("instagram",       "paid_social"): 7,
    ("google",          "display"):     8,
    ("stackadapt",      "display"):     9,
    ("tradedesk",       "display"):     10,
    ("email",           "email"):       11,
    ("google",          "organic"):     12,  # organic/direct
}


def resolve_vendor_from_utm(utm_source: Optional[str],
                             utm_medium: Optional[str],
                             campaign_id: Optional[str],
                             campaign_lookup: dict,
                             vendor_lookup: dict) -> tuple:
    """
    Resolve VendorKey and ChannelKey from UTM fields or campaign lookup.

    Resolution order:
      1. UTM source + medium → UTM_TO_VENDOR map (ChannelKey resolved via vendor_lookup)
      2. Campaign ID → campaign_vendor_lookup
      3. Fallback → VendorKey 12 (Organic / Direct), ChannelKey 6

    Returns (vendor_key, channel_key, resolution_method)
    Always returns a real ChannelKey — never None.

    Resolution order — checked in sequence, first match wins:
      1. UTM fields (utm_source + utm_medium) → UTM_TO_VENDOR map
         Used by 2,093 of 2,150 daily leads (97.3%) in mock data.
      2. Campaign ID (Campaign__c) → campaign_vendor_lookup table
         Used for organic leads (57/day) which have CAMP0012202604 but null UTM.
         Also acts as fallback when UTM fields are missing but campaign is present.
      3. Hardcoded VK12/CK6 — true last resort only.
         Should rarely fire in practice since every lead has a Campaign__c.

    UTM structure (standardized across mock generator and real Salesforce tagging):
      ILS:         utm_source=zillow|apartments.com|apartment_list, utm_medium=ils
      Paid Search: utm_source=google|bing,                          utm_medium=cpc
      Paid Social: utm_source=facebook|instagram,                   utm_medium=paid_social
      Display:     utm_source=google|stackadapt|tradedesk,          utm_medium=display
      Email:       utm_source=email,                                utm_medium=email
      Organic:     utm_source=null, utm_medium=null, Campaign__c=CAMP0012202604

    Note: google+cpc=VK4 (Google Ads) vs google+display=VK8 (Google Display).
    The medium disambiguates — both use "google" as utm_source.
    """
    # 1. UTM match — resolve ChannelKey via vendor_lookup so it is never None
    if utm_source and utm_medium:
        key = (utm_source.lower().strip(), utm_medium.lower().strip())
        if key in UTM_TO_VENDOR:
            vk = UTM_TO_VENDOR[key]
            ck = vendor_lookup.get(vk, {}).get("ChannelKey")
            if vk == 12:
                ck = 6  # Organic / Direct is always CK6
            if ck is None:
                raise ValueError(
                    f"resolve_vendor_from_utm: VendorKey={vk} resolved from UTM "
                    f"({utm_source}/{utm_medium}) but has no ChannelKey in vendor_lookup. "
                    f"Ensure dim_vendor is fully populated before running the pipeline."
                )
            return vk, ck, "UTM"

    # 2. Campaign lookup — VendorKey and ChannelKey both present by design
    if campaign_id and campaign_id in campaign_lookup:
        camp = campaign_lookup[campaign_id]
        return camp["VendorKey"], camp["ChannelKey"], "CAMPAIGN"

    # 3. Organic / Direct fallback — null UTM, no campaign match
    return 12, 6, "ORGANIC_FALLBACK"


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# Whitelist of tables allowed in validate_row_counts to prevent SQL injection
_ALLOWED_GOLD_TABLES = {
    "dbo.fact_leasing_daily",
    "dbo.fact_prospect_journey",
    "dbo.fact_marketing_spend_daily",
    "dbo.fact_marketing_funnel_daily",
    "dbo.fact_property_ops_daily",
}

def validate_row_counts(engine, table_name: str, expected_min: int) -> int:
    """
    Validate that a gold table has at least expected_min rows with DataSource > 1.
    Returns actual row count. Raises if below threshold.
    table_name must be in _ALLOWED_GOLD_TABLES whitelist.
    """
    if table_name not in _ALLOWED_GOLD_TABLES:
        raise ValueError(f"validate_row_counts: table '{table_name}' not in whitelist")
    sql = f"SELECT COUNT(*) FROM {table_name} WHERE DataSource > 1"
    with engine.connect() as conn:
        actual = conn.execute(text(sql)).scalar_one()

    if actual < expected_min:
        raise ValueError(
            f"Row count validation failed for {table_name}: "
            f"expected >= {expected_min}, got {actual}"
        )
    log.info("Row count OK: %s has %d live pipeline rows", table_name, actual)
    return actual


def check_attr_conflict(engine, run_date: datetime.date) -> list:
    """
    Check for ATTR_CONFLICT: AttributedNewLeases > NewLeases
    for any DateKey + PropertyKey on or after run_date.
    Returns list of conflict rows for quarantine processing.
    """
    sql = """
        SELECT DateKey, PropertyKey, NewLeases, AttributedNewLeases,
               (AttributedNewLeases - NewLeases) AS ConflictDelta
        FROM   dbo.fact_leasing_daily
        WHERE  DataSource = 2
          AND  AttributedNewLeases > NewLeases
          AND  DateKey >= :dk
    """
    date_key = int(run_date.strftime("%Y%m%d"))
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"dk": date_key}).fetchall()

    if rows:
        log.error("ATTR_CONFLICT detected: %d rows with AttributedNewLeases > NewLeases",
                  len(rows))
    return [dict(r._mapping) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# BRONZE SOURCE STAGING (Cloud — Phase 3)
# ══════════════════════════════════════════════════════════════════════════════
#
# Pipelines running in Azure Function Apps need to read source files from
# Azure Blob Storage instead of a local mock_sources/ directory. The cleanest
# bridge is to download the bronze blobs to a tmp directory and let the
# existing local-file pipeline run unchanged against that directory.
#
# Architecture: bronze/{pipeline_name}/{YYYY-MM-DD}/{filename}
# Example:      bronze/spend/2026-04-22/google_ads_export_20260422.csv
#
# Auth: uses DefaultAzureCredential() which automatically picks up the
# Function App's system-assigned managed identity in production. In local
# development it falls back to the developer's az-cli login.
#
# Caller pattern:
#   tmp_root = stage_bronze_to_tmp("spend", run_date)
#   os.environ["SOURCE_PATH"] = tmp_root
#   run_spend_pipeline(run_date)        # reads from tmp_root/{date}/...
#
# The function returns the PARENT directory (tmp_root), not the dated
# subdirectory, because spend_pipeline's resolve_source_dir() looks for
# a "{SOURCE_PATH}/{run_date.isoformat()}" subfolder. The blob layout
# matches that convention so we just point SOURCE_PATH at the parent.

def stage_bronze_to_tmp(
    pipeline_name: str,
    run_date: datetime.date,
    storage_account: str = "maatruthlake",
    container: str = "bronze",
    tmp_parent: str = None,
) -> str:
    """
    Download all blobs under bronze/{pipeline_name}/{YYYY-MM-DD}/ to a
    local tmp directory and return the PARENT path of the dated subfolder.

    The returned path is suitable for pointing SOURCE_PATH at, since
    pipeline source-resolvers look for {SOURCE_PATH}/{run_date}/ as a
    dated subfolder.

    Returns:
        tmp_parent path containing one subfolder named {YYYY-MM-DD} with
        all bronze blobs for that pipeline+date.

    Raises:
        FileNotFoundError if no blobs exist for that pipeline+date.

    Example:
        >>> tmp_root = stage_bronze_to_tmp("spend", date(2026, 4, 22))
        >>> # tmp_root = "/tmp/bronze_stage_xyz"
        >>> # /tmp/bronze_stage_xyz/2026-04-22/google_ads_export_20260422.csv
        >>> # /tmp/bronze_stage_xyz/2026-04-22/apartment_list_20260422.csv
        >>> # ... etc
    """
    import tempfile
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    # Build the blob prefix that matches our convention
    date_iso = run_date.isoformat()  # "2026-04-22"
    prefix   = f"{pipeline_name}/{date_iso}/"
    account_url = f"https://{storage_account}.blob.core.windows.net"

    # Create tmp staging area. mkdtemp ensures unique path per run, no
    # collisions if multiple pipelines stage concurrently. The OS cleans
    # /tmp on its own schedule so we don't need to delete after.
    if tmp_parent is None:
        tmp_parent = tempfile.mkdtemp(prefix="bronze_stage_")
    dated_dir = os.path.join(tmp_parent, date_iso)
    os.makedirs(dated_dir, exist_ok=True)

    log.info("[BRONZE] Staging blobs from %s/%s%s to %s",
             account_url, container, prefix, dated_dir)

    # Connect using managed identity in cloud, az-cli locally
    credential = DefaultAzureCredential()
    blob_svc   = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_svc.get_container_client(container)

    blob_count = 0
    total_bytes = 0
    for blob in container_client.list_blobs(name_starts_with=prefix):
        # Strip the prefix to get just the filename
        # e.g. "spend/2026-04-22/google_ads_export_20260422.csv"
        #   -> "google_ads_export_20260422.csv"
        filename = blob.name[len(prefix):]
        if not filename or filename.endswith("/"):
            # Skip the prefix marker itself or any nested subfolders
            continue
        if filename.startswith("_"):
            # Skip metadata files like _metadata.json or _SUCCESS markers
            log.debug("[BRONZE] Skipping metadata blob: %s", blob.name)
            continue

        local_path = os.path.join(dated_dir, filename)
        blob_client = container_client.get_blob_client(blob.name)
        with open(local_path, "wb") as f:
            stream = blob_client.download_blob()
            stream.readinto(f)

        blob_count  += 1
        total_bytes += blob.size or 0
        log.info("[BRONZE]   %s (%d bytes)", filename, blob.size or 0)

    if blob_count == 0:
        raise FileNotFoundError(
            f"No blobs found under {container}/{prefix} in {storage_account}. "
            f"Either the bronze layer is empty for this date, or the "
            f"managed identity lacks Storage Blob Data Reader on the account."
        )

    log.info("[BRONZE] Staged %d files (%.1f KB) to %s",
             blob_count, total_bytes / 1024, dated_dir)
    return tmp_parent


# ══════════════════════════════════════════════════════════════════════════════
# QUICK SELF-TEST
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    print("pipeline_utils.py — self test")
    print("Testing connection with env vars...")

    try:
        engine = get_engine()
        test_connection(engine)

        # Test lookup loads
        props    = load_property_lookup(engine)
        vendors  = load_vendor_lookup(engine)
        campaigns = load_campaign_lookup(engine)
        dates    = load_date_lookup(engine)

        print(f"\nLookup tables loaded:")
        print(f"  Properties:  {len(props)}")
        print(f"  Vendors:     {len(vendors)}")
        print(f"  Campaigns:   {len(campaigns)}")
        print(f"  Dates:       {len(dates)}")

        # Test watermarks
        for pk in [PIPELINE_CRM, PIPELINE_SPEND, PIPELINE_OPS]:
            wm = get_watermark(engine, pk)
            print(f"  Watermark [{PIPELINE_NAMES[pk]}]: {wm}")

        print("\nAll checks passed. pipeline_utils.py is ready.")
        engine.dispose()
        sys.exit(0)

    except Exception as e:
        print(f"\nFAILED: {e}")
        sys.exit(1)
