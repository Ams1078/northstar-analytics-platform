"""
publish_canonical_truth_incremental_to_sql.py
============================================
NorthStar MAA — Incremental Daily Canonical Truth SQL Publisher

Purpose:
    Publish a one-day canonical truth slice into the Azure SQL silver layer.

What it does:
    - reads one-day parquet files from ./truth_store/daily/YYYY-MM-DD/
    - runs the same pre-publish alignment checks as the full publisher
    - deletes ONLY that run date for DataSource = 1 from each target fact table
    - inserts ONLY that day's rows

This leaves historical silver rows in place and makes same-day reruns safe.

Usage:
    python publish_canonical_truth_incremental_to_sql.py --date 2026-04-12
    python publish_canonical_truth_incremental_to_sql.py --date 2026-04-12 --dry-run
    python publish_canonical_truth_incremental_to_sql.py --date 2026-04-12 --table funnel
    python publish_canonical_truth_incremental_to_sql.py --date 2026-04-12 --slice-dir ./truth_store/daily/2026-04-12
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pipeline_utils import connect_with_retry
from sqlalchemy.pool import NullPool

load_dotenv()

DATASOURCE = 1
CHUNK_SIZE = 10_000
TRUTH_DIR = Path(os.environ.get("TRUTH_DIR", "/tmp/truth_store"))
DEFAULT_DAILY_ROOT = TRUTH_DIR / "daily"
FUNNEL_STAGE_MAP = {1: "Impressions", 2: "Clicks", 3: "Visits", 4: "Leads", 5: "Leases"}

PUBLISH_PLAN = [
    {
        "parquet": "canonical_spend_truth",
        "sql_table": "dbo.fact_marketing_spend_daily",
        "key": "spend",
        "transform": None,
        "slice_date_column": "DateKey",
        "sql_delete_column": "DateKey",
        "columns": {
            "DateKey": None, "PropertyKey": None, "VendorKey": None, "Spend": None, "DataSource": None,
        },
    },
    {
        "parquet": "canonical_funnel_truth",
        "sql_table": "dbo.fact_marketing_funnel_daily",
        "key": "funnel",
        "transform": "unpivot_funnel",
        "slice_date_column": "DateKey",
        "sql_delete_column": "DateKey",
        "columns": {
            "DateKey": None, "PropertyKey": None, "VendorKey": None, "FunnelStageKey": None,
            "MetricValue": None, "DataSource": None,
        },
    },
    {
        "parquet": "canonical_touch_truth",
        "sql_table": "dbo.fact_prospect_journey",
        "key": "touch",
        "transform": None,
        "slice_date_column": "TouchDateKey",
        "sql_delete_column": "DateKey",
        "columns": {
            "ProspectKey": None, "PropertyKey": None, "TouchDateKey": "DateKey", "VendorKey": None,
            "ChannelKey": None, "FunnelStageKey": None, "TouchSequence": "TouchNumber",
            "TotalTouches": None, "DaysBeforeLease": None, "LeaseDateKey": None, "Converted": None,
            "AttributedCredit": None, "IsDirectCredit": None, "IsAssistedCredit": None,
            "LeaseValue": "LeaseValueAnnual", "DataSource": None,
        },
    },
    {
        "parquet": "canonical_leasing_truth",
        "sql_table": "dbo.fact_leasing_daily",
        "key": "leasing",
        "transform": None,
        "slice_date_column": "LeaseDateKey",
        "sql_delete_column": "DateKey",
        "columns": {
            "LeaseDateKey": "DateKey", "PropertyKey": None, "Leads": None, "NewLeases": None,
            "Visits": None, "AttributedNewLeases": None, "UnattributedLeases": None, "DataSource": None,
        },
    },
    {
        "parquet": "canonical_ops_truth",
        "sql_table": "dbo.fact_property_ops_daily",
        "key": "ops",
        "transform": None,
        "slice_date_column": "DateKey",
        "sql_delete_column": "DateKey",
        "columns": {
            "DateKey": None, "PropertyKey": None, "OccupiedUnits": None, "VacantUnits": None,
            "VacantReadyUnits": "AvailableUnits", "MoveIns": None, "MoveOuts": None,
            "LeaseExpirations": None, "ScheduledMoveIns": None,
            "LeaseExpirations_Next60D": None, "ScheduledMoveIns_Next60D": None, "DataSource": None,
        },
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a one-day truth slice into Azure SQL.")
    parser.add_argument("--date", required=True, help="Run date YYYY-MM-DD")
    parser.add_argument("--slice-dir", default=None, help="Override daily slice directory. Default: ./truth_store/daily/YYYY-MM-DD")
    parser.add_argument("--validate-only", action="store_true", help="Validate daily files but do not write to SQL")
    parser.add_argument("--dry-run", action="store_true", help="Show actions but do not write to SQL")
    parser.add_argument("--table", choices=[p["key"] for p in PUBLISH_PLAN], default=None, help="Publish only one target table")
    return parser.parse_args()



def parse_run_date(raw: str) -> tuple[date, int]:
    run_date = date.fromisoformat(raw)
    return run_date, int(run_date.strftime("%Y%m%d"))



def get_engine():
    server = os.environ["SQL_SERVER"]
    database = os.environ["SQL_DATABASE"]
    username = os.environ["SQL_USERNAME"]
    password = os.environ["SQL_PASSWORD"]
    driver = os.environ.get("SQL_DRIVER", "ODBC Driver 18 for SQL Server")
    odbc = (
        f"DRIVER={{{driver}}};"
        f"SERVER=tcp:{server},1433;"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;"
        "Authentication=SqlPassword;Connection Timeout=300;"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}",
        fast_executemany=True,
        future=True,
        pool_pre_ping=True,
        poolclass=NullPool,
    )



def load_parquet(slice_dir: Path, parquet_name: str) -> pd.DataFrame:
    path = slice_dir / f"{parquet_name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing daily slice file: {path}")
    df = pd.read_parquet(path, engine="pyarrow")
    print(f"  Loaded {path.name:<35} {len(df):>10,} rows")
    return df



def enforce_single_day(df: pd.DataFrame, date_column: str, date_key: int, table_key: str) -> pd.DataFrame:
    if date_column not in df.columns:
        raise KeyError(f"{table_key} missing expected date column: {date_column}")
    bad = df[df[date_column].astype("Int64") != date_key]
    if not bad.empty:
        distinct_dates = sorted(bad[date_column].dropna().astype("Int64").astype(str).unique().tolist())[:5]
        raise ValueError(
            f"{table_key} daily slice is not restricted to requested date {date_key}. "
            f"Found {len(bad):,} off-date rows in column {date_column}. "
            f"This usually means --slice-dir points at full truth files instead of ./truth_store/daily/YYYY-MM-DD/. "
            f"Sample off-date values: {distinct_dates}"
        )
    return df



def unpivot_funnel(df: pd.DataFrame) -> pd.DataFrame:
    metric_cols = ["Impressions", "Clicks", "Visits", "Leads", "Leases"]
    stage_reverse = {v: k for k, v in FUNNEL_STAGE_MAP.items()}
    existing_metrics = [c for c in metric_cols if c in df.columns]
    id_cols = [c for c in ["DateKey", "PropertyKey", "VendorKey", "DataSource"] if c in df.columns]
    long = df[id_cols + existing_metrics].melt(
        id_vars=id_cols,
        value_vars=existing_metrics,
        var_name="MetricName",
        value_name="MetricValue",
    )
    long["FunnelStageKey"] = long["MetricName"].map(stage_reverse).astype("int8")
    long["MetricValue"] = long["MetricValue"].fillna(0).astype("int32")
    return long.drop(columns=["MetricName"])



def validate_before_publish(tables: dict[str, pd.DataFrame], is_single_day: bool = True) -> None:
    print("\n── Pre-publish validation ───────────────────────────────────")
    errors: list[str] = []
    spend = tables.get("spend")
    funnel = tables.get("funnel")
    touch = tables.get("touch")
    leasing = tables.get("leasing")
    ops = tables.get("ops")

    if spend is not None and funnel is not None:
        spend_keys = set(zip(spend["DateKey"], spend["PropertyKey"], spend["VendorKey"]))
        funnel_keys = set(zip(funnel["DateKey"], funnel["PropertyKey"], funnel["VendorKey"]))
        unmatched = funnel_keys - spend_keys
        if unmatched:
            errors.append(f"CHECK 1 FAIL: {len(unmatched):,} funnel rows have no matching spend key")
        else:
            print(f"  ✓ Check 1: All {len(funnel_keys):,} funnel rows match spend keys")

    if touch is not None:
        missing = touch[(touch["Converted"] == 1) & (touch["LeaseDateKey"].isna())]
        if len(missing) > 0:
            errors.append(f"CHECK 2 FAIL: {len(missing):,} converted touches have null LeaseDateKey")
        else:
            print("  ✓ Check 2: All converted touches have LeaseDateKey")

    if touch is not None and "AttributedCredit" in touch.columns:
        conv = touch[touch["Converted"] == 1]
        if len(conv) > 0:
            if is_single_day:
                print("  ⚠ Check 3: Skipped for one-day touch slice — full journey credit may span multiple touch dates")
            else:
                sums = conv.groupby("ProspectKey")["AttributedCredit"].sum()
                bad = sums[abs(sums - 1.0) > 0.001]
                if len(bad) > 0:
                    errors.append(f"CHECK 3 FAIL: {len(bad):,} prospects have journey credit != 1.0")
                else:
                    print(f"  ✓ Check 3: All {len(sums):,} converted journeys sum to 1.0")

    if leasing is not None:
        over = leasing[leasing["AttributedNewLeases"] > leasing["NewLeases"]]
        if len(over) > 0:
            errors.append(f"CHECK 4 FAIL: {len(over):,} rows have AttributedNewLeases > NewLeases")
        else:
            print("  ✓ Check 4: AttributedNewLeases ≤ NewLeases on all rows")

    if ops is not None and leasing is not None:
        ops_mi = ops.groupby("PropertyKey")["MoveIns"].sum()
        lease_new = leasing.groupby("PropertyKey")["NewLeases"].sum()
        combined = ops_mi.to_frame("ops").join(lease_new.to_frame("lease"), how="inner")
        if len(combined) > 0:
            var = abs(combined["ops"] - combined["lease"]) / combined["lease"].replace(0, float("nan"))
            pct_high = float((var > 0.05).sum() / len(combined))
            if pct_high > 0.10:
                print(f"  ⚠ Check 5: {pct_high:.1%} of properties have >5% ops/leasing variance (warning only)")
            else:
                print(f"  ✓ Check 5: Ops MoveIns ≈ Leasing NewLeases on {100 * (1 - pct_high):.0f}% of properties")

    if errors:
        for err in errors:
            print(f"  ✗ {err}")
        raise ValueError(f"Pre-publish validation failed with {len(errors)} error(s).")
    print("  All checks passed. Safe to write to SQL.")



def prepare_df(df: pd.DataFrame, column_map: dict[str, str | None]) -> pd.DataFrame:
    rename: dict[str, str] = {}
    select: list[str] = []
    for parquet_col, sql_col in column_map.items():
        if parquet_col in df.columns:
            select.append(parquet_col)
            if sql_col is not None:
                rename[parquet_col] = sql_col
    result = df[select].copy()
    if rename:
        result = result.rename(columns=rename)
    return result



def delete_run_date_rows(engine, sql_table: str, sql_delete_column: str, date_key: int) -> int:
    """
    Delete rows for the given date_key — SCOPED TO DataSource = 1 (canonical).
    Other DataSources (CRM=2, Spend=3, Ops=4) are independent pipelines and
    must not be touched by canonical re-publishes. Without this scope, a
    canonical re-run of any date wipes downstream pipeline rows for that
    date — see RunId=24/25 incident on 2026-05-01.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {sql_table} "
                 f"WHERE DataSource = :ds AND {sql_delete_column} = :dk"),
            {"ds": DATASOURCE, "dk": date_key},
        )
        return int(result.rowcount or 0)



def log_pipeline_run(engine, run_date: date, date_key: int, status: str, row_counts: dict[str, int], table_filter: str | None = None) -> None:
    """
    Best-effort run logging. Expects a table like dbo.pipeline_run_log with columns:
      RunDate (date), DateKey (int), Status (varchar), TableFilter (varchar, nullable),
      RowCountsJson (nvarchar(max)), LoggedAtUtc (datetime2 default SYSUTCDATETIME())
    If the table does not exist yet, logging is skipped without failing the publish.
    """
    import json

    payload = json.dumps(row_counts, sort_keys=True)
    stmt = text("""
        INSERT INTO dbo.pipeline_run_log (RunDate, DateKey, Status, TableFilter, RowCountsJson)
        VALUES (:run_date, :date_key, :status, :table_filter, :row_counts_json)
    """)
    try:
        with engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "run_date": run_date.isoformat(),
                    "date_key": date_key,
                    "status": status,
                    "table_filter": table_filter,
                    "row_counts_json": payload,
                },
            )
        print("  Logged run to dbo.pipeline_run_log")
    except Exception as exc:
        print(f"  ⚠ Run log skipped: {exc}")


def write_df_to_sql(engine, df: pd.DataFrame, sql_table: str) -> int:
    schema, table_name = sql_table.split(".", 1)
    total = 0
    start = time.time()
    total_chunks = max(1, (len(df) + CHUNK_SIZE - 1) // CHUNK_SIZE)
    for i, chunk_start in enumerate(range(0, len(df), CHUNK_SIZE), 1):
        chunk = df.iloc[chunk_start: chunk_start + CHUNK_SIZE]
        with engine.begin() as conn:
            chunk.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists="append",
                index=False,
                method=None,
                chunksize=CHUNK_SIZE,
            )
        total += len(chunk)
        pct = total / max(len(df), 1) * 100
        elapsed = time.time() - start
        print(f"    chunk {i}/{total_chunks} — {total:>10,} / {len(df):,} rows ({pct:.0f}%) — {elapsed:.0f}s")
    return total



def cast_nullable_int_columns(df: pd.DataFrame) -> pd.DataFrame:
    int_cols = [
        "DateKey", "PropertyKey", "VendorKey", "ChannelKey", "FunnelStageKey", "MetricValue", "DataSource",
        "TouchNumber", "TotalTouches", "DaysBeforeLease", "Converted", "IsDirectCredit", "IsAssistedCredit",
        "Leads", "NewLeases", "Visits", "AttributedNewLeases", "UnattributedLeases",
        "OccupiedUnits", "VacantUnits", "AvailableUnits", "MoveIns", "MoveOuts", "LeaseExpirations",
        "ScheduledMoveIns", "ProspectKey", "LeaseDateKey",
    ]
    for col in int_cols:
        if col in df.columns:
            try:
                df[col] = df[col].astype("Int64")
            except (TypeError, ValueError):
                pass
    return df



def publish_table(engine, plan: dict, df_raw: pd.DataFrame, date_key: int, dry_run: bool = False) -> dict[str, object]:
    sql_table = plan["sql_table"]
    if plan["transform"] == "unpivot_funnel":
        df = unpivot_funnel(df_raw)
    else:
        df = df_raw.copy()

    df = prepare_df(df, plan["columns"])
    df["DataSource"] = DATASOURCE
    df = df.where(pd.notnull(df), None)
    df = cast_nullable_int_columns(df)

    print(f"\n── Publishing {sql_table} ─────────────────────────────────")
    print(f"  Run date      : {date_key}")
    print(f"  Rows to write : {len(df):,}")
    print(f"  Columns       : {list(df.columns)}")

    if dry_run:
        print("  [DRY RUN] Skipping DELETE + INSERT")
        return {"table": sql_table, "deleted": 0, "written": 0, "dry_run": True}

    deleted = delete_run_date_rows(engine, sql_table, plan["sql_delete_column"], date_key)
    print(f"  Deleted: {deleted:,} existing DataSource=1 rows for {date_key}")
    written = write_df_to_sql(engine, df, sql_table)
    print(f"  Written: {written:,} rows to {sql_table}")
    return {"table": sql_table, "deleted": deleted, "written": written, "dry_run": False}



def main() -> None:
    args = parse_args()
    run_date, date_key = parse_run_date(args.date)
    slice_dir = Path(args.slice_dir) if args.slice_dir else DEFAULT_DAILY_ROOT / run_date.isoformat()

    print("=" * 65)
    print("NorthStar Incremental Daily Truth Publisher")
    print(f"Slice dir: {slice_dir.resolve()}")
    print(f"Run date : {run_date.isoformat()} ({date_key})")
    print(f"Mode     : {'VALIDATE ONLY' if args.validate_only else 'DRY RUN' if args.dry_run else 'LIVE WRITE'}")
    print("=" * 65)

    tables: dict[str, pd.DataFrame] = {}
    print("\n── Loading daily slice parquet files ─────────────────────────")
    for plan in PUBLISH_PLAN:
        if args.table and plan["key"] != args.table:
            continue
        df = load_parquet(slice_dir, plan["parquet"])
        tables[plan["key"]] = enforce_single_day(df, plan["slice_date_column"], date_key, plan["key"])

    # validation dependencies
    if args.table and "spend" not in tables:
        tables["spend"] = enforce_single_day(load_parquet(slice_dir, "canonical_spend_truth"), "DateKey", date_key, "spend")
    if args.table and "funnel" not in tables:
        tables["funnel"] = enforce_single_day(load_parquet(slice_dir, "canonical_funnel_truth"), "DateKey", date_key, "funnel")
    if args.table and "touch" not in tables:
        tables["touch"] = enforce_single_day(load_parquet(slice_dir, "canonical_touch_truth"), "TouchDateKey", date_key, "touch")
    if args.table and "leasing" not in tables:
        tables["leasing"] = enforce_single_day(load_parquet(slice_dir, "canonical_leasing_truth"), "LeaseDateKey", date_key, "leasing")
    if args.table and "ops" not in tables:
        tables["ops"] = enforce_single_day(load_parquet(slice_dir, "canonical_ops_truth"), "DateKey", date_key, "ops")

    validate_before_publish(tables, is_single_day=True)

    if args.validate_only:
        print("\nValidate-only mode — no SQL writes performed.")
        return

    engine = None
    if not args.dry_run:
        print("\n── Connecting to Azure SQL ───────────────────────────────────")
        engine = get_engine()
        # connect_with_retry handles Azure SQL Serverless auto-pause wakeup
        # by retrying on OperationalError with exponential backoff (5s,15s,30s,60s)
        with connect_with_retry(engine) as conn:
            db_name = conn.execute(text("SELECT DB_NAME()")).scalar_one()
        print(f"  Connected to: {db_name}")

    results: list[dict[str, object]] = []
    start_total = time.time()
    for plan in PUBLISH_PLAN:
        if args.table and plan["key"] != args.table:
            print(f"\n── Skipping {plan['sql_table']} (filter: --table {args.table})")
            continue
        if plan["key"] not in tables:
            continue
        result = publish_table(engine, plan, tables[plan["key"]], date_key, dry_run=args.dry_run)
        results.append(result)

    if not args.dry_run and engine is not None:
        print("\n── Post-write validation ────────────────────────────────────")
        expected = {r["table"]: r["written"] for r in results if not r["dry_run"]}
        mismatches: list[str] = []
        for plan in PUBLISH_PLAN:
            if args.table and plan["key"] != args.table:
                continue
            sql_table = plan["sql_table"]
            with connect_with_retry(engine) as conn:
                count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {sql_table} WHERE DataSource = :ds AND {plan['sql_delete_column']} = :dk"),
                    {"ds": DATASOURCE, "dk": date_key},
                ).scalar_one()
            exp = expected.get(sql_table, 0)
            match = "✓" if count == exp else "⚠ MISMATCH"
            print(f"  {match} {sql_table:<42} {count:>10,} rows (expected {exp:,})")
            if count != exp:
                mismatches.append(f"{sql_table}: wrote {exp:,}, found {count:,}")
        if mismatches:
            print("\n  ⚠ Row count mismatches detected:")
            for item in mismatches:
                print(f"    {item}")

        # Write run log — best-effort, won't fail publish if table doesn't exist yet
        row_counts = {r["table"]: r["written"] for r in results if not r["dry_run"]}
        status = "ROW_COUNT_MISMATCH" if mismatches else "SUCCESS"
        log_pipeline_run(engine, run_date, date_key, status, row_counts, args.table)

        engine.dispose()

    elapsed = time.time() - start_total
    print("\n" + "=" * 65)
    print("INCREMENTAL PUBLISH COMPLETE" if not args.dry_run else "DRY RUN COMPLETE")
    print("=" * 65)
    for result in results:
        mode = "DRY RUN" if result["dry_run"] else "WRITTEN"
        print(f"  {result['table']:<45} {result['written']:>10,} rows [{mode}]")
    print(f"\n  Total time: {elapsed:.0f}s")


if __name__ == "__main__":
    try:
        main()
    except Exception as _exc:
        # Best-effort failure logging: write a FAILED row to
        # pipeline_run_log so failures leave a SQL trace, not just
        # Function App logs. If this itself fails, the original
        # exception is still re-raised below.
        import sys as _sys
        print(f"\n[X] Pipeline failed: {type(_exc).__name__}: {_exc}",
              file=_sys.stderr)
        try:
            _args = parse_args()
            _run_date, _date_key = parse_run_date(_args.date)
            _eng = get_engine()
            log_pipeline_run(
                _eng, _run_date, _date_key,
                status="FAILED",
                row_counts={"_error": str(_exc)[:500]},
                table_filter=_args.table,
            )
            _eng.dispose()
            print(f"  Logged FAILED row to dbo.pipeline_run_log",
                  file=_sys.stderr)
        except Exception as _log_exc:
            print(f"  Could not write FAILED row: {_log_exc}",
                  file=_sys.stderr)
        raise  # re-raise so Function App still reports the failure
