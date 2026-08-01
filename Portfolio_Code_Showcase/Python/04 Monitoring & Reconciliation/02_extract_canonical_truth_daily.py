"""
extract_canonical_truth_daily.py
================================
NorthStar MAA — Daily Canonical Truth Extractor

Purpose:
    Create a one-day canonical truth slice from the current full or partially
    filtered truth parquet files. This is the handoff layer between the truth
    store and the daily silver SQL publisher.

What it does:
    - reads canonical parquet files from ./truth_store (or a supplied source dir)
    - filters each dataset to one requested run date
    - writes the single-day files to ./truth_store/daily/YYYY-MM-DD/
    - writes a manifest.json with row counts and paths

This script does NOT write to SQL.

Usage:
    python extract_canonical_truth_daily.py --date 2026-04-12
    python extract_canonical_truth_daily.py --date 2026-04-12 --source-dir ./truth_store
    python extract_canonical_truth_daily.py --date 2026-04-12 --output-root ./truth_store/daily
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

TRUTH_DIR = Path(os.environ.get("TRUTH_DIR", "/tmp/truth_store"))
SOURCE_DEFAULT = TRUTH_DIR
OUTPUT_ROOT_DEFAULT = TRUTH_DIR / "daily"


@dataclass(frozen=True)
class ExtractPlan:
    parquet: str
    key: str
    date_column: str


EXTRACT_PLAN = [
    ExtractPlan("canonical_spend_truth", "spend", "DateKey"),
    ExtractPlan("canonical_funnel_truth", "funnel", "DateKey"),
    ExtractPlan("canonical_touch_truth", "touch", "TouchDateKey"),
    ExtractPlan("canonical_prospect_truth", "prospect", "LeadDateKey"),
    ExtractPlan("canonical_campaign_truth", "campaign", "StartDateKey"),
    ExtractPlan("canonical_leasing_truth", "leasing", "LeaseDateKey"),
    ExtractPlan("canonical_ops_truth", "ops", "DateKey"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract a one-day canonical truth slice.")
    parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD format")
    parser.add_argument("--source-dir", default=str(SOURCE_DEFAULT), help="Directory containing canonical parquet files")
    parser.add_argument("--output-root", default=str(OUTPUT_ROOT_DEFAULT), help="Root directory for daily extracted files")
    return parser.parse_args()



def parse_run_date(raw: str) -> tuple[date, int, int]:
    run_date = date.fromisoformat(raw)
    date_key = int(run_date.strftime("%Y%m%d"))
    month_key = int(run_date.strftime("%Y%m"))
    return run_date, date_key, month_key



def load_parquet(source_dir: Path, parquet_name: str) -> pd.DataFrame:
    path = source_dir / f"{parquet_name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet file: {path}")
    df = pd.read_parquet(path, engine="pyarrow")
    print(f"  Loaded {path.name:<35} {len(df):>10,} rows")
    return df



def filter_df(plan: ExtractPlan, df: pd.DataFrame, date_key: int, month_key: int) -> pd.DataFrame:
    if plan.date_column not in df.columns:
        raise KeyError(f"{plan.parquet} missing expected date column: {plan.date_column}")

    # Campaign truth is month-window based, so keep active overlapping campaigns.
    if plan.key == "campaign":
        if "StartDateKey" in df.columns and "EndDateKey" in df.columns:
            filtered = df[(df["EndDateKey"] >= date_key) & (df["StartDateKey"] <= date_key)].copy()
        else:
            filtered = df[(df[plan.date_column] // 100) == month_key].copy()
        return filtered.reset_index(drop=True)

    filtered = df[df[plan.date_column].astype("Int64") == date_key].copy()
    return filtered.reset_index(drop=True)



def write_slice(output_dir: Path, parquet_name: str, df: pd.DataFrame) -> Path:
    output_path = output_dir / f"{parquet_name}.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")
    return output_path



def main() -> None:
    args = parse_args()
    run_date, date_key, month_key = parse_run_date(args.date)
    source_dir = Path(args.source_dir)
    output_dir = Path(args.output_root) / run_date.isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("NorthStar Daily Canonical Truth Extractor")
    print(f"Source dir : {source_dir.resolve()}")
    print(f"Output dir : {output_dir.resolve()}")
    print(f"Run date   : {run_date.isoformat()} ({date_key})")
    print("=" * 70)

    manifest: dict[str, object] = {
        "run_date": run_date.isoformat(),
        "date_key": date_key,
        "month_key": month_key,
        "source_dir": str(source_dir.resolve()),
        "output_dir": str(output_dir.resolve()),
        "tables": {},
    }

    for plan in EXTRACT_PLAN:
        df = load_parquet(source_dir, plan.parquet)
        filtered = filter_df(plan, df, date_key, month_key)
        output_path = write_slice(output_dir, plan.parquet, filtered)
        manifest["tables"][plan.key] = {
            "parquet": plan.parquet,
            "date_column": plan.date_column,
            "rows": int(len(filtered)),
            "path": str(output_path.resolve()),
        }
        print(f"  Wrote  {plan.parquet:<35} {len(filtered):>10,} rows")

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print("-" * 70)
    print(f"Manifest written: {manifest_path.resolve()}")
    print("Daily extract complete.")


if __name__ == "__main__":
    main()
