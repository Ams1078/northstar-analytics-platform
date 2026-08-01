import logging
import azure.functions as func
from datetime import datetime, timedelta
import subprocess
import sys
import os

app = func.FunctionApp()

def run_step(step_name, command):
    logging.info(f"Running {step_name}: {' '.join(command)}")
    logging.info(f"Working directory: {os.getcwd()}")
    logging.info(f"Files in cwd: {os.listdir(os.getcwd())}")

    env = os.environ.copy()

    function_site_packages = "/home/site/wwwroot/.python_packages/lib/site-packages"
    existing_pythonpath = env.get("PYTHONPATH", "")

    if existing_pythonpath:
        env["PYTHONPATH"] = f"{function_site_packages}:{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = function_site_packages

    env["MAA_OUTPUT_DIR"] = "/tmp/maa_generated_data"
    env["TRUTH_DIR"] = "/tmp/truth_store"

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        cwd=os.getcwd(),
        env=env
    )

    logging.info(f"{step_name} stdout:\n{result.stdout}")
    logging.error(f"{step_name} stderr:\n{result.stderr}")

    if result.returncode != 0:
        logging.error(f"{step_name} failed. STDERR:\n{result.stderr}")
        raise RuntimeError(f"{step_name} failed with exit code {result.returncode}")

@app.timer_trigger(schedule="0 0 2 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def daily_canonical_pipeline(myTimer: func.TimerRequest) -> None:
    run_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    logging.info("=====================================")
    logging.info("DAILY CANONICAL PIPELINE")
    logging.info(f"Run Date: {run_date}")
    logging.info("=====================================")

    try:
        run_step(
            "Step 1 build canonical truth",
            [sys.executable, "build_canonical_truth_v4_final.py", "--date", run_date]
        )

        run_step(
            "Step 2 extract daily slice",
            [sys.executable, "extract_canonical_truth_daily.py", "--date", run_date]
        )

        run_step(
            "Step 3 publish incremental to SQL",
            [sys.executable, "publish_canonical_truth_incremental_to_sql.py", "--date", run_date]
        )

        logging.info("Pipeline completed successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# DAILY SPEND PIPELINE — runs at 3AM UTC, after canonical (2AM)
# ══════════════════════════════════════════════════════════════════════════════
#
# Reads bronze sources from Azure Blob (bronze/spend/{YYYY-MM-DD}/), writes
# to gold tables at DataSource=3, populates pipeline_runs/flags/quarantine,
# and runs SPEND_CONFLICT detection against canonical (DataSource=1).
#
# Why 3AM and not 2AM:
#   - Canonical writes truth at DataSource=1 starting at 2AM
#   - Spend's conflict detection compares its DS=3 writes to canonical's
#     DS=1 writes. If Spend ran first, conflict detection would compare
#     against yesterday's canonical truth (still valid, but less ideal).
#   - 3AM gives canonical ~1 hour to finish. Canonical typically completes
#     in <10 min, so the gap is comfortable.
#
# Why subprocess:
#   - Same pattern as daily_canonical_pipeline (consistency)
#   - Isolates spend_pipeline state from the Function App process
#   - Lets us reuse spend_pipeline.py's existing CLI unchanged

@app.timer_trigger(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def daily_spend_pipeline(myTimer: func.TimerRequest) -> None:
    from datetime import date

    # Yesterday in UTC — the date we're processing
    run_date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_date_obj = date.fromisoformat(run_date_str)

    logging.info("=====================================")
    logging.info("DAILY SPEND PIPELINE")
    logging.info(f"Run Date: {run_date_str}")
    logging.info("=====================================")

    try:
        # Stage bronze blobs to local /tmp so spend_pipeline can read them
        # via its existing local-file path. stage_bronze_to_tmp returns the
        # parent of the dated subfolder, which is what SOURCE_PATH expects.
        from pipeline_utils import stage_bronze_to_tmp

        logging.info("Staging bronze sources for date %s...", run_date_str)
        tmp_root = stage_bronze_to_tmp("spend", run_date_obj)
        os.environ["SOURCE_PATH"] = tmp_root
        logging.info("SOURCE_PATH set to staged dir: %s", tmp_root)

        # Run the spend pipeline as a subprocess. Inherits SOURCE_PATH from
        # this process's env, so resolve_source_dir() finds tmp_root/{date}/
        run_step(
            "Daily spend pipeline",
            [sys.executable, "spend_pipeline.py", "--date", run_date_str]
        )

        logging.info("Spend pipeline completed successfully")

    except FileNotFoundError as e:
        # Bronze had no files for this date. Log and exit cleanly without
        # raising — Azure Functions treats unraised exits as success and
        # the pipeline didn't actually fail; there was just nothing to do.
        # When the bronze layer is empty for a date, the next 3AM run
        # will retry naturally.
        logging.warning("No bronze sources found for %s — skipping run: %s",
                        run_date_str, str(e))

    except Exception as e:
        logging.error(f"Spend pipeline failed: {str(e)}")
        raise

# ══════════════════════════════════════════════════════════════════════════════
# DAILY OPS PIPELINE — runs at 2:30 AM UTC, between canonical (2 AM) and spend (3 AM)
# ══════════════════════════════════════════════════════════════════════════════
#
# Reads bronze sources from Azure Blob (bronze/ops/{YYYY-MM-DD}/), writes
# property-level operational metrics to dbo.fact_property_ops_daily at
# DataSource=4, sitting alongside DataSource=1 (canonical truth) for the
# same date so the reconciliation control plane can compare them.
#
# Why 2:30AM:
#   - 2:00AM canonical writes truth at DataSource=1
#   - 2:30AM ops writes Yardi at DataSource=4 (this)
#   - 3:00AM spend writes vendor data at DataSource=3
#   - Spread out so any single pipeline failure is visible in isolation
#     and downstream reconciliation has all DataSources to compare against
#
# Same subprocess pattern as canonical/spend — isolated process, reused CLI.

@app.timer_trigger(schedule="0 30 2 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def daily_ops_pipeline(myTimer: func.TimerRequest) -> None:
    from datetime import date

    # Yesterday in UTC — the date we're processing
    run_date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_date_obj = date.fromisoformat(run_date_str)

    logging.info("=====================================")
    logging.info("DAILY OPS PIPELINE")
    logging.info(f"Run Date: {run_date_str}")
    logging.info("=====================================")

    try:
        # Stage bronze blobs to local /tmp so ops_pipeline can read them
        # via its existing local-file path. stage_bronze_to_tmp returns the
        # parent of the dated subfolder, which is what SOURCE_PATH expects.
        from pipeline_utils import stage_bronze_to_tmp

        logging.info("Staging bronze sources for date %s...", run_date_str)
        tmp_root = stage_bronze_to_tmp("ops", run_date_obj)
        os.environ["SOURCE_PATH"] = tmp_root
        logging.info("SOURCE_PATH set to staged dir: %s", tmp_root)

        # Run the ops pipeline as a subprocess. Inherits SOURCE_PATH from
        # this process's env, so resolve_source_dir() finds tmp_root/{date}/
        run_step(
            "Daily ops pipeline",
            [sys.executable, "ops_pipeline.py", "--date", run_date_str]
        )

        logging.info("Ops pipeline completed successfully")

    except FileNotFoundError as e:
        # Bronze had no files for this date. Log and exit cleanly without
        # raising — Azure Functions treats unraised exits as success and
        # the pipeline didn't actually fail; there was just nothing to do.
        # When the bronze layer is empty for a date, the next 2:30AM run
        # will retry naturally.
        logging.warning("No bronze sources found for %s — skipping run: %s",
                        run_date_str, str(e))

    except Exception as e:
        logging.error(f"Ops pipeline failed: {str(e)}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# DAILY CRM PIPELINE — runs at 4 AM UTC, after canonical/ops/spend
# ══════════════════════════════════════════════════════════════════════════════
#
# Reads bronze sources from Azure Blob (bronze/crm/{YYYY-MM-DD}/), writes
# to gold tables at DataSource=2 (fact_leasing_daily, fact_prospect_journey),
# and runs the full 12-step CRM pipeline including dirty data detection,
# dedup, attribution, and quarantine.
#
# Why 4AM:
#   - All three upstream pipelines (canonical 2AM, ops 2:30AM, spend 3AM)
#     have completed by 4AM, leaving the system quiet
#   - CRM is the heaviest pipeline (12 steps, 2,700+ leads/day, attribution
#     window comparisons), so it runs last when nothing else is using SQL
#   - Cross-source attribution analysis (DS=2 vs DS=1) needs canonical
#     truth already written for the same date
#
# Same subprocess pattern as canonical/spend/ops — isolated process, reused CLI.

@app.timer_trigger(schedule="0 0 4 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def daily_crm_pipeline(myTimer: func.TimerRequest) -> None:
    from datetime import date

    # Yesterday in UTC — the date we're processing
    run_date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_date_obj = date.fromisoformat(run_date_str)

    logging.info("=====================================")
    logging.info("DAILY CRM PIPELINE")
    logging.info(f"Run Date: {run_date_str}")
    logging.info("=====================================")

    try:
        # Stage bronze blobs to local /tmp so crm_pipeline can read them
        # via its existing local-file path. stage_bronze_to_tmp returns the
        # parent of the dated subfolder, which is what SOURCE_PATH expects.
        from pipeline_utils import stage_bronze_to_tmp

        logging.info("Staging bronze sources for date %s...", run_date_str)
        tmp_root = stage_bronze_to_tmp("crm", run_date_obj)
        os.environ["SOURCE_PATH"] = tmp_root
        logging.info("SOURCE_PATH set to staged dir: %s", tmp_root)

        # Run the crm pipeline as a subprocess. Inherits SOURCE_PATH from
        # this process's env, so resolve_source_dir() finds tmp_root/{date}/
        run_step(
            "Daily crm pipeline",
            [sys.executable, "crm_pipeline.py", "--date", run_date_str]
        )

        logging.info("CRM pipeline completed successfully")

    except FileNotFoundError as e:
        # Bronze had no files for this date. Log and exit cleanly without
        # raising — Azure Functions treats unraised exits as success and
        # the pipeline didn't actually fail; there was just nothing to do.
        # When the bronze layer is empty for a date, the next 4AM run
        # will retry naturally.
        logging.warning("No bronze sources found for %s — skipping run: %s",
                        run_date_str, str(e))

    except Exception as e:
        logging.error(f"CRM pipeline failed: {str(e)}")
        raise
