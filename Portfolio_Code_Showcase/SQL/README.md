# SQL

How the warehouse is operated. These statements are extracted verbatim from the
Python modules that execute them, so what is here is what actually runs against
Azure SQL each night.

Two rules govern almost everything in this folder.

**Every fact carries a `DataSource` column.** Canonical truth is `DS=1`; the CRM,
spend and operations pipelines write `DS=2`, `3` and `4`. A query that does not filter
on it blends two versions of reality and returns a number that means nothing.

**Every write is idempotent by date and source.** Deletes are scoped to both, so
re-running a night reproduces it rather than doubling it, and one pipeline can never
erase another's rows.

### 01 Run Ledger

- **`01_start_run.sql`** — Opens a run row with status RUNNING and returns its identity. OUTPUT INSERTED to get the key back in one round trip. Every downstream write is keyed to this RunId, which is what makes a night auditable.
- **`02_finish_run.sql`** — Closes the run with status, duration and five row counters. Duration is computed in SQL rather than Python, so it is measured against one clock. Extracted, cleansed, loaded, flagged and quarantined are stored separately because a run that extracts 7,957 rows and loads 180 is doing something worth seeing.
- **`03_read_watermark.sql`** — Reads the last successful run for a pipeline. Incremental processing starts here. The watermark is per pipeline, not global, so one slow source never holds back the others.
- **`04_advance_watermark.sql`** — Advances the watermark only after a successful load. Ordering is the point: the watermark moves last. A crash mid-load leaves it unchanged, so the next run reprocesses rather than skips.

### 02 Data Quality

- **`01_quarantine_row.sql`** — Writes a rejected row to quarantine with its reason. Bad rows are set aside with a cause, never dropped. The row survives, so a defect can be investigated instead of inferred from a count that does not add up.
- **`02_raise_flag.sql`** — Raises a typed, severity-graded flag against a run. Severity is assigned at write time by the rule that detected the problem, not later by a human triaging a list.
- **`03_record_processed.sql`** — Records rows that passed cleansing. Processed and quarantined are written to separate tables. Their sum reconciles to extracted, which is the check that catches a silent drop.
- **`04_clear_quarantine_for_rerun.sql`** — Clears quarantine for one date and source before a re-run. The idempotency pattern in miniature: scoped by both date and source, so re-running a night reproduces it rather than doubling it.

### 03 ETL and Publishing

- **`01_merge_canonical_truth.sql`** — Merges staged canonical truth into the warehouse fact. The largest statement in the platform and the one worth reading closely. Staged first, then merged on the natural key, so the published table is never left half-written.
- **`02_merge_dimension.sql`** — Upserts dimension rows. Matched, not-matched and source-missing handled explicitly. Nothing is deleted implicitly.
- **`03_spend_at_comparable_grain.sql`** — Aggregates spend to the grain the source file can actually be compared at. The hard part of vendor integration. A multi-vendor file cannot be anchored to a single VendorKey, so the comparison is lifted to a grain where both sides mean the same thing.
- **`04_leasing_for_attribution.sql`** — Reads leasing facts for attribution reconstruction. Note the DataSource filter. Any query here that omitted it would blend canonical truth with the CRM rebuild and return a number that means nothing.
- **`05_property_key_resolution.sql`** — Resolves incoming property identifiers to the dimension key. Seven vendor formats name the property seven ways. This is where they converge on one key.

### 04 Reconciliation

- **`01_reconcile_spend_divergence.sql`** — Compares vendor spend against canonical truth and writes divergence actions. The platform's core idea in one statement: two systems disagree, the disagreement is measured, graded and queued for a person. It is not averaged away.
- **`02_reconcile_occupancy_variance.sql`** — Detects occupancy differences beyond tolerance. A tolerance band rather than an equality test, because two systems recording the same building will never agree to the row.
- **`03_reconcile_attribution_coverage.sql`** — Measures leases carrying no marketing trail. Turns an absence into a measured KPI. Possible only because canonical truth is held beside the CRM reconstruction.
- **`04_write_action.sql`** — Writes a reconciliation action with type, severity and status. Every detector converges on this one shape, which is why the dashboard queue can filter across sources that share nothing else.
- **`05_clear_actions_for_rerun.sql`** — Removes prior actions for a date before recomputation. Same idempotency contract as the rest of the platform, applied to derived output.
- **`06_ops_canonical_snapshot.sql`** — Reads the canonical operations position for comparison. The DS=1 side of the occupancy check. Short, but it is the anchor the whole comparison hangs from.
- **`07_prospect_journey_lookup.sql`** — Retrieves reconstructed prospect journeys. Feeds attribution scoring. The journey is rebuilt once and read many times rather than recomputed per model.
