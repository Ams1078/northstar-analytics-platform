# 01 Run Ledger

Every pipeline run opens a row, closes it with counters, and advances a watermark
only on success. This is what makes a night auditable after the fact.

The ordering matters more than any single statement: the watermark moves last, so a
crash mid-load leaves it unchanged and the next run reprocesses rather than skips.

### `01_start_run.sql`

Opens a run row with status RUNNING and returns its identity.

*OUTPUT INSERTED to get the key back in one round trip. Every downstream write is keyed to this RunId, which is what makes a night auditable.*

### `02_finish_run.sql`

Closes the run with status, duration and five row counters.

*Duration is computed in SQL rather than Python, so it is measured against one clock. Extracted, cleansed, loaded, flagged and quarantined are stored separately because a run that extracts 7,957 rows and loads 180 is doing something worth seeing.*

### `03_read_watermark.sql`

Reads the last successful run for a pipeline.

*Incremental processing starts here. The watermark is per pipeline, not global, so one slow source never holds back the others.*

### `04_advance_watermark.sql`

Advances the watermark only after a successful load.

*Ordering is the point: the watermark moves last. A crash mid-load leaves it unchanged, so the next run reprocesses rather than skips.*
