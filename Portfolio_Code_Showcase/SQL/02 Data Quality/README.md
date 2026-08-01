# 02 Data Quality

Rejected rows are set aside with a reason rather than dropped, and problems are
flagged with a severity assigned by the rule that found them.

Processed and quarantined rows are written to separate tables whose sum reconciles
to extracted. That reconciliation is the check that catches a silent drop.

### `01_quarantine_row.sql`

Writes a rejected row to quarantine with its reason.

*Bad rows are set aside with a cause, never dropped. The row survives, so a defect can be investigated instead of inferred from a count that does not add up.*

### `02_raise_flag.sql`

Raises a typed, severity-graded flag against a run.

*Severity is assigned at write time by the rule that detected the problem, not later by a human triaging a list.*

### `03_record_processed.sql`

Records rows that passed cleansing.

*Processed and quarantined are written to separate tables. Their sum reconciles to extracted, which is the check that catches a silent drop.*

### `04_clear_quarantine_for_rerun.sql`

Clears quarantine for one date and source before a re-run.

*The idempotency pattern in miniature: scoped by both date and source, so re-running a night reproduces it rather than doubling it.*
