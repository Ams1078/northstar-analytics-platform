# 04 Reconciliation

The layer the rest of the platform exists to support.

Each source pipeline is compared against canonical truth, and every disagreement
becomes an action with a type, a severity and a status. Nothing is silently
corrected. The operations dashboard reads this output directly, which is how a
spend divergence becomes a queued decision rather than a number nobody questions.

### `01_reconcile_spend_divergence.sql`

Compares vendor spend against canonical truth and writes divergence actions.

*The platform's core idea in one statement: two systems disagree, the disagreement is measured, graded and queued for a person. It is not averaged away.*

### `02_reconcile_occupancy_variance.sql`

Detects occupancy differences beyond tolerance.

*A tolerance band rather than an equality test, because two systems recording the same building will never agree to the row.*

### `03_reconcile_attribution_coverage.sql`

Measures leases carrying no marketing trail.

*Turns an absence into a measured KPI. Possible only because canonical truth is held beside the CRM reconstruction.*

### `04_write_action.sql`

Writes a reconciliation action with type, severity and status.

*Every detector converges on this one shape, which is why the dashboard queue can filter across sources that share nothing else.*

### `05_clear_actions_for_rerun.sql`

Removes prior actions for a date before recomputation.

*Same idempotency contract as the rest of the platform, applied to derived output.*

### `06_ops_canonical_snapshot.sql`

Reads the canonical operations position for comparison.

*The DS=1 side of the occupancy check. Short, but it is the anchor the whole comparison hangs from.*

### `07_prospect_journey_lookup.sql`

Retrieves reconstructed prospect journeys.

*Feeds attribution scoring. The journey is rebuilt once and read many times rather than recomputed per model.*
