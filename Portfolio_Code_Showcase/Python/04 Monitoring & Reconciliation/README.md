# 04 Monitoring & Reconciliation

The platform checking itself. These modules produce the numbers behind the operations dashboard.

### `compute_reconciliation_actions.py`

Compares each source pipeline against canonical truth and writes typed, severity-graded actions.

*The idea the platform is built on: a disagreement between systems is a finding with an owner, not noise to be averaged away. Spend divergence, occupancy variance, attribution coverage gaps and duplicate emails each become a queued action rather than a silent correction.*

### `extract_canonical_truth_daily.py`

Extracts a single day of canonical truth for downstream comparison.

*Small and deliberately included. It is the seam that makes reconciliation possible, because the same day can be re-extracted and re-compared at any time.*
