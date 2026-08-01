# 03 ETL and Publishing

Publication into the warehouse, and the identity resolution that has to happen first.

Seven vendor formats name the same property seven different ways and bill on four
different cost models. These statements are where they converge on one dimensional
key at a grain where both sides mean the same thing.

### `01_merge_canonical_truth.sql`

Merges staged canonical truth into the warehouse fact.

*The largest statement in the platform and the one worth reading closely. Staged first, then merged on the natural key, so the published table is never left half-written.*

### `02_merge_dimension.sql`

Upserts dimension rows.

*Matched, not-matched and source-missing handled explicitly. Nothing is deleted implicitly.*

### `03_spend_at_comparable_grain.sql`

Aggregates spend to the grain the source file can actually be compared at.

*The hard part of vendor integration. A multi-vendor file cannot be anchored to a single VendorKey, so the comparison is lifted to a grain where both sides mean the same thing.*

### `04_leasing_for_attribution.sql`

Reads leasing facts for attribution reconstruction.

*Note the DataSource filter. Any query here that omitted it would blend canonical truth with the CRM rebuild and return a number that means nothing.*

### `05_property_key_resolution.sql`

Resolves incoming property identifiers to the dimension key.

*Seven vendor formats name the property seven ways. This is where they converge on one key.*
