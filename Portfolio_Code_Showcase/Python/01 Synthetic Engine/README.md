# 01 Synthetic Engine

The platform has no upstream vendor. These modules manufacture one, then degrade it on purpose so the downstream pipeline has something real to fail against.

### `canonical_truth_generator.py`

Generates the canonical business: 120 properties, 4 regions, 12 markets, 11 vendors, daily from 2024 to 2027.

*The heart of the platform. Performance tiers, seasonality, migration cycles and scripted market events make the data behave rather than merely exist. Attribution credit is assigned per prospect and sums to 1.0, which is what makes the Attribution Lab measurable instead of decorative.*

### `bronze_source_generator.py`

Emits 14 bronze files a night across 7 vendor formats, plus Yardi and six Salesforce tables.

*Each file imitates a real platform's native export: Google Ads writes banner rows above its header and currency as text, Apartment List uses snake_case, one Meta file resolves to two vendor keys. Property identity arrives under five different column names. This is the file that makes the integration problem real.*

### `crm_defect_injector.py`

Injects deliberate data quality defects into the CRM extracts.

*Malformed phone numbers, invalid emails, inconsistent casing. Without defects the reconciliation layer has nothing to detect, and a clean-data demo proves nothing.*
