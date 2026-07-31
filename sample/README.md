# Bronze Source File Samples

One day of source files as they arrive in the bronze layer, before any transformation.

Each file is truncated to 12 data rows. A full production day is larger: 360 rows from Google Ads, 2,700 from Salesforce leads, 240 from the DSPs. Everything else about them is untouched, including the column order, the banner rows, the formatting quirks, and the inconsistencies between platforms.

These were produced by `mock_source_generator.py` for run date 2026-04-09 and land at `maatruthlake/bronze/{source}/{YYYY-MM-DD}/`.

## Why the formats differ

Each file imitates the native export of the platform it represents. That is the point. Marketing data integration is difficult not because any single format is hard, but because no two vendors agree on anything.

| File | Vendor keys | Cost model | Property identified by |
|---|---|---|---|
| `google_ads_export_20260409.csv` | 4, 8 | Cost per click | `Property ID` |
| `bing_ads_export_20260409.csv` | 5 | Cost per click | `Property_ID` |
| `meta_ads_export_20260409.csv` | 6, 7 | CPM | `Property_ID` |
| `zillow_leads_20260409.csv` | 1 | Subscription plus lead fee | `Property_Key` |
| `apartments_com_20260409.csv` | 2 | Listing package | `Property_Key` |
| `apartment_list_20260409.csv` | 3 | Pay per lease | `property_key` |
| `display_dsp_20260409.csv` | 9, 10 | CPM | `Property_Key` |

Things the spend pipeline has to reconcile:

- Google Ads writes four banner lines above the header row, and formats currency as `$106.14` and rates as `1.09%` rather than as numbers.
- Apartment List uses snake_case throughout while every other file uses title case.
- Meta reports Facebook and Instagram from a single account, so one file resolves to two vendor keys. Google Ads does the same for Search and Display.
- Cost arrives as `Cost`, `Spend`, `Amount spent (USD)`, `Monthly Cost`, `Monthly Spend`, `estimated_spend_usd`, and `Spend (USD)`.
- Conversion arrives as `Conversions`, `Results`, `Applications`, `Tour Requests`, `lease_signings_attributed`, and `Post-Click Conversions`.

## Parser status

All seven vendor files are generated and land in bronze. The spend pipeline reads them through a parser registry in `spend_pipeline.py`, where each source is one entry: a filename template and a parse function returning rows in a canonical shape.

Google Ads is implemented as the reference parser and covers VK4 Search and VK8 Display. The remaining sources are scaffolded against the same contract. Adding one is a parser module plus a single registry line, with no change to the orchestration loop.

`Email Marketing` (VK11) carries spend in canonical truth but has no source file in this set.

## Operations

| File | Source system | Notes |
|---|---|---|
| `yardi_ops_export_20260409.csv` | Yardi Voyager | Flat file extract, all 120 properties, 31 columns including unit states, absorption, and 60-day forward windows |

## CRM

Six raw Salesforce tables that preserve referential integrity across the set. Every task points at a real lead or contact, and every opportunity points at a real lead.

| File | Grain |
|---|---|
| `sf_leads_raw_20260409.csv` | One row per lead |
| `sf_contacts_raw_20260409.csv` | One row per converted contact |
| `sf_opportunities_raw_20260409.csv` | One row per lease opportunity |
| `sf_tasks_raw_20260409.csv` | One row per touchpoint |
| `sf_campaigns_raw_20260409.csv` | One row per campaign |
| `sf_campaign_members_raw_20260409.csv` | Junction linking leads and contacts to campaigns |

The CRM files carry deliberate data quality defects: malformed phone numbers, invalid email addresses, and inconsistent name casing. The reconciliation layer is built to detect and classify them rather than silently repair them.

## Regenerating

```bash
python mock_source_generator.py --date 2026-04-09 --output ./samples
```
