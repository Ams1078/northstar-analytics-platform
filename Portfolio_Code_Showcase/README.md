# NorthStar Analytics Platform — Code Showcase

A curated selection of code from the NorthStar Intelligence Platform, assembled for
one purpose: to let an experienced reader understand how the platform is built in
about thirty minutes.

This is not a backup and not a repository export. Everything here was selected,
reordered and annotated. Where two files demonstrated the same idea, the stronger
one was kept.

---

## Scale

| | Complete platform | This showcase |
|---|---|---|
| DAX measures | ~1,500 active (~1,750 including quarantine) | **85** |
| Python modules | 12 pipeline and generator modules | **11** |
| SQL statements | Embedded across the pipeline layer | **20** |
| Supporting files | Warehouse, semantic model, report definitions, portal, docs | not included |

Roughly 5% of the model's measures, chosen to teach the architecture rather than
document it.

---

## What each folder demonstrates

**SQL** — how the warehouse is operated. Run ledger and watermarks, quarantine and
flagging, incremental publication, and the reconciliation layer that compares each
source system against canonical truth.

**DAX** — how the semantic model was built, ordered as it would be learned. Base
facts, then business metrics, then the four executive indexes, then the measures
that write sentences, then the reusable dynamic patterns.

**Python** — the architecture end to end. A synthetic business generator, the bronze
source emitters that imitate seven real vendor export formats, the Azure Functions
scheduler, one pipeline per source system, and the reconciliation engine.

---

## Reading order

If you have thirty minutes:

1. **`Python/01 Synthetic Engine`** — start here. The platform manufactures its own
   upstream, and understanding the shape of the generated business makes everything
   downstream legible.
2. **`Python/03 Pipeline/02_spend_pipeline.py`** — the parser registry. Seven vendor
   formats, one contract.
3. **`SQL/01 Run Ledger`** and **`SQL/02 Data Quality`** — how a night is recorded and
   how bad rows are handled. Four short scripts each.
4. **`SQL/04 Reconciliation`** — the idea the platform is built around: a disagreement
   between systems is a finding with an owner, not noise.
5. **`DAX/01 Base Measures`** through **`03 Executive Indexes`** — the composition
   pattern, from `SUM` to a weighted 0-100 portfolio score.
6. **`DAX/05 Dynamic Logic & Patterns`** — the machinery that repeats across the model.

If you have ten, read items 2, 4 and 5.

---

## A note on what is here

Every file is real. The DAX is exported from the live model; the Python is the
running pipeline; the SQL is extracted verbatim from the modules that execute it,
with a header added explaining what each statement demonstrates. Nothing was written
for the showcase.

One consequence worth stating: `Portfolio Health Index` in `DAX/03` computes weights
of 0.35 / 0.35 / 0.30, while the designed framework specifies 0.45 GPI / 0.30 OPI /
0.25 VPI. The model is mid-migration between the two, and the file is annotated to
say so rather than quietly showing the intended version.
