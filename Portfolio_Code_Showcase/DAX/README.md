# DAX

A teaching path through the semantic model. The live model holds roughly 1,500
active measures; these 85 were chosen to show how it is built, and are ordered as
they would be learned rather than as they were exported.

Each file keeps the original DAX unchanged, with a header explaining its purpose and
why it exists.

**Conventions visible throughout.** Measures prefixed with an underscore are internal
helpers never placed on a visual. Every expensive intermediate is read into a `VAR`
once rather than recomputed. Ratios use `DIVIDE` so an empty selection returns blank
instead of an error. Formatting and colour live in measures, so they are versioned
with the model rather than buried in visual settings.

### 01 Base Measures  (14 measures)

Additive facts and the two habits everything else depends on: DIVIDE for safe ratios, and capacity read from the dimension rather than the fact.

### 02 Business Metrics  (20 measures)

Cost, conversion and the forward-looking windows. Where business rules enter the model.

### 03 Executive Indexes  (25 measures)

The four composite scores. Read in order: the pillars that feed each index, then the index, then the tier that turns a number into a word.

### 04 Insights & Narrative  (12 measures)

Measures that write sentences. The layout never moves; the argument always does.

### 05 Dynamic Logic & Patterns  (14 measures)

Reusable machinery: parameter-driven switching, dispatch, formatting and windowing. These patterns repeat across the model, so they are worth reading once carefully.
