# 03 Pipeline

One module per source system. Each conforms a different vendor's idea of a record into one dimensional model, and each writes its own DataSource so nothing blends.

### `publish_canonical_truth.py`

Publishes canonical truth to Azure SQL incrementally.

*Stages before publishing rather than rebuilding in place. Deletes are scoped to both DateKey and DataSource, so re-running a night reproduces it exactly and one pipeline can never erase another's rows.*

### `spend_pipeline.py`

Reads bronze vendor exports and conforms them to the spend fact.

*Built on a parser registry: each source is one entry pairing a filename template with a parse function returning a canonical shape. The orchestration loop never learns anything vendor-specific, so adding the seventh source costs the same as the second.*

### `crm_pipeline.py`

Reconstructs marketing journeys from six raw Salesforce tables.

*The most involved transformation in the platform. Preserves referential integrity across leads, contacts, opportunities and tasks, and rebuilds the prospect journey that the Attribution Lab later scores.*

### `ops_pipeline.py`

Loads the Yardi operations extract.

*The simplest pipeline, included precisely for that reason: it shows the shared contract every pipeline implements, without the source-specific complexity of the other two.*
