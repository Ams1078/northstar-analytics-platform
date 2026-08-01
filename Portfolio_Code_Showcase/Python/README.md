# Python

The platform architecture in 11 modules, from the synthetic business that produces
the data through to the reconciliation engine that audits it.

**How they interact.** The generator writes canonical truth. The bronze emitters
degrade that truth into seven vendor export formats and land them in blob storage.
Azure Functions wakes each pipeline on a schedule; each pipeline reads its own bronze
folder, conforms it, and writes its own `DataSource`. Reconciliation then compares
every source against canonical truth and queues the differences.

Nothing blends. That is the design constraint the whole architecture serves.

### 01 Synthetic Engine

The platform has no upstream vendor. These modules manufacture one, then degrade it on purpose so the downstream pipeline has something real to fail against.

### 02 Azure Functions

The scheduler. Five timer triggers, each a thin wrapper around a pipeline module, with the failure semantics that matter living here rather than in the pipelines.

### 03 Pipeline

One module per source system. Each conforms a different vendor's idea of a record into one dimensional model, and each writes its own DataSource so nothing blends.

### 04 Monitoring & Reconciliation

The platform checking itself. These modules produce the numbers behind the operations dashboard.
