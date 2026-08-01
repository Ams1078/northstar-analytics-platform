# 02 Azure Functions

The scheduler. Five timer triggers, each a thin wrapper around a pipeline module, with the failure semantics that matter living here rather than in the pipelines.

### `function_app.py`

Timer-triggered entry points for canonical, ops, spend and CRM.

*Worth reading for the exception handling rather than the orchestration. A missing bronze folder raises FileNotFoundError and is caught, logged and exited cleanly, because a night with nothing to do is not a failure. Any other exception is re-raised so the platform fails loudly. That distinction is the whole design.*

### `pipeline_utils.py`

Shared infrastructure: run logging, watermarks, staging, reconciliation writes, quarantine.

*The largest module and the most reused. start_run and finish_run bracket every pipeline; the gold-table whitelist blocks unparameterised table names; blob staging isolates cloud access from pipeline logic so each pipeline can be run locally against a folder.*
