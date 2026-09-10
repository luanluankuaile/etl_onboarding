# Local metadata-driven ETL

A small, dependency-light Python framework implementing Landing CSV discovery, Raw SQLite ingestion, YAML-defined Raw-to-Persistent mappings, configurable casts and not-null quarantine, deduplication, incremental file processing, Persistent-to-Consumption SQL, and an auditable control database.

## Run the demo

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e '.[test]'
pytest
```

Create `landing/customers.csv` using the columns in `metadata/demo.yml`, then run either the Python API or CLI:

```bash
python -m etl_framework metadata/demo.yml --landing landing --environment local
```

```python
from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.runner import ETLRunner

metadata = load_metadata('metadata/demo.yml')
context = RuntimeContext('run-001', landing_dir='landing', raw_db='raw.sqlite',
    persistent_db='persistent.sqlite', consumption_db='consumption.sqlite', control_db='etl_control.sqlite')
ETLRunner(metadata, context).run()
```

The three data layers have separate SQLite files. `etl_control.sqlite` records runs, processors, row counts, file manifests and watermarks. Processed files are skipped on subsequent runs. Persistent rows receive `run_id`, `environment`, `latest_update_datetime`, and `latest_insert_datetime`; invalid not-null rows are written to the configured quarantine table.

## Metadata and extension points

`persistent[].columns` supports `name`, optional `source`, SQLite-compatible `type`, `nullable`, and `default`; `keys` enables upsert semantics and `deduplicate_by` removes repeated source keys within a load. Consumption SQL can reference Persistent tables through the attached `source` database (for example `source.customers`). Ordinary Python callables can be composed with `etl_framework.workflow.DAG` for custom processors.

This is intentionally local and synchronous: production scheduling, secrets, cloud storage, schema registry integration, and human approval workflows remain deployment/governance responsibilities. Review metadata, mappings, DQ policy, security classification, and release approval before promoting changes.
