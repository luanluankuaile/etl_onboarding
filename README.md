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

See [the local architecture guide](docs/local_etl_architecture.md) for the layer
contracts, table/workflow YAML examples, and notebook-compatible Python processor
entry points.

## Metadata and extension points

`persistent[].columns` supports `name`, optional `source`, SQLite-compatible `type`, `nullable`, and `default`; `keys` enables upsert semantics and `deduplicate_by` removes repeated source keys within a load. Consumption SQL can reference Persistent tables through the attached `source` database (for example `source.customers`). Ordinary Python callables can be composed with `etl_framework.workflow.DAG` for custom processors.

This is intentionally local and synchronous: production scheduling, secrets, cloud storage, schema registry integration, and human approval workflows remain deployment/governance responsibilities. Review metadata, mappings, DQ policy, security classification, and release approval before promoting changes.

## Databricks deployment

The Databricks Asset Bundle in `databricks.yml` packages this project as a wheel,
syncs `metadata/` to the workspace, and deploys the `metadata_catalog_sync` job.
The job reads every table contract and creates the `raw`, `persistent`, and
`consumption` schemas in the configured Unity Catalog. It creates missing Delta
tables and adds metadata columns that are new to an existing table.

Raw contracts without columns inherit source columns from their Persistent mapping.
Consumption contracts can similarly inherit from `source_table`; see
`metadata/tables/consumption/csp_dim_account.yml`.

The bundle supports `dev`, `staging`, and `prod` targets. Each target has its own
catalog default and job name; the workspace URL is supplied per deployment. Store
the following values in the matching GitHub Environment: `DATABRICKS_HOST` and
`DATABRICKS_TOKEN` as secrets; `DATABRICKS_CATALOG`, `DATABRICKS_NODE_TYPE`, and
`DATABRICKS_DATA_SECURITY_MODE` as variables. A push to `main` deploys `prod`.
Manual runs can select any target. For a local deployment, use:

```bash
databricks bundle deploy -t dev --var "databricks_host=https://<workspace>" --var "catalog_name=<catalog>" --var "node_type_id=<node-type>" --var "data_security_mode=SINGLE_USER"
databricks bundle run metadata_catalog_sync -t dev --var "databricks_host=https://<workspace>" --var "catalog_name=<catalog>" --var "node_type_id=<node-type>" --var "data_security_mode=SINGLE_USER"
```

The current Landing, Raw-to-Persistent, and Consumption processing implementation
uses SQLite. The catalog job is Databricks-native, but executing transformations
against Delta tables requires a separate Spark/Delta execution adapter.
