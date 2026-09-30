# Local ETL Architecture

The local implementation keeps each layer in an independent SQLite file:

```text
landing/                         # incoming *.csv, then *.csv.processed
landing/quarantine/              # rejected Raw-to-Persistent records as JSONL
RAW.db                           # source-shaped rows plus arrival/file audit data
PERSISTENT.db                    # typed, deduplicated business entities
CONSUMPTION.db                   # reporting tables and views
ETL_CONTROL.db                   # run, processor, row-count, watermark, manifest state
metadata/
  tables/raw/                    # Raw table and DQ contracts
  tables/persistent/             # typed mappings and upsert keys
  workflows/                     # processor DAGs
etl_framework/
  blueprints/                    # one Blueprint and notebook entry point per layer
  workflow_runner.py             # YAML DAG executor
```

`LandingToRawBlueprint` preserves CSV values and appends `arrival_date`, source-file
metadata, and the standard audit columns. It changes a successfully loaded input from
`.csv` to `.csv.processed`. `RawToPersistentBlueprint` maps and casts configured
columns, validates `data_quality_checks`, writes rejected source records to
`landing/quarantine`, and performs SQLite `INSERT ... ON CONFLICT DO UPDATE` using
`primary_keys`. `PersistentToConsumptionBlueprint` uses SQLite `ATTACH DATABASE` to
run metadata SQL against Persistent and store results in Consumption.

## Table metadata

```yaml
table_name: customers
database_layer: persistent
table_update_mode: incremental_upsert
primary_keys: [customer_id]
columns:
  - name: customer_id
    source: customer_id
    type: INTEGER
    nullable: false
```

Raw contracts can declare `data_quality_checks` such as `col_is_not_null`; audit
columns `run_id`, `environment`, `latest_update_datetime`, and
`latest_insert_datetime` are injected by the framework and are not configured in YAML.

## Workflow execution

Workflow YAML uses `data_processors`, `path`, and optional `depends_on`. The path
identifies a normal Python callable accepting `RuntimeContext`.

Each workflow step declares a unique `processor_name`. The workflow runner uses
that name as the DAG node and resolves its `notebook` callable. The notebook
instantiates the layer Blueprint and runs `blueprint.processor(processor_name)`;
the resulting `DataProcessor` invokes the Blueprint's `extract`, `transform`, and
`load` lifecycle for the configured table.

```yaml
data_processors:
  - processor_name: customer_customers_raw_to_per
    notebook: etl_framework.blueprints.raw_to_persistent.execute
    depends_on: [customer_customers_land_to_raw]
    source_tables: [raw.customers]
    target_tables: [persistent.customers]
```

```python
from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.workflow_runner import LocalWorkflowRunner

context = RuntimeContext("run-001", landing_dir="data/landing")
context.values["metadata"] = load_metadata("metadata/demo.yml")
LocalWorkflowRunner("metadata/workflows/customer_daily.yml", context).run()
```

## Terraform deployment

Terraform deploys all version-controlled table contracts and workflows to the
runtime metadata directory. Configure the target directory for each environment
and apply the module from `terraform/`:

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
terraform -chdir=terraform init
terraform -chdir=terraform apply
```

The module deploys `metadata/tables/**/*.yml` to `<metadata_directory>/tables/`
and `metadata/workflows/**/*.yml` to `<metadata_directory>/workflows/`. Terraform
tracks each deployed file, so a changed, added, or removed YAML is reconciled on
the next apply.