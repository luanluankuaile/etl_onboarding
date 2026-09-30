# Databricks notebook source
# COMMAND ----------

from pathlib import Path
from uuid import uuid4

from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.workflow_runner import LocalWorkflowRunner

workflow_path = Path(
    "/Workspace/Shared/etl_onboarding/metadata/workflows/customer_daily.yml"
)
processor_name = "customer_customers_land_to_raw"
landing_dir = Path("/Volumes/main/default/etl/landing")
storage_dir = Path("/Volumes/main/default/etl/state")
environment = "dev"
run_id = str(uuid4())

if not workflow_path.is_file():
    raise FileNotFoundError(f"Workflow metadata was not found: {workflow_path}")

storage_dir.mkdir(parents=True, exist_ok=True)
context = RuntimeContext(
    run_id=run_id,
    environment=environment,
    landing_dir=landing_dir,
    raw_db=storage_dir / "raw.sqlite",
    persistent_db=storage_dir / "persistent.sqlite",
    consumption_db=storage_dir / "consumption.sqlite",
    control_db=storage_dir / "etl_control.sqlite",
)
context.values["metadata"] = load_metadata(workflow_path)

# The workflow runner configures the selected table processor and calls its
# entry point, which invokes Blueprint.processor(...).execute().
LocalWorkflowRunner(workflow_path, context).run(processor_name)

print(
    f"Completed processor {processor_name} for workflow {workflow_path.name}; "
    f"run_id={run_id}"
)
