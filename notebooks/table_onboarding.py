# Databricks notebook source
# COMMAND ----------

from pathlib import Path
from uuid import uuid4

from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.workflow_runner import LocalWorkflowRunner

# Install the etl_framework wheel or add its repository to the cluster before running.
dbutils.widgets.text(
    "workflow_path",
    "/Workspace/Shared/etl_onboarding/metadata/workflows/customer_process_daily.yaml",
)
dbutils.widgets.text("landing_dir", "/Volumes/main/default/etl/landing")
dbutils.widgets.text("storage_dir", "/Volumes/main/default/etl/state")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("run_id", "")

# COMMAND ----------

workflow_path = Path(dbutils.widgets.get("workflow_path"))
landing_dir = Path(dbutils.widgets.get("landing_dir"))
storage_dir = Path(dbutils.widgets.get("storage_dir"))
environment = dbutils.widgets.get("environment")
run_id = dbutils.widgets.get("run_id") or str(uuid4())

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

# The workflow runner configures each table processor and calls its entry point,
# which invokes Blueprint.processor(...).execute().
LocalWorkflowRunner(workflow_path, context).run()

print(f"Completed table onboarding workflow {workflow_path.name}; run_id={run_id}")
