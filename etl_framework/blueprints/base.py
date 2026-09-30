"""Shared Blueprint lifecycle support."""
from __future__ import annotations

from ..context import RuntimeContext
from ..control import ControlService

AUDIT_COLUMNS = [
    ("run_id", "TEXT", False),
    ("environment", "TEXT", False),
    ("latest_update_datetime", "TEXT", False),
    ("latest_insert_datetime", "TEXT", False),
]


class Blueprint:
    def __init__(self, context: RuntimeContext, control: ControlService):
        self.context = context
        self.control = control

    def processor(self, processor_name: str):
        from ..data_processor import DataProcessor

        return DataProcessor(processor_name, self)


def execute_processor(context: RuntimeContext, blueprint: Blueprint):
    processor_name = context.processor_parameters["processor_name"]
    return blueprint.processor(processor_name).execute()


def target_names(context: RuntimeContext) -> set[str]:
    return {target.split(".", 1)[-1] for target in context.target_metadata.get("tables", [])}