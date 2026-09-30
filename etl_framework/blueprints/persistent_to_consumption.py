"""Persistent-to-Consumption Blueprint and workflow notebook entry point."""
from __future__ import annotations

from pathlib import Path

from ..context import RuntimeContext
from ..control import ControlService
from ..metadata import Metadata
from ..transforms import run_sql
from .base import Blueprint, execute_processor, target_names


class PersistentToConsumptionBlueprint(Blueprint):
    def __init__(self, context: RuntimeContext, control: ControlService, item: dict[str, object]):
        super().__init__(context, control)
        self.item = item

    def extract(self) -> str:
        sql = self.item.get("sql") or self.item.get("mapping") or self.item.get("query_file")
        if not sql:
            raise ValueError(f"Consumption transformation {self.item.get('name')} has no SQL")
        return str(sql)

    def transform(self, source: str) -> str:
        query_file = self.item.get("query_file")
        if query_file:
            source = Path(str(query_file)).read_text(encoding="utf-8")
        if self.item.get("mapping"):
            target_table = self.item.get("table") or self.item.get("name")
            if not target_table:
                raise ValueError("Consumption mapping requires a target table or name")
            query = source.strip().rstrip(";")
            return f'DROP TABLE IF EXISTS "{target_table}"; CREATE TABLE "{target_table}" AS {query};'
        return source

    def load(self, sql: str) -> None:
        run_sql(sql, self.context.persistent_db, self.context.consumption_db)


def execute(context: RuntimeContext) -> list[None]:
    metadata: Metadata = context.values["metadata"]
    items = metadata.consumption if isinstance(metadata.consumption, list) else [metadata.consumption]
    targets = target_names(context)
    items = [item for item in items if not targets or item.get("name", item.get("table")) in targets]
    processor_name = context.processor_parameters["processor_name"]
    if not items:
        raise ValueError(f"{processor_name} has no matching Consumption transformation")
    return [
        execute_processor(context, PersistentToConsumptionBlueprint(context, ControlService(context.control_db), item))
        for item in items
    ]