"""Workflow-YAML execution for locally runnable Python processors."""
from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from .context import RuntimeContext
from .workflow import DAG


@dataclass
class ProcessorDefinition:
    processor_name: str
    notebook: str
    depends_on: list[str]
    parameters: dict[str, Any]
    source_tables: list[str]
    target_tables: list[str]


def load_workflow(path: str | Path) -> list[ProcessorDefinition]:
    with Path(path).open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream) or {}
    processors = document.get("data_processors", document.get("processors", document.get("steps", [])))
    return [ProcessorDefinition(
        processor_name=item.get("processor_name") or item.get("process_name") or item["name"],
        notebook=item.get("notebook", item.get("path", item.get("processor", ""))),
        depends_on=item.get("depends_on", []),
        parameters=item.get("parameters", {}),
        source_tables=item.get("source_tables", []),
        target_tables=item.get("target_tables", []),
    ) for item in processors]


class DataProcessorRegistry:
    def __init__(self, definitions: list[ProcessorDefinition]):
        self._entry_points = {
            definition.processor_name: LocalWorkflowRunner._load_entry_point(definition.notebook)
            for definition in definitions
        }

    def execute(self, processor_name: str, context: RuntimeContext):
        try:
            return self._entry_points[processor_name](context)
        except KeyError as exc:
            raise ValueError(f"Unknown processor_name: {processor_name}") from exc


class LocalWorkflowRunner:
    def __init__(self, workflow_path: str | Path, context: RuntimeContext):
        self.definitions = load_workflow(workflow_path)
        self.context = context
        self.registry = DataProcessorRegistry(self.definitions)

    @staticmethod
    def _load_entry_point(path: str):
        module_name, separator, attribute = path.replace("/", ".").replace(":", ".").rpartition(".")
        if not separator:
            raise ValueError(f"Processor path must be module.callable: {path}")
        return getattr(import_module(module_name), attribute)

    def _execute_definition(self, definition: ProcessorDefinition) -> None:
        self.context.processor_parameters = {"processor_name": definition.processor_name, **definition.parameters}
        self.context.source_metadata = {"tables": definition.source_tables}
        self.context.target_metadata = {"tables": definition.target_tables}
        self.registry.execute(definition.processor_name, self.context)

    def run(self, processor_name: str | None = None) -> None:
        if processor_name is not None:
            for definition in self.definitions:
                if definition.processor_name == processor_name:
                    self._execute_definition(definition)
                    return
            raise ValueError(f"Unknown processor_name: {processor_name}")

        dag = DAG()
        for definition in self.definitions:
            def task(_: RuntimeContext, definition: ProcessorDefinition = definition):
                self._execute_definition(definition)
            dag.add(definition.processor_name, task, definition.depends_on)
        dag.run(self.context)