"""Synchronous local orchestrator that invokes notebook processor entry points."""
from __future__ import annotations

from .blueprints.landing_to_raw import execute as execute_landing_to_raw
from .blueprints.persistent_to_consumption import execute as execute_persistent_to_consumption
from .blueprints.raw_to_persistent import execute as execute_raw_to_persistent
from .context import RuntimeContext, utc_now
from .control import ControlService
from .metadata import Metadata


class ETLRunner:
    def __init__(self, metadata: Metadata, context: RuntimeContext):
        self.metadata = metadata
        self.context = context
        self.control = ControlService(context.control_db)
        self.context.values["metadata"] = metadata

    def _configure_processor(
        self,
        processor_name: str,
        source_tables: list[str],
        target_tables: list[str],
    ) -> None:
        self.context.processor_parameters = {"processor_name": processor_name}
        self.context.source_metadata = {"tables": source_tables}
        self.context.target_metadata = {"tables": target_tables}

    def _run_processor(self, name: str, operation):
        started = utc_now()
        self.control.processor(self.context.run_id, name, "RUNNING", started)
        try:
            result = operation()
        except Exception as exc:
            self.control.processor(self.context.run_id, name, "FAILED", started, utc_now(), str(exc))
            raise
        self.control.processor(self.context.run_id, name, "SUCCEEDED", started, utc_now())
        return result

    def landing_to_raw(self) -> int:
        processor_name = "landing_to_raw"
        raw_table = self.metadata.landing.get("raw_table", "raw")
        self._configure_processor(processor_name, [], [f"raw.{raw_table}"])
        rows = self._run_processor(
            processor_name,
            lambda: execute_landing_to_raw(self.context),
        )
        self.control.rows(self.context.run_id, processor_name, "raw", raw_table, rows)
        return rows

    def raw_to_persistent(self) -> None:
        for mapping in self.metadata.persistent:
            processor_name = f"raw_to_persistent:{mapping.name}"
            self._configure_processor(
                processor_name,
                [f"raw.{mapping.source_table}"],
                [f"persistent.{mapping.target_table}"],
            )
            inserted, rejected = self._run_processor(
                processor_name,
                lambda: execute_raw_to_persistent(self.context)[0],
            )
            self.control.rows(self.context.run_id, "raw_to_persistent", "persistent",
                              mapping.target_table, inserted, rejected)

    def persistent_to_consumption(self) -> None:
        items = self.metadata.consumption if isinstance(self.metadata.consumption, list) else [self.metadata.consumption]
        for item in items:
            name = item.get("name", item.get("table", "consumption"))
            processor_name = f"persistent_to_consumption:{name}"
            source_table = item.get("source_table", name)
            self._configure_processor(
                processor_name,
                [f"persistent.{source_table}"],
                [f"consumption.{name}"],
            )
            self._run_processor(
                processor_name,
                lambda: execute_persistent_to_consumption(self.context)[0],
            )

    def run(self) -> None:
        started = utc_now()
        self.context.process_date = self.context.process_date or started
        self.context.previous_successful_process_date = (
            self.context.previous_successful_process_date or self.control.previous_successful_process_date()
        )
        self.control.run(self.context.run_id, self.context.environment, "RUNNING", started)
        try:
            self.landing_to_raw()
            self.raw_to_persistent()
            self.persistent_to_consumption()
        except Exception as exc:
            self.control.run(self.context.run_id, self.context.environment, "FAILED", started, utc_now(), str(exc))
            raise
        self.control.run(self.context.run_id, self.context.environment, "SUCCEEDED", started, utc_now())