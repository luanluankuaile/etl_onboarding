"""Synchronous local orchestrator for metadata-defined layer blueprints."""
from __future__ import annotations

from .blueprints import LandingToRawBlueprint, PersistentToConsumptionBlueprint, RawToPersistentBlueprint
from .context import RuntimeContext, utc_now
from .control import ControlService
from .metadata import Metadata


class ETLRunner:
    def __init__(self, metadata: Metadata, context: RuntimeContext):
        self.metadata = metadata
        self.context = context
        self.control = ControlService(context.control_db)

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
        rows = self._run_processor(
            "landing_to_raw", LandingToRawBlueprint(self.context, self.control, self.metadata).execute
        )
        self.control.rows(self.context.run_id, "landing_to_raw", "raw",
                          self.metadata.landing.get("raw_table", "raw"), rows)
        return rows

    def raw_to_persistent(self) -> None:
        for mapping in self.metadata.persistent:
            inserted, rejected = self._run_processor(
                f"raw_to_persistent:{mapping.name}",
                RawToPersistentBlueprint(self.context, self.control, mapping).execute,
            )
            self.control.rows(self.context.run_id, "raw_to_persistent", "persistent",
                              mapping.target_table, inserted, rejected)

    def persistent_to_consumption(self) -> None:
        items = self.metadata.consumption if isinstance(self.metadata.consumption, list) else [self.metadata.consumption]
        for item in items:
            name = item.get("name", item.get("table", "consumption"))
            self._run_processor(
                f"persistent_to_consumption:{name}",
                PersistentToConsumptionBlueprint(self.context, self.control, item).execute,
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