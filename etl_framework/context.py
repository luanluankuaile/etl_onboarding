from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RuntimeContext:
    run_id: str
    environment: str = "local"
    landing_dir: Path = Path("landing")
    raw_db: Path = Path("raw.sqlite")
    persistent_db: Path = Path("persistent.sqlite")
    consumption_db: Path = Path("consumption.sqlite")
    control_db: Path = Path("etl_control.sqlite")
    values: dict[str, Any] = field(default_factory=dict)
    process_date: str | None = None
    previous_successful_process_date: str | None = None
    processor_parameters: dict[str, Any] = field(default_factory=dict)
    source_metadata: dict[str, Any] = field(default_factory=dict)
    target_metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def quarantine_dir(self) -> Path:
        return self.landing_dir / "quarantine"

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "environment": self.environment,
            "process_date": self.process_date,
            "previous_successful_process_date": self.previous_successful_process_date,
            "parameters": self.processor_parameters,
            "source": self.source_metadata,
            "target": self.target_metadata,
            **self.values,
        }
