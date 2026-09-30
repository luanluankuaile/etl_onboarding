"""Named execution wrapper around one layer blueprint."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class DataProcessor:
    processor_name: str
    blueprint: Any

    def execute(self) -> Any:
        return self.blueprint.execute()