"""Named lifecycle executor for a layer blueprint."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class BlueprintLifecycle(Protocol):
    """The extract, transform, and load contract shared by all blueprints."""

    def extract(self) -> Any: ...

    def transform(self, source: Any) -> Any: ...

    def load(self, transformed: Any) -> Any: ...


@dataclass
class DataProcessor:
    processor_name: str
    blueprint: BlueprintLifecycle

    def execute(self) -> Any:
        source = self.blueprint.extract()
        transformed = self.blueprint.transform(source)
        return self.blueprint.load(transformed)