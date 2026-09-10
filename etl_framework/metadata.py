"""Validated, deliberately small YAML metadata model."""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import yaml


@dataclass
class Column:
    name: str
    source: str | None = None
    data_type: str = "TEXT"
    nullable: bool = True
    default: Any = None


@dataclass
class TableMapping:
    name: str
    source_table: str
    target_table: str
    columns: list[Column]
    keys: list[str] = field(default_factory=list)
    deduplicate_by: list[str] = field(default_factory=list)
    watermark_column: str | None = None
    watermark_type: str = "TEXT"
    dq_quarantine_table: str | None = None


@dataclass
class Metadata:
    landing: dict[str, Any]
    raw: dict[str, Any]
    persistent: list[TableMapping]
    consumption: list[dict[str, Any]]


def load_metadata(path: str | Path) -> Metadata:
    with open(path, encoding="utf-8") as stream:
        data = yaml.safe_load(stream) or {}
    persistent = []
    for item in data.get("persistent", []):
        columns = [Column(name=c["name"], source=c.get("source"),
                          data_type=c.get("type", "TEXT"), nullable=c.get("nullable", True),
                          default=c.get("default")) for c in item.get("columns", [])]
        if not columns:
            raise ValueError(f"Mapping {item.get('name')} has no columns")
        persistent.append(TableMapping(
            name=item["name"], source_table=item["source_table"], target_table=item["target_table"],
            columns=columns, keys=item.get("keys", []), deduplicate_by=item.get("deduplicate_by", []),
            watermark_column=item.get("watermark_column"), watermark_type=item.get("watermark_type", "TEXT"),
            dq_quarantine_table=item.get("dq_quarantine_table")))
    return Metadata(data.get("landing", {}), data.get("raw", {}), persistent, data.get("consumption", []))
