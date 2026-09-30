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
    data_quality_checks: list[dict[str, Any]] = field(default_factory=list)
    update_mode: str = "incremental_upsert"


@dataclass
class Metadata:
    landing: dict[str, Any]
    raw: dict[str, Any]
    persistent: list[TableMapping]
    consumption: list[dict[str, Any]]
    quality: list[dict[str, Any]] = field(default_factory=list)


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream) or {}


def _load_table_references(data: dict[str, Any], metadata_path: Path) -> dict[str, Any]:
    table_metadata = data.get("table_metadata", {})
    raw_reference = table_metadata.get("raw")
    if raw_reference:
        data["raw"] = _load_yaml(metadata_path.parent / raw_reference)
    persistent_references = table_metadata.get("persistent", [])
    if persistent_references:
        if isinstance(persistent_references, str):
            persistent_references = [persistent_references]
        data["persistent"] = [
            _load_yaml(metadata_path.parent / reference)
            for reference in persistent_references
        ]
    return data


def load_metadata(path: str | Path) -> Metadata:
    metadata_path = Path(path)
    data = _load_table_references(_load_yaml(metadata_path), metadata_path)
    persistent = []
    persistent_config = data.get("persistent", [])
    if isinstance(persistent_config, dict):
        item = dict(persistent_config)
        item.setdefault("name", item.get("table"))
        item.setdefault("source_table", data.get("raw", {}).get("table_name", data.get("raw", {}).get("table")))
        item.setdefault("target_table", item.get("table"))
        persistent_config = [item]
    registered_columns = data.get("columns", [])
    for item in persistent_config:
        item = dict(item)
        item.setdefault("name", item.get("table_name") or item.get("table"))
        item.setdefault("target_table", item.get("table_name") or item.get("table"))
        item.setdefault("keys", item.get("primary_keys", []))
        item["columns"] = item.get("columns") or registered_columns
        columns = [Column(name=c["name"], source=c.get("source"),
                          data_type=c.get("type", "TEXT"), nullable=c.get("nullable", True),
                          default=c.get("default")) for c in item.get("columns", [])]
        if not columns:
            raise ValueError(f"Mapping {item.get('name')} has no columns")
        raw_quality = data.get("raw", {}).get("data_quality_checks", [])
        persistent.append(TableMapping(
            name=item["name"], source_table=item["source_table"], target_table=item["target_table"],
            columns=columns, keys=item.get("keys", []), deduplicate_by=item.get("deduplicate_by", []),
            watermark_column=item.get("watermark_column"), watermark_type=item.get("watermark_type", "TEXT"),
            dq_quarantine_table=item.get("dq_quarantine_table"),
            data_quality_checks=item.get("data_quality_checks", raw_quality),
            update_mode=item.get("table_update_mode", "incremental_upsert")))
    return Metadata(data.get("landing", {}), data.get("raw", {}), persistent,
                    data.get("consumption", []), data.get("quality", {}).get("rules", []))
