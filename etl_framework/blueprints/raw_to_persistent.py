"""Raw-to-Persistent Blueprint and workflow notebook entry point."""
from __future__ import annotations

import json
from typing import Any

from ..context import RuntimeContext, utc_now
from ..control import ControlService
from ..metadata import Metadata, TableMapping
from ..sqlite import connect, create_table
from .base import AUDIT_COLUMNS, Blueprint, execute_processor, target_names


def cast(value: Any, data_type: str) -> Any:
    if value in (None, ""):
        return None
    normalized = data_type.upper()
    if normalized.startswith("INT"):
        return int(value)
    if normalized.startswith(("REAL", "FLOAT")):
        return float(value)
    return str(value) if normalized.startswith("DECIMAL") else value


class RawToPersistentBlueprint(Blueprint):
    def __init__(self, context: RuntimeContext, control: ControlService, mapping: TableMapping):
        super().__init__(context, control)
        self.mapping = mapping

    def extract(self) -> list[Any]:
        source = connect(self.context.raw_db)
        try:
            return source.execute(f'SELECT * FROM "{self.mapping.source_table}"').fetchall()
        finally:
            source.close()

    def _is_valid(self, row: dict[str, Any], values: dict[str, Any]) -> bool:
        for column in self.mapping.columns:
            if not column.nullable and values[column.name] is None:
                return False
        for check in self.mapping.data_quality_checks:
            rule = check.get("rule", check.get("check", "")).lower()
            columns = check.get("columns") or [check.get("column")]
            if rule in {"col_is_not_null", "not_null"} and any(values.get(column) is None for column in columns):
                return False
        return True

    def _write_quarantine_file(self, rejected: list[dict[str, Any]]) -> None:
        if not rejected:
            return
        self.context.quarantine_dir.mkdir(parents=True, exist_ok=True)
        target = self.context.quarantine_dir / f"{self.mapping.source_table}_{self.context.run_id}.jsonl"
        with target.open("w", encoding="utf-8") as stream:
            for row in rejected:
                stream.write(json.dumps(row, default=str) + "\n")

    def transform(self, source_rows: list[Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        valid: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for source_row in source_rows:
            source = dict(source_row)
            try:
                values = {column.name: cast(source.get(column.source or column.name), column.data_type)
                          for column in self.mapping.columns}
            except (TypeError, ValueError):
                values = {column.name: None for column in self.mapping.columns}
            if self.mapping.deduplicate_by:
                dedup_key = tuple(source.get(column) for column in self.mapping.deduplicate_by)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
            now = utc_now()
            values.update(run_id=self.context.run_id, environment=self.context.environment,
                          latest_update_datetime=now, latest_insert_datetime=now)
            if self._is_valid(source, values):
                valid.append(values)
            else:
                rejected.append({"source": source, "mapped": values, "reason": "data_quality_failed"})
        return valid, rejected

    def load(self, transformed: tuple[list[dict[str, Any]], list[dict[str, Any]]]) -> tuple[int, int]:
        target = connect(self.context.persistent_db)
        valid, quarantined = transformed
        try:
            create_table(target, self.mapping.target_table,
                         [(column.name, column.data_type, column.nullable) for column in self.mapping.columns] + AUDIT_COLUMNS,
                         self.mapping.keys)
            quarantine_table = self.mapping.dq_quarantine_table or f"{self.mapping.target_table}__quarantine"
            create_table(target, quarantine_table,
                         [(column.name, column.data_type, True) for column in self.mapping.columns] + AUDIT_COLUMNS)
            for values in valid:
                table = self.mapping.target_table
                names = list(values)
                placeholders = ",".join("?" for _ in names)
                if table == self.mapping.target_table and self.mapping.keys:
                    updates = [name for name in names if name not in self.mapping.keys and name != "latest_insert_datetime"]
                    conflict = ",".join(f'"{key}"' for key in self.mapping.keys)
                    sql = (f'INSERT INTO "{table}" ({",".join(chr(34) + name + chr(34) for name in names)}) VALUES ({placeholders}) '
                           f'ON CONFLICT ({conflict}) DO UPDATE SET ' + ",".join(f'"{name}"=excluded."{name}"' for name in updates))
                else:
                    sql = f'INSERT INTO "{table}" ({",".join(chr(34) + name + chr(34) for name in names)}) VALUES ({placeholders})'
                target.execute(sql, [values[name] for name in names])
            for rejected_row in quarantined:
                values = rejected_row["mapped"]
                names = list(values)
                target.execute(
                    f'INSERT INTO "{quarantine_table}" ({",".join(chr(34) + name + chr(34) for name in names)}) '
                    f'VALUES ({",".join("?" for _ in names)})',
                    [values[name] for name in names],
                )
            target.commit()
        finally:
            target.close()
        self._write_quarantine_file(quarantined)
        return len(valid), len(quarantined)


def execute(context: RuntimeContext) -> list[tuple[int, int]]:
    metadata: Metadata = context.values["metadata"]
    targets = target_names(context)
    mappings = [mapping for mapping in metadata.persistent if not targets or mapping.target_table in targets]
    processor_name = context.processor_parameters["processor_name"]
    if not mappings:
        raise ValueError(f"{processor_name} has no matching Persistent table mapping")
    return [
        execute_processor(context, RawToPersistentBlueprint(context, ControlService(context.control_db), mapping))
        for mapping in mappings
    ]