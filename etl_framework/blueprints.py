"""Layer-specific processors for the local SQLite ETL architecture."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .context import RuntimeContext, utc_now
from .control import ControlService
from .landing import discover_csv, read_csv
from .metadata import Metadata, TableMapping
from .sqlite import connect, create_table
from .transforms import run_sql

AUDIT_COLUMNS = [
    ("run_id", "TEXT", False),
    ("environment", "TEXT", False),
    ("latest_update_datetime", "TEXT", False),
    ("latest_insert_datetime", "TEXT", False),
]
RAW_AUDIT_COLUMNS = [
    ("arrival_date", "TEXT", False),
    ("source_file_name", "TEXT", False),
    ("source_file_path", "TEXT", False),
    ("source_file_checksum", "TEXT", False),
    ("source_row_number", "INTEGER", False),
] + AUDIT_COLUMNS


def cast(value: Any, data_type: str) -> Any:
    if value in (None, ""):
        return None
    normalized = data_type.upper()
    if normalized.startswith("INT"):
        return int(value)
    if normalized.startswith(("REAL", "FLOAT")):
        return float(value)
    # Exact financial DECIMAL values remain text in SQLite.
    return str(value) if normalized.startswith("DECIMAL") else value


class Blueprint:
    def __init__(self, context: RuntimeContext, control: ControlService):
        self.context = context
        self.control = control

    def processor(self, processor_name: str):
        from .data_processor import DataProcessor
        return DataProcessor(processor_name, self)


class LandingToRawBlueprint(Blueprint):
    def __init__(self, context: RuntimeContext, control: ControlService, metadata: Metadata):
        super().__init__(context, control)
        self.metadata = metadata

    def extract(self) -> list[Path]:
        return discover_csv(self.context, self.control, self.metadata.landing.get("pattern", "*.csv"))

    def transform(self, files: list[Path]) -> list[tuple[Path, str, list[dict[str, Any]]]]:
        transformed = []
        for path in files:
            rows = read_csv(path)
            if not rows:
                continue
            checksum = hashlib.sha256(path.read_bytes()).hexdigest()
            now = utc_now()
            raw_rows = []
            for number, row in enumerate(rows, 1):
                raw_rows.append(dict(
                    row,
                    arrival_date=(self.context.process_date or now)[:10],
                    source_file_name=path.name,
                    source_file_path=str(path),
                    source_file_checksum=checksum,
                    source_row_number=number,
                    run_id=self.context.run_id,
                    environment=self.context.environment,
                    latest_update_datetime=now,
                    latest_insert_datetime=now,
                ))
            transformed.append((path, checksum, raw_rows))
        return transformed

    def load(self, files: list[tuple[Path, str, list[dict[str, Any]]]]) -> int:
        total = 0
        connection = connect(self.context.raw_db)
        try:
            for path, checksum, rows in files:
                table = self.metadata.landing.get("raw_table", path.stem)
                audit_names = {name for name, _, _ in RAW_AUDIT_COLUMNS}
                create_table(connection, table,
                             [(name, "TEXT", True) for name in rows[0] if name not in audit_names] + RAW_AUDIT_COLUMNS)
                for values in rows:
                    names = list(values)
                    connection.execute(
                        f'INSERT INTO "{table}" ({",".join(chr(34) + name + chr(34) for name in names)}) '
                        f'VALUES ({",".join("?" for _ in names)})', [values[name] for name in names]
                    )
                    total += 1
            connection.commit()
            for path, checksum, _ in files:
                table = self.metadata.landing.get("raw_table", path.stem)
                self.control.mark_file_processed(checksum, str(path), self.context.run_id, utc_now(), table)
                path.rename(path.with_suffix(path.suffix + ".processed"))
        finally:
            connection.close()
        return total

    def execute(self) -> int:
        return self.load(self.transform(self.extract()))


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

    def execute(self) -> tuple[int, int]:
        return self.load(self.transform(self.extract()))


class PersistentToConsumptionBlueprint(Blueprint):
    def __init__(self, context: RuntimeContext, control: ControlService, item: dict[str, Any]):
        super().__init__(context, control)
        self.item = item

    def extract(self) -> str:
        sql = self.item.get("sql") or self.item.get("mapping") or self.item.get("query_file")
        if not sql:
            raise ValueError(f"Consumption transformation {self.item.get('name')} has no SQL")
        return sql

    def transform(self, source: str) -> str:
        query_file = self.item.get("query_file")
        if query_file:
            source = Path(query_file).read_text(encoding="utf-8")
        if self.item.get("mapping"):
            target_table = self.item.get("table") or self.item.get("name")
            if not target_table:
                raise ValueError("Consumption mapping requires a target table or name")
            query = source.strip().rstrip(";")
            return f'DROP TABLE IF EXISTS "{target_table}"; CREATE TABLE "{target_table}" AS {query};'
        return source

    def load(self, sql: str) -> None:
        run_sql(sql, self.context.persistent_db, self.context.consumption_db)

    def execute(self) -> None:
        self.load(self.transform(self.extract()))