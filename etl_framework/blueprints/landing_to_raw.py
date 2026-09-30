"""Landing-to-Raw Blueprint and workflow notebook entry point."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from ..context import RuntimeContext, utc_now
from ..control import ControlService
from ..landing import discover_csv, read_csv
from ..metadata import Metadata
from ..sqlite import connect, create_table
from .base import AUDIT_COLUMNS, Blueprint, execute_processor

RAW_AUDIT_COLUMNS = [
    ("arrival_date", "TEXT", False),
    ("source_file_name", "TEXT", False),
    ("source_file_path", "TEXT", False),
    ("source_file_checksum", "TEXT", False),
    ("source_row_number", "INTEGER", False),
] + AUDIT_COLUMNS


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


def execute(context: RuntimeContext) -> int:
    metadata: Metadata = context.values["metadata"]
    blueprint = LandingToRawBlueprint(context, ControlService(context.control_db), metadata)
    return execute_processor(context, blueprint)