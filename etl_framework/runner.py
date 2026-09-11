import csv
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from .context import RuntimeContext, utc_now
from .control import ControlService
from .landing import discover_csv, read_csv
from .metadata import Metadata, TableMapping
from .sqlite import connect, create_table
from .transforms import run_sql


def cast(value, data_type):
    """Cast a source value and validate the small SQLite type vocabulary."""
    if value in (None, ""):
        return None
    kind = data_type.upper()
    if kind.startswith("INT"):
        return int(value)
    if kind.startswith(("REAL", "FLOAT", "DECIMAL")):
        return float(value)
    if kind == "DATE":
        datetime.fromisoformat(value)
        return value
    return value


class ETLRunner:
    def __init__(self, metadata: Metadata, context: RuntimeContext):
        self.metadata, self.context = metadata, context
        self.control = ControlService(context.control_db)

    def landing_to_raw(self):
        processor = "landing_to_raw"
        started = utc_now(); self.control.processor(self.context.run_id, processor, "RUNNING", started)
        files = discover_csv(self.context, self.control, self.metadata.landing.get("pattern", "*.csv"))
        conn = connect(self.context.raw_db)
        for path in files:
            rows = read_csv(path)
            if not rows:
                continue
            table = self.metadata.landing.get("raw_table", path.stem)
            expected = [c.source or c.name for mapping in self.metadata.persistent
                        if mapping.source_table == table for c in mapping.columns]
            actual = list(rows[0])
            if expected and actual != expected:
                raise ValueError(f"{path.name}: expected columns {expected}, got {actual}")
            columns = [(name, "TEXT", True) for name in actual]
            create_table(conn, table, columns)
            for row in rows:
                names, vals = zip(*row.items())
                conn.execute(f'INSERT INTO "{table}" ({",".join(chr(34)+n+chr(34) for n in names)}) VALUES ({",".join("?" for _ in vals)})', vals)
            self.control.rows(self.context.run_id, "landing_to_raw", "raw", table, len(rows))
        conn.commit(); conn.close()
        self.control.processor(self.context.run_id, processor, "SUCCEEDED", started, utc_now())

    def raw_to_persistent(self, mapping: TableMapping):
        processor = "raw_to_persistent"
        started = utc_now(); self.control.processor(self.context.run_id, processor, "RUNNING", started)
        raw, persistent = connect(self.context.raw_db), connect(self.context.persistent_db)
        audit = [("run_id", "TEXT", False), ("environment", "TEXT", False), ("latest_update_datetime", "TEXT", False), ("latest_insert_datetime", "TEXT", False)]
        create_table(persistent, mapping.target_table, [(c.name, c.data_type, c.nullable) for c in mapping.columns] + audit, mapping.keys)
        quarantine = mapping.dq_quarantine_table or mapping.target_table + "__quarantine"
        create_table(persistent, quarantine, [(c.name, c.data_type, True) for c in mapping.columns] + audit)
        rows = raw.execute(f'SELECT * FROM "{mapping.source_table}"').fetchall()
        seen = set(); inserted = rejected = 0
        watermark = self.control.watermark(mapping.source_table)
        max_watermark = watermark
        for source_row in rows:
            try:
                values = [cast(source_row[c.source or c.name], c.data_type) for c in mapping.columns]
                invalid = any(v is None and not c.nullable for v, c in zip(values, mapping.columns))
            except (TypeError, ValueError):
                values = [source_row[c.source or c.name] for c in mapping.columns]
                invalid = True
            current = source_row[mapping.watermark_column] if mapping.watermark_column else None
            if watermark is not None and current is not None and current <= watermark:
                continue
            if invalid: target, rejected = quarantine, rejected + 1
            else: target, inserted = mapping.target_table, inserted + 1
            if current is not None and (max_watermark is None or current > max_watermark):
                max_watermark = current
            now = utc_now(); values += [self.context.run_id, self.context.environment, now, now]
            if mapping.deduplicate_by:
                dedup = tuple(source_row[k] for k in mapping.deduplicate_by)
                if dedup in seen: continue
                seen.add(dedup)
            cols = [c.name for c in mapping.columns] + [a[0] for a in audit]
            sql = f'INSERT OR REPLACE INTO "{target}" ({",".join(chr(34)+c+chr(34) for c in cols)}) VALUES ({",".join("?" for _ in cols)})'
            persistent.execute(sql, values)
        persistent.commit()
        if mapping.watermark_column and max_watermark is not None:
            self.control.set_watermark(mapping.source_table, max_watermark)
        raw.close(); persistent.close()
        self.control.rows(self.context.run_id, processor, "persistent", mapping.target_table, inserted, rejected)
        self.control.processor(self.context.run_id, processor, "SUCCEEDED", started, utc_now())

    def run(self):
        started = utc_now(); self.control.run(self.context.run_id, self.context.environment, "RUNNING", started)
        try:
            self.landing_to_raw()
            for mapping in self.metadata.persistent: self.raw_to_persistent(mapping)
            for item in self.metadata.consumption: run_sql(item["sql"], self.context.persistent_db, self.context.consumption_db)
            self.control.run(self.context.run_id, self.context.environment, "SUCCEEDED", started, utc_now())
        except Exception as exc:
            self.control.run(self.context.run_id, self.context.environment, "FAILED", started, utc_now(), str(exc)); raise
