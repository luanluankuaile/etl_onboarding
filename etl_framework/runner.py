import csv
import sqlite3
import uuid
from datetime import date, datetime, timezone
import re
from pathlib import Path
from .context import RuntimeContext, utc_now
from .control import ControlService
from .landing import discover_csv, read_csv
from .metadata import Metadata, TableMapping
from .sqlite import connect, create_table
from .transforms import run_sql


def cast(value, data_type):
    """Cast a source value and validate the supported metadata types strictly."""
    if value is None or (isinstance(value, str) and value == ""):
        return None
    kind = data_type.upper()
    if kind.startswith("INT"):
        return int(str(value).strip())
    if kind.startswith(("REAL", "FLOAT", "DECIMAL")):
        return float(str(value).strip())
    if kind == "DATE":
        text = str(value)
        if not re.fullmatch(r"\\d{4}-\\d{2}-\\d{2}", text):
            raise ValueError("DATE must use YYYY-MM-DD format")
        date.fromisoformat(text)
        return text
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
        watermark_key = mapping.keys[0] if mapping.keys else None
        for source_row in rows:
            try:
                values = [cast(source_row[c.source or c.name], c.data_type) for c in mapping.columns]
                invalid = any(v is None and not c.nullable for v, c in zip(values, mapping.columns))
                current = (cast(source_row[mapping.watermark_column], mapping.watermark_type)
                           if mapping.watermark_column else None)
                key_value = source_row[watermark_key] if watermark_key else "__table__"
                if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
                    invalid = True
            except (TypeError, ValueError):
                values = [source_row[c.source or c.name] for c in mapping.columns]
                current = None
                key_value = source_row[watermark_key] if watermark_key else "__table__"
                invalid = True

            # Validate and apply the per-key watermark before deduplication. An invalid
            # duplicate therefore cannot hide a later valid row.
            stored = (self.control.watermark_for_key(mapping.source_table, key_value)
                      if mapping.watermark_column else None)
            stored_value = cast(stored, mapping.watermark_type) if stored is not None else None
            if not invalid and current is not None and stored_value is not None and current <= stored_value:
                continue
            dedup = tuple(source_row[k] for k in mapping.deduplicate_by) if mapping.deduplicate_by else None
            if dedup is not None and dedup in seen:
                continue
            if invalid:
                target = quarantine; rejected += 1
            else:
                target = mapping.target_table; inserted += 1
                if mapping.watermark_column and current is not None:
                    self.control.set_watermark_for_key(mapping.source_table, key_value, str(current))
            if dedup is not None and not invalid:
                seen.add(dedup)

            now = utc_now()
            insert_audit = now
            if target == mapping.target_table and not invalid and mapping.keys:
                existing = persistent.execute(
                    f'SELECT latest_insert_datetime FROM "{mapping.target_table}" WHERE "{mapping.keys[0]}"=?',
                    (key_value,)).fetchone()
                if existing:
                    insert_audit = existing[0]
            values += [self.context.run_id, self.context.environment, now, insert_audit]
            cols = [c.name for c in mapping.columns] + [a[0] for a in audit]
            sql = f'INSERT OR REPLACE INTO "{target}" ({",".join(chr(34)+c+chr(34) for c in cols)}) VALUES ({",".join("?" for _ in cols)})'
            persistent.execute(sql, values)
        persistent.commit()
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
