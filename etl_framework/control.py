import sqlite3
from pathlib import Path
from typing import Any


class ControlService:
    def __init__(self, path: str | Path):
        self.path = str(path)
        with self.connection() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, environment TEXT, started_at TEXT, ended_at TEXT, status TEXT, error TEXT);
            CREATE TABLE IF NOT EXISTS processors (run_id TEXT, processor TEXT, status TEXT, started_at TEXT, ended_at TEXT, error TEXT);
            CREATE TABLE IF NOT EXISTS row_counts (run_id TEXT, processor TEXT, layer TEXT, table_name TEXT, inserted INTEGER, rejected INTEGER);
            CREATE TABLE IF NOT EXISTS watermarks (source_table TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS file_manifests (path TEXT PRIMARY KEY, size INTEGER, modified REAL, checksum TEXT, processed_at TEXT, run_id TEXT);
            CREATE TABLE IF NOT EXISTS processed_file_ledger (checksum TEXT PRIMARY KEY, path TEXT, processed_at TEXT, run_id TEXT);
            """)

    def connection(self):
        return sqlite3.connect(self.path)

    def run(self, run_id: str, environment: str, status: str, started: str, ended: str | None = None, error: str | None = None):
        with self.connection() as c:
            c.execute("INSERT OR REPLACE INTO runs VALUES (?,?,?,?,?,?)", (run_id, environment, started, ended, status, error))

    def processor(self, run_id: str, name: str, status: str, started: str, ended: str | None = None, error: str | None = None):
        with self.connection() as c:
            c.execute("INSERT INTO processors VALUES (?,?,?,?,?,?)", (run_id, name, status, started, ended, error))

    def rows(self, run_id: str, processor: str, layer: str, table: str, inserted: int, rejected: int = 0):
        with self.connection() as c:
            c.execute("INSERT INTO row_counts VALUES (?,?,?,?,?,?)", (run_id, processor, layer, table, inserted, rejected))

    def watermark(self, table: str) -> str | None:
        with self.connection() as c:
            row = c.execute("SELECT value FROM watermarks WHERE source_table=?", (table,)).fetchone()
            return row[0] if row else None

    def set_watermark(self, table: str, value: str):
        with self.connection() as c:
            c.execute("INSERT OR REPLACE INTO watermarks VALUES (?,?)", (table, value))

    def manifest(self, path: str, size: int, modified: float, checksum: str, run_id: str, processed_at: str) -> bool:
        with self.connection() as c:
            existing = c.execute("SELECT size, modified, checksum FROM file_manifests WHERE path=?", (path,)).fetchone()
            if existing and existing == (size, modified, checksum): return False
            c.execute("INSERT OR REPLACE INTO file_manifests VALUES (?,?,?,?,?,?)", (path, size, modified, checksum, processed_at, run_id))
            return True

    def processed_checksums(self, source_table: str = "CI_ACCT") -> set[str]:
        with self.connection() as c:
            return {r[0] for r in c.execute("SELECT checksum FROM processed_file_ledger WHERE path LIKE ?", (source_table + ":%",))}

    def mark_file_processed(self, checksum: str, path: str, run_id: str, processed_at: str, source_table: str = "CI_ACCT") -> None:
        with self.connection() as c:
            c.execute("INSERT OR IGNORE INTO processed_file_ledger VALUES (?,?,?,?)", (checksum, source_table + ":" + path, processed_at, run_id))
