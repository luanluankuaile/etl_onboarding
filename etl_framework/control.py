import sqlite3
from pathlib import Path


class ControlService:
    """SQLite-backed control plane. Eligibility is read-only; recording is explicit."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        with self.connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, environment TEXT, started_at TEXT,
                    ended_at TEXT, status TEXT, error TEXT
                );
                CREATE TABLE IF NOT EXISTS processors (
                    run_id TEXT, processor TEXT, status TEXT, started_at TEXT,
                    ended_at TEXT, error TEXT
                );
                CREATE TABLE IF NOT EXISTS row_counts (
                    run_id TEXT, processor TEXT, layer TEXT, table_name TEXT,
                    inserted INTEGER, rejected INTEGER
                );
                CREATE TABLE IF NOT EXISTS watermarks (
                    source_table TEXT PRIMARY KEY, value TEXT
                );
                CREATE TABLE IF NOT EXISTS file_manifests (
                    path TEXT PRIMARY KEY, size INTEGER, modified REAL,
                    processed_at TEXT, run_id TEXT, checksum TEXT
                );
            """)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(file_manifests)")}
            # Five-column databases created by earlier releases are migrated in place.
            if "checksum" not in columns:
                conn.execute("ALTER TABLE file_manifests ADD COLUMN checksum TEXT")

    def connection(self):
        return sqlite3.connect(self.path)

    def run(self, run_id, environment, status, started, ended=None, error=None):
        with self.connection() as c:
            c.execute("INSERT OR REPLACE INTO runs VALUES (?,?,?,?,?,?)",
                      (run_id, environment, started, ended, status, error))

    def processor(self, run_id, name, status, started, ended=None, error=None):
        with self.connection() as c:
            c.execute("INSERT INTO processors VALUES (?,?,?,?,?,?)",
                      (run_id, name, status, started, ended, error))

    def rows(self, run_id, processor, layer, table, inserted, rejected=0):
        with self.connection() as c:
            c.execute("INSERT INTO row_counts VALUES (?,?,?,?,?,?)",
                      (run_id, processor, layer, table, inserted, rejected))

    def watermark(self, table):
        with self.connection() as c:
            row = c.execute("SELECT value FROM watermarks WHERE source_table=?", (table,)).fetchone()
            return row[0] if row else None

    def set_watermark(self, table, value):
        with self.connection() as c:
            c.execute("INSERT OR REPLACE INTO watermarks VALUES (?,?)", (table, value))

    def manifest_eligible(self, path, size, modified, checksum):
        """Return whether a file is new/changed without mutating the manifest."""
        with self.connection() as c:
            row = c.execute("SELECT size, modified, checksum FROM file_manifests WHERE path=?",
                            (path,)).fetchone()
        return row is None or row[0] != size or row[1] != modified or row[2] != checksum

    def record_manifest(self, path, size, modified, run_id, processed_at, checksum):
        with self.connection() as c:
            c.execute("""INSERT OR REPLACE INTO file_manifests
                        (path, size, modified, processed_at, run_id, checksum)
                        VALUES (?,?,?,?,?,?)""",
                      (path, size, modified, processed_at, run_id, checksum))

    # Compatibility for callers from the pre-checksum framework.
    def manifest(self, path, size, modified, run_id=None, processed_at=None, checksum=None):
        if checksum is None:
            checksum = ""
        return self.manifest_eligible(path, size, modified, checksum)
