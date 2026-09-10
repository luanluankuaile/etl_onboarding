import sqlite3
from pathlib import Path
from typing import Iterable


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def create_table(conn, table: str, columns: Iterable[tuple[str, str, bool]], keys: list[str] = []):
    defs = [f'"{n}" {typ}' + (" NOT NULL" if not nullable else "") for n, typ, nullable in columns]
    if keys: defs.append("PRIMARY KEY (" + ",".join('"'+k+'"' for k in keys) + ")")
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(defs)})')
