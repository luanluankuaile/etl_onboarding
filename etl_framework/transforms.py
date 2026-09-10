import sqlite3
from pathlib import Path


def run_sql(sql: str, source_db, target_db):
    target = sqlite3.connect(target_db)
    try:
        target.execute("ATTACH DATABASE ? AS source", (str(Path(source_db)),))
        target.executescript(sql)
        target.commit()
    finally:
        target.close()
