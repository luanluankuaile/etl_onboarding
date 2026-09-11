"""Metadata-driven CI_ACCT landing validation and technical enrichment."""
from __future__ import annotations
import csv, hashlib
from pathlib import Path
from datetime import datetime, timezone

TECHNICAL_COLUMNS = ("ingestion_run_id", "source_file_name", "source_file_path", "source_file_modified_ts", "source_file_checksum", "source_row_number", "ingestion_ts", "source_system", "record_status")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_file(path: Path, expected_columns: list[str], encoding: str = "utf-8-sig", delimiter: str = ",") -> list[dict[str, str]]:
    """Validate non-empty CSV structure and return parsed rows."""
    if path.stat().st_size == 0:
        raise ValueError(f"empty CI_ACCT file: {path}")
    with path.open(newline="", encoding=encoding) as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        if reader.fieldnames != expected_columns:
            raise ValueError(f"header mismatch: expected {expected_columns}, got {reader.fieldnames}")
        rows = list(reader)
    return rows


def enrich_rows(rows: list[dict[str, str]], path: Path, run_id: str, source_system: str = "SHAREPOINT") -> list[dict[str, object]]:
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    modified = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
    now = utc_now()
    return [dict(row, ingestion_run_id=run_id, source_file_name=path.name,
                 source_file_path=str(path), source_file_modified_ts=modified,
                 source_file_checksum=checksum, source_row_number=i,
                 ingestion_ts=now, source_system=source_system, record_status="VALID")
            for i, row in enumerate(rows, 1)]
