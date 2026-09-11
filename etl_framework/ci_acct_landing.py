"""CSV landing validation; parser behavior is explicit and auditable."""
from __future__ import annotations
import csv
import hashlib
from pathlib import Path
from datetime import datetime, timezone

TECHNICAL_COLUMNS = ("ingestion_run_id", "source_file_name", "source_file_path", "source_file_modified_ts", "source_file_checksum", "source_row_number", "ingestion_ts", "source_system", "record_status")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_file(path: Path, expected_columns: list[str], encoding: str = "utf-8-sig", delimiter: str = ",") -> list[dict[str, str]]:
    if path.stat().st_size == 0:
        raise ValueError(f"empty CI_ACCT file: {path}")
    try:
        with path.open(newline="", encoding=encoding) as handle:
            reader = csv.DictReader(handle, delimiter=delimiter, strict=True)
            if reader.fieldnames != expected_columns:
                raise ValueError(f"header mismatch: expected {expected_columns}, got {reader.fieldnames}")
            rows = []
            for row in reader:
                # DictReader uses None for missing fields and a None key for
                # extra fields. Reject both rather than silently truncating.
                if None in row or any(row.get(column) is None for column in expected_columns):
                    raise ValueError(f"row schema mismatch in {path}")
                rows.append(row)
    except (UnicodeDecodeError, csv.Error) as exc:
        raise ValueError(f"malformed CI_ACCT CSV: {path}") from exc
    return rows


def enrich_rows(rows: list[dict[str, str]], path: Path, run_id: str, source_system: str = "SHAREPOINT") -> list[dict[str, object]]:
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    modified = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
    now = utc_now()
    return [dict(row, ingestion_run_id=run_id, source_file_name=path.name, source_file_path=str(path), source_file_modified_ts=modified, source_file_checksum=checksum, source_row_number=i, ingestion_ts=now, source_system=source_system, record_status="VALID") for i, row in enumerate(rows, 1)]
