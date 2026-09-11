"""Metadata-driven CI_ACCT landing/raw/persistent processing primitives.

The SharePoint adapter is intentionally injected: deployments provide the
approved connector, while this module owns validation, idempotency, DQ, and
watermark-safe sequencing.
"""
from __future__ import annotations

import csv
import hashlib
from collections.abc import Callable, Iterable, MutableMapping
from pathlib import Path
from typing import Any

from .ci_acct_persistent import merge_scd1, reduce_to_latest

REQUIRED_COLUMNS = ("acct_id", "version")


def checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def discover_and_validate(paths: Iterable[Path], processed_checksums: set[str] | None = None) -> list[dict[str, Any]]:
    """Read valid CSV files and attach audit metadata; skip known checksums."""
    result = []
    processed_checksums = processed_checksums if processed_checksums is not None else set()
    for path in sorted(paths):
        file_checksum = checksum(path)
        if file_checksum in processed_checksums:
            continue
        processed_checksums.add(file_checksum)
        with path.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if not reader.fieldnames or not set(REQUIRED_COLUMNS).issubset(reader.fieldnames):
                raise ValueError(f"{path}: missing required columns {REQUIRED_COLUMNS}")
            for number, source in enumerate(reader, 1):
                row = {key: value.strip() if isinstance(value, str) else value for key, value in source.items()}
                row.update(source_file=str(path), source_file_checksum=file_checksum,
                           source_row_number=number)
                row["dq_status"] = "REJECTED" if not row.get("acct_id") else "VALID"
                result.append(row)
    return result


def raw_select(rows: Iterable[dict[str, Any]], watermark: int | None = None) -> list[dict[str, Any]]:
    """Cast version and retain only valid records newer than the watermark."""
    standardized = []
    for row in rows:
        if row.get("dq_status") == "REJECTED":
            continue
        try:
            row = dict(row)
            row["acct_id"] = str(row["acct_id"]).strip()
            row["version"] = int(row["version"])
        except (KeyError, TypeError, ValueError):
            continue
        if watermark is None or row["version"] > watermark:
            standardized.append(row)
    return reduce_to_latest(standardized)


def process_ci_acct(paths: Iterable[Path], target: MutableMapping, watermark: int | None,
                    advance_watermark: Callable[[int], None]) -> tuple[int, int, int]:
    """Execute layers in order; watermark callback runs only after successful merge."""
    landed = discover_and_validate(paths)
    incoming = raw_select(landed, watermark)
    inserted, updated = merge_scd1(target, incoming)
    if incoming:
        advance_watermark(max(row["version"] for row in incoming))
    return len(landed), inserted, updated
