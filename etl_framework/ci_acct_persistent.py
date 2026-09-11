"""Deterministic SCD1 current-state merge for CI_ACCT.

The source contract is one eligible row per business key.  This module enforces
that contract before applying the target watermark, so a batch containing many
versions cannot produce a multi-match merge.
"""
from __future__ import annotations
from collections.abc import Iterable, MutableMapping


def _version(row: dict, field: str) -> int | None:
    try:
        return None if row.get(field) in (None, "") else int(row[field])
    except (TypeError, ValueError):
        return None


def _tie_key(row: dict) -> tuple:
    """Stable winner ordering for equal versions and repeated physical rows."""
    try:
        row_number = int(row.get("source_row_number", 0))
    except (TypeError, ValueError):
        row_number = 0
    return (str(row.get("source_file_modified_ts", "")),
            str(row.get("ingestion_ts", "")), row_number,
            str(row.get("source_file_checksum", "")),
            repr(sorted(row.items())))


def reduce_to_latest(rows: Iterable[dict], key: str = "acct_id", version: str = "version") -> list[dict]:
    """Return exactly one valid, highest-version row for each business key."""
    selected: dict[object, dict] = {}
    for original in rows:
        row = dict(original)
        if row.get("dq_status") == "REJECTED" or row.get(key) is None:
            continue
        parsed = _version(row, version)
        if parsed is None:
            continue
        row[version] = parsed
        current = selected.get(row[key])
        if current is None or (parsed, _tie_key(row)) > (current[version], _tie_key(current)):
            selected[row[key]] = row
    return list(selected.values())


def merge_scd1(target: MutableMapping, incoming: Iterable[dict], key: str = "acct_id", version: str = "version") -> tuple[int, int]:
    """Insert/update SCD1 state; incoming versions <= target never overwrite."""
    inserted = updated = 0
    for row in reduce_to_latest(incoming, key, version):
        current = target.get(row[key])
        if current is None:
            target[row[key]] = dict(row)
            inserted += 1
        elif _version(row, version) is not None and _version(current, version) is not None and row[version] > _version(current, version):
            target[row[key]] = dict(row)
            updated += 1
    return inserted, updated
