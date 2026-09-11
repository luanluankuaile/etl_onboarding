"""Pure, testable Raw-layer transformations for CI_ACCT."""
from __future__ import annotations
from collections.abc import Iterable
from hashlib import sha256


def cast_version(value: str | int | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def deduplicate(rows: Iterable[dict], key: tuple[str, str] = ("acct_id", "version")) -> list[dict]:
    """Keep the newest row for each composite key using approved ordering."""
    selected: dict[tuple, dict] = {}
    for row in rows:
        row = dict(row)
        row["version"] = cast_version(row.get("version"))
        composite = tuple(row.get(k) for k in key)
        try:
            row_number = int(row.get("source_row_number", 0))
        except (TypeError, ValueError):
            row_number = 0
        sort_key = (row.get("source_file_modified_ts", ""), row.get("ingestion_ts", ""), row_number)
        if composite not in selected or sort_key > selected[composite]["_sort_key"]:
            row["_sort_key"] = sort_key
            selected[composite] = row
    result = []
    for row in selected.values():
        row.pop("_sort_key", None)
        row["record_hash"] = sha256(repr(sorted(row.items())).encode()).hexdigest()
        row["dq_status"] = "VALID" if row.get("acct_id") is not None and row.get("version") is not None and row["version"] >= 0 else "REJECTED"
        result.append(row)
    return result
