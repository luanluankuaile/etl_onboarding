"""Raw CI_ACCT transformation with auditable duplicate policy.

Duplicate (acct_id, version) rows are expected corrections. One deterministic
winner is retained; discarded physical rows are represented in the audit list.
The business-record hash intentionally excludes technical metadata and is built
from normalized mapped business columns only.
"""
from __future__ import annotations
from collections.abc import Iterable
from collections import Counter
from hashlib import sha256
import json


def cast_version(value: str | int | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def deduplicate(rows: Iterable[dict], key: tuple[str, str] = ("acct_id", "version"), audit: list[dict] | None = None) -> list[dict]:
    rows = list(rows)
    normalized = []
    for original in rows:
        row = dict(original)
        if row.get("acct_id") is not None:
            row["acct_id"] = str(row["acct_id"]).strip()
        row["version"] = cast_version(row.get("version"))
        normalized.append(row)
    counts = Counter(tuple(row.get(k) for k in key) for row in normalized)
    selected: dict[tuple, dict] = {}
    for row in normalized:
        composite = tuple(row.get(k) for k in key)
        try:
            row_number = int(row.get("source_row_number", 0))
        except (TypeError, ValueError):
            row_number = 0
        sort_key = (str(row.get("source_file_modified_ts", "")), str(row.get("ingestion_ts", "")), row_number, str(row.get("source_file_checksum", "")))
        if composite not in selected or sort_key > selected[composite]["_sort_key"]:
            row["_sort_key"] = sort_key
            selected[composite] = row
    result = []
    for row in selected.values():
        row.pop("_sort_key", None)
        business = tuple("" if row.get(k) is None else str(row[k]).strip() for k in sorted(row) if not k.startswith("source_") and k not in {"ingestion_ts", "ingestion_run_id", "record_status"})
        canonical = {k: "" if row.get(k) is None else str(row[k]).strip() for k in sorted(row) if not k.startswith("source_") and k not in {"ingestion_ts", "ingestion_run_id", "record_status", "record_hash"}}
        row["record_hash"] = sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()
        row["dq_status"] = "VALID" if row.get("acct_id") is not None and row.get("version") is not None and row["version"] >= 0 else "REJECTED"
        if counts[tuple(row.get(k) for k in key)] > 1 and audit is not None:
            audit.append({"rule_id": "CI_ACCT_DQ_003", "status": "WARN", "key": tuple(row.get(k) for k in key), "reason": "deterministic source metadata winner retained"})
        result.append(row)
    return result
