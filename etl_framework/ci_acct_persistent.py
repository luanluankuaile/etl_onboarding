"""SCD1 current-state merge for CI_ACCT."""
from __future__ import annotations
from collections.abc import MutableMapping, Iterable


def merge_scd1(target: MutableMapping, incoming: Iterable[dict], key: str = "acct_id", version: str = "version") -> tuple[int, int]:
    """Insert new accounts and overwrite only when the incoming version is newer."""
    inserted = updated = 0
    for row in incoming:
        if row.get("dq_status") == "REJECTED" or row.get(key) is None:
            continue
        current = target.get(row[key])
        if current is None:
            target[row[key]] = dict(row); inserted += 1
        elif int(row[version]) > int(current[version]):
            target[row[key]] = dict(row); updated += 1
    return inserted, updated
