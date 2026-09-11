"""CI_ACCT onboarding processor with deterministic duplicate and replay handling."""
import csv
import hashlib
from datetime import datetime

from .context import utc_now
from .sqlite import connect, create_table
from .landing import discover_csv

COLUMNS = [
    "acct_id", "bill_cyc_cd", "setup_dt", "currency_cd", "acct_mgmt_grp",
    "bill_after_dt", "protect_cyc_sw", "cis_division", "mailing_prem_id",
    "protect_prem_sw", "coll_cl_cd", "cr_review_dt", "postpone_cr_rvw_dt",
    "int_cr_review_sw", "cust_cl_cd", "bill_prt_intercept", "no_dep_rvw_sw",
    "version",
]
DATES = {"setup_dt", "bill_after_dt", "cr_review_dt", "postpone_cr_rvw_dt"}
AUDIT_COLUMNS = [
    "_source_file_name", "_source_file_path", "_ingestion_timestamp",
    "_ingestion_batch_id", "_source_file_checksum",
]


def _date(value):
    if value is None or not value.strip():
        return None
    return datetime.strptime(value.strip(), "%m/%d/%Y").date().isoformat()


def _clean(row):
    out = {c: (row.get(c) or "").strip() or None for c in COLUMNS}
    for column in DATES:
        out[column] = _date(out[column])
    if out["version"] is None:
        raise ValueError("version is required")
    try:
        out["version"] = int(out["version"])
    except (TypeError, ValueError) as exc:
        raise ValueError("version must be an integer") from exc
    if out["version"] < 0:
        raise ValueError("version must be non-negative")
    return out
