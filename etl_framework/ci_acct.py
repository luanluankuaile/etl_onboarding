"""Metadata-selected CI_ACCT processor with idempotent version-aware merge."""
import csv
import hashlib
from datetime import datetime
from .context import utc_now
from .landing import discover_csv, file_checksum
from .sqlite import connect, create_table

COLUMNS = ["acct_id", "bill_cyc_cd", "setup_dt", "currency_cd", "acct_mgmt_grp", "bill_after_dt", "protect_cyc_sw", "cis_division", "mailing_prem_id", "protect_prem_sw", "coll_cl_cd", "cr_review_dt", "postpone_cr_rvw_dt", "int_cr_review_sw", "cust_cl_cd", "bill_prt_intercept", "no_dep_rvw_sw", "version"]
DATES = {"setup_dt", "bill_after_dt", "cr_review_dt", "postpone_cr_rvw_dt"}


def _date(value):
    if value is None or not value.strip():
        return None
    return datetime.strptime(value.strip(), "%m/%d/%Y").date().isoformat()


def _clean(source):
    row = {c: (source.get(c) or "").strip() or None for c in COLUMNS}
    for column in DATES:
        row[column] = _date(row[column])
    if row["version"] is None:
        raise ValueError("version is required")
    if not row["version"].isdigit():
        raise ValueError("version must be a non-negative decimal string")
    return row


def run_ci_acct(context, control, pattern="*.csv"):
    files = discover_csv(context, control, pattern)
    raw, persistent = connect(context.raw_db), connect(context.persistent_db)
    raw_cols = [(c, "TEXT", True) for c in COLUMNS] + [("row_status", "TEXT", False)]
    create_table(raw, "raw_cust_ci_acct", raw_cols)
    create_table(raw, "raw_cust_ci_acct__quarantine", raw_cols)
    audit = [("run_id", "TEXT", False), ("environment", "TEXT", False), ("latest_update_datetime", "TEXT", False), ("latest_insert_datetime", "TEXT", False), ("_record_hash", "TEXT", False)]
    create_table(persistent, "per_cust_ci_acct", [(c, "TEXT", c == "acct_id") for c in COLUMNS] + audit, ["acct_id"])
    winners = {}
    for path in files:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != COLUMNS:
                raise ValueError(f"CI_ACCT header mismatch in {path.name}")
            for source in reader:
                status = "valid"
                try:
                    row = _clean(source)
                    if not row["acct_id"]:
                        raise ValueError("acct_id is required")
                except (TypeError, ValueError):
                    row = {c: None for c in COLUMNS}; status = "invalid"
                if status == "invalid":
                    raw.execute('INSERT INTO "raw_cust_ci_acct__quarantine" VALUES (' + ','.join('?' for _ in raw_cols) + ')', [row[c] for c in COLUMNS] + [status])
                    continue
                key = row["acct_id"]
                previous = winners.get(key)
                if previous is None or row["version"] > previous["version"]:
                    winners[key] = row
                raw.execute('INSERT INTO "raw_cust_ci_acct" VALUES (' + ','.join('?' for _ in raw_cols) + ')', [row[c] for c in COLUMNS] + [status])
        control.record_manifest(str(path), path.stat().st_size, path.stat().st_mtime, context.run_id, utc_now(), file_checksum(path))
    for key, row in winners.items():
        payload = [row[c] for c in COLUMNS]
        digest = hashlib.sha256("|".join(str(v or "") for v in payload).encode()).hexdigest()
        existing = persistent.execute('SELECT version, run_id, latest_insert_datetime FROM per_cust_ci_acct WHERE acct_id=?', (key,)).fetchone()
        if existing and row["version"] < existing[0]:
            continue
        now = utc_now()
        if existing is None:
            values = payload + [context.run_id, context.environment, now, now, digest]
            persistent.execute('INSERT INTO per_cust_ci_acct VALUES (' + ','.join('?' for _ in values) + ')', values)
        else:
            assignments = ", ".join(f'"{c}"=?' for c in COLUMNS[1:])
            persistent.execute(f'UPDATE per_cust_ci_acct SET {assignments}, environment=?, latest_update_datetime=?, _record_hash=? WHERE acct_id=?', payload[1:] + [context.environment, now, digest, key])
    raw.commit(); persistent.commit(); raw.close(); persistent.close()
    control.rows(context.run_id, "ci_acct", "persistent", "per_cust_ci_acct", len(winners), 0)
