"""CI_ACCT onboarding processors; kept behind metadata-driven runner dispatch."""
import csv
import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from .context import utc_now
from .sqlite import connect, create_table
from .landing import discover_csv

COLUMNS = ["acct_id", "bill_cyc_cd", "setup_dt", "currency_cd", "acct_mgmt_grp", "bill_after_dt", "protect_cyc_sw", "cis_division", "mailing_prem_id", "protect_prem_sw", "coll_cl_cd", "cr_review_dt", "postpone_cr_rvw_dt", "int_cr_review_sw", "cust_cl_cd", "bill_prt_intercept", "no_dep_rvw_sw", "version"]
DATES = {"setup_dt", "bill_after_dt", "cr_review_dt", "postpone_cr_rvw_dt"}

def _date(value):
    if value is None or not value.strip(): return None
    return datetime.strptime(value.strip(), "%m/%d/%Y").date().isoformat()

def _clean(row):
    out = {c: (row.get(c) or "").strip() or None for c in COLUMNS}
    for c in DATES: out[c] = _date(out[c])
    if out["version"] is not None: out["version"] = int(out["version"])
    return out

def run_ci_acct(context, control, pattern="*.csv"):
    files = discover_csv(context, control, pattern)
    raw = connect(context.raw_db); persistent = connect(context.persistent_db)
    landing_cols = [(c, "TEXT", True) for c in COLUMNS] + [(c, "TEXT", True) for c in ["_source_file_name", "_source_file_path", "_ingestion_timestamp", "_ingestion_batch_id", "_source_file_checksum"]]
    raw_cols = [(c, "INTEGER" if c == "version" else "TEXT", True) for c in COLUMNS] + [("row_status", "TEXT", False)]
    create_table(raw, "land_cust_ci_acct", landing_cols)
    create_table(raw, "raw_cust_ci_acct", raw_cols)
    create_table(raw, "raw_cust_ci_acct__quarantine", raw_cols)
    audit = [("run_id", "TEXT", False), ("environment", "TEXT", False), ("latest_update_datetime", "TEXT", False), ("latest_insert_datetime", "TEXT", False), ("_record_hash", "TEXT", False)]
    create_table(persistent, "per_cust_ci_acct", [(c, "INTEGER" if c == "version" else "TEXT", c == "acct_id") for c in COLUMNS] + audit, ["acct_id"])
    seen = set()
    for path in files:
        checksum = hashlib.sha256(path.read_bytes()).hexdigest(); now = utc_now()
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for source in csv.DictReader(handle):
                landing = [source.get(c) for c in COLUMNS] + [path.name, str(path), now, context.run_id, checksum]
                raw.execute('INSERT INTO "land_cust_ci_acct" VALUES (' + ','.join('?' for _ in landing) + ')', landing)
                status = "valid"
                try: row = _clean(source)
                except (TypeError, ValueError): row, status = _clean({c: None for c in COLUMNS}), "invalid"
                if not row.get("acct_id"): status = "invalid"
                if row.get("acct_id") in seen: status = "duplicate"
                seen.add(row.get("acct_id"))
                vals = [row[c] for c in COLUMNS] + [status]
                target = "raw_cust_ci_acct" if status in ("valid", "duplicate") else "raw_cust_ci_acct__quarantine"
                raw.execute('INSERT INTO "' + target + '" VALUES (' + ','.join('?' for _ in vals) + ')', vals)
    # Highest version wins; INSERT OR REPLACE provides Type 1 overwrite semantics.
    rows = raw.execute("SELECT * FROM raw_cust_ci_acct WHERE row_status IN ('valid', 'duplicate') AND acct_id IS NOT NULL ORDER BY acct_id, version").fetchall()
    winners = {}
    for r in rows:
        if r['acct_id'] not in winners or (r['version'] or -1) >= (winners[r['acct_id']]['version'] or -1): winners[r['acct_id']] = r
    for r in winners.values():
        payload = [r[c] for c in COLUMNS]; record_hash = hashlib.sha256("|".join("" if x is None else str(x) for x in payload).encode()).hexdigest()
        stamp = utc_now(); values = payload + [context.run_id, context.environment, stamp, stamp, record_hash]
        persistent.execute('INSERT OR REPLACE INTO per_cust_ci_acct VALUES (' + ','.join('?' for _ in values) + ')', values)
    raw.commit(); persistent.commit(); raw.close(); persistent.close()
    control.rows(context.run_id, "ci_acct", "persistent", "per_cust_ci_acct", len(rows), 0)
