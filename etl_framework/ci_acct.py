"""CI_ACCT onboarding processor."""
import csv, hashlib
from datetime import datetime
from .context import utc_now
from .sqlite import connect, create_table
from .landing import discover_csv

COLUMNS = ["acct_id", "bill_cyc_cd", "setup_dt", "currency_cd", "acct_mgmt_grp", "bill_after_dt", "protect_cyc_sw", "cis_division", "mailing_prem_id", "protect_prem_sw", "coll_cl_cd", "cr_review_dt", "postpone_cr_rvw_dt", "int_cr_review_sw", "cust_cl_cd", "bill_prt_intercept", "no_dep_rvw_sw", "version"]
DATES = {"setup_dt", "bill_after_dt", "cr_review_dt", "postpone_cr_rvw_dt"}

def _clean(row, date_format):
    formats = {"M/d/yyyy": "%m/%d/%Y", "MM/dd/yyyy": "%m/%d/%Y", "yyyy-MM-dd": "%Y-%m-%d"}
    if date_format not in formats: raise ValueError(f"unsupported date format: {date_format}")
    out = {c: (row.get(c) or "").strip() or None for c in COLUMNS}
    for c in DATES:
        if out[c] is not None: out[c] = datetime.strptime(out[c], formats[date_format]).date().isoformat()
    if out["version"] is not None: int(out["version"])
    return out

def _version_order(value):
    return (0, 0, "") if value is None else (1, int(value), value)

def run_ci_acct(context, control, pattern="*.csv"):
    files = discover_csv(context, control, pattern); raw = connect(context.raw_db); persistent = connect(context.persistent_db)
    audit_cols = ["_source_file_name", "_source_file_path", "_ingestion_timestamp", "_ingestion_batch_id", "_source_file_checksum"]
    raw_audit = [(c, "TEXT", True) for c in audit_cols]
    raw_cols = [(c, "TEXT", True) for c in COLUMNS] + raw_audit + [("row_status", "TEXT", False)]
    create_table(raw, "land_cust_ci_acct", [(c, "TEXT", True) for c in COLUMNS] + raw_audit)
    create_table(raw, "raw_cust_ci_acct", raw_cols); create_table(raw, "raw_cust_ci_acct__quarantine", raw_cols)
    audit = [("run_id", "TEXT", False), ("environment", "TEXT", False), ("latest_update_datetime", "TEXT", False), ("latest_insert_datetime", "TEXT", False), ("_record_hash", "TEXT", False)]
    create_table(persistent, "per_cust_ci_acct", [(c, "TEXT", c == "acct_id") for c in COLUMNS] + audit, ["acct_id"])
    seen, rejected = set(), 0; date_format = context.values.get("ci_acct_date_format", "M/d/yyyy")
    for path in files:
        checksum, now = hashlib.sha256(path.read_bytes()).hexdigest(), utc_now()
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != COLUMNS: raise ValueError(f"CI_ACCT header mismatch in {path.name}: expected {COLUMNS}, got {reader.fieldnames}")
            for source in reader:
                if None in source: raise ValueError(f"CI_ACCT row has more fields than the declared header in {path.name}")
                raw.execute('INSERT INTO "land_cust_ci_acct" VALUES (' + ','.join('?' for _ in COLUMNS+audit_cols) + ')', [source.get(c) for c in COLUMNS] + [path.name, str(path), now, context.run_id, checksum])
                status = "valid"
                try: row = _clean(source, date_format)
                except (TypeError, ValueError): row, status, rejected = {c: (source.get(c) or "").strip() or None for c in COLUMNS}, "invalid", rejected + 1
                if not row.get("acct_id"): status, rejected = "invalid", rejected + (status == "valid")
                elif row["acct_id"] in seen: status = "duplicate"
                else: seen.add(row["acct_id"])
                vals = [row[c] for c in COLUMNS] + [path.name, str(path), now, context.run_id, checksum, status]
                target = "raw_cust_ci_acct" if status in ("valid", "duplicate") else "raw_cust_ci_acct__quarantine"
                raw.execute(f'INSERT INTO "{target}" VALUES (' + ','.join('?' for _ in vals) + ')', vals)
    rows = raw.execute("SELECT rowid,* FROM raw_cust_ci_acct WHERE row_status IN ('valid','duplicate') AND acct_id IS NOT NULL").fetchall(); winners = {}
    for r in rows:
        if r["acct_id"] not in winners or _version_order(r["version"]) >= _version_order(winners[r["acct_id"]]["version"]): winners[r["acct_id"]] = r
    for r in winners.values():
        payload = [r[c] for c in COLUMNS]; stamp = utc_now(); audit = [context.run_id, context.environment, stamp, stamp, hashlib.sha256("|".join(str(x or "") for x in payload).encode()).hexdigest()]
        persistent.execute('INSERT OR REPLACE INTO per_cust_ci_acct VALUES (' + ','.join('?' for _ in payload+audit) + ')', payload+audit)
    raw.commit(); persistent.commit(); raw.close(); persistent.close()
    for path in files: control.record_manifest(str(path), path.stat().st_size, path.stat().st_mtime, context.run_id, utc_now(), hashlib.sha256(path.read_bytes()).hexdigest())
    control.rows(context.run_id, "ci_acct", "persistent", "per_cust_ci_acct", len(winners), rejected)
