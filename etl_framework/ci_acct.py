"""CI_ACCT onboarding processor with deterministic duplicate and replay handling."""
import csv
import hashlib
from datetime import datetime
from .context import utc_now
from .sqlite import connect, create_table
from .landing import discover_csv

COLUMNS = ["acct_id", "bill_cyc_cd", "setup_dt", "currency_cd", "acct_mgmt_grp", "bill_after_dt", "protect_cyc_sw", "cis_division", "mailing_prem_id", "protect_prem_sw", "coll_cl_cd", "cr_review_dt", "postpone_cr_rvw_dt", "int_cr_review_sw", "cust_cl_cd", "bill_prt_intercept", "no_dep_rvw_sw", "version"]
DATES = {"setup_dt", "bill_after_dt", "cr_review_dt", "postpone_cr_rvw_dt"}


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


def run_ci_acct(context, control, pattern="*.csv"):
    files = discover_csv(context, control, pattern)
    raw = connect(context.raw_db)
    persistent = connect(context.persistent_db)
    landing_cols = [(c, "TEXT", True) for c in COLUMNS] + [(c, "TEXT", True) for c in ["_source_file_name", "_source_file_path", "_ingestion_timestamp", "_ingestion_batch_id", "_source_file_checksum"]]
    raw_cols = [(c, "INTEGER" if c == "version" else "TEXT", True) for c in COLUMNS] + [("row_status", "TEXT", False), ("_source_file_name", "TEXT", False)]
    create_table(raw, "land_cust_ci_acct", landing_cols)
    create_table(raw, "raw_cust_ci_acct", raw_cols)
    create_table(raw, "raw_cust_ci_acct__quarantine", raw_cols)
    audit = [("run_id", "TEXT", False), ("environment", "TEXT", False), ("latest_update_datetime", "TEXT", False), ("latest_insert_datetime", "TEXT", False), ("_record_hash", "TEXT", False)]
    create_table(persistent, "per_cust_ci_acct", [(c, "INTEGER" if c == "version" else "TEXT", c == "acct_id") for c in COLUMNS] + audit, ["acct_id"])

    seen = set()
    for path in files:
        checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        now = utc_now()
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != COLUMNS:
                raise ValueError(f"CI_ACCT header mismatch in {path.name}: expected {COLUMNS}, got {reader.fieldnames}")
            for source in reader:
                if None in source:
                    raise ValueError(f"CI_ACCT row has more fields than the declared header in {path.name}")
                landing = [source.get(c) for c in COLUMNS] + [path.name, str(path), now, context.run_id, checksum]
                raw.execute('INSERT INTO "land_cust_ci_acct" VALUES (' + ','.join('?' for _ in landing) + ')', landing)
                status = "valid"
                try:
                    row = _clean(source)
                except (TypeError, ValueError):
                    row = {c: None for c in COLUMNS}
                    status = "invalid"
                if not row.get("acct_id"):
                    status = "invalid"
                elif row["acct_id"] in seen:
                    status = "duplicate"
                seen.add(row.get("acct_id"))
                vals = [row[c] for c in COLUMNS] + [status, path.name]
                target = "raw_cust_ci_acct" if status in ("valid", "duplicate") else "raw_cust_ci_acct__quarantine"
                raw.execute('INSERT INTO "' + target + '" VALUES (' + ','.join("?" for _ in vals) + ')', vals)

    # Deduplicate within each physical file first. DESC rowid makes the last
    # source row win for an equal (acct_id, version) key.
    rows = raw.execute("SELECT rowid, * FROM raw_cust_ci_acct WHERE row_status IN ('valid', 'duplicate') AND acct_id IS NOT NULL ORDER BY acct_id, version, _source_file_name, rowid DESC").fetchall()
    seen_keys = set()
    candidates = []
    for row in rows:
        key = (row["_source_file_name"], row["acct_id"], row["version"])
        if key not in seen_keys:
            candidates.append(row)
            seen_keys.add(key)

    # Across files/runs, retain the first candidate at an equal version and
    # retain only the highest version for each account.
    winners = {}
    for row in sorted(candidates, key=lambda item: item["rowid"]):
        account = row["acct_id"]
        if account not in winners or row["version"] > winners[account]["version"]:
            winners[account] = row

    for row in winners.values():
        payload = [row[c] for c in COLUMNS]
        record_hash = hashlib.sha256("|".join("" if value is None else str(value) for value in payload).encode()).hexdigest()
        existing = persistent.execute("SELECT version FROM per_cust_ci_acct WHERE acct_id = ?", (row["acct_id"],)).fetchone()
        if existing is not None and row["version"] <= existing["version"]:
            continue
        stamp = utc_now()
        if existing is None:
            values = payload + [context.run_id, context.environment, stamp, stamp, record_hash]
            persistent.execute('INSERT INTO per_cust_ci_acct VALUES (' + ','.join('?' for _ in values) + ')', values)
        else:
            assignments = ", ".join(f'"{column}" = ?' for column in COLUMNS[1:])
            persistent.execute(f'UPDATE per_cust_ci_acct SET {assignments}, environment = ?, latest_update_datetime = ?, _record_hash = ? WHERE acct_id = ?', payload[1:] + [context.environment, stamp, record_hash, row["acct_id"]])
    raw.commit()
    persistent.commit()
    raw.close()
    persistent.close()
    for path in files:
        checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        control.record_manifest(str(path), path.stat().st_size, path.stat().st_mtime, context.run_id, utc_now(), checksum)
    control.rows(context.run_id, "ci_acct", "persistent", "per_cust_ci_acct", len(winners), 0)
