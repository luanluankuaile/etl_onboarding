"""CI_ACCT Landing, Raw and Persistent transformations."""
import hashlib
import sqlite3
from .context import utc_now

BUSINESS = ["acct_id", "version"] + [f"attr_{i:02d}" for i in range(1, 17)]
TECH = ["record_source", "source_file_name", "source_batch_id", "source_update_ts", "persistent_insert_ts", "persistent_update_ts"]

def valid(row):
    if not row.get("acct_id") or row.get("version") in (None, ""): return False
    try: return int(row["version"]) >= 0 and str(int(row["version"])).strip() == str(row["version"]).strip()
    except (TypeError, ValueError): return False

def enrich(row, path, batch, ingestion):
    d = dict(row); d.update(source_file_name=path.name, source_file_path=str(path), source_file_size=str(path.stat().st_size), source_file_modified_ts=str(path.stat().st_mtime), source_file_checksum=hashlib.sha256(path.read_bytes()).hexdigest(), ingestion_batch_id=batch, ingestion_ts=ingestion)
    return d

def run(raw_db, persistent_db, rows, batch):
    raw = sqlite3.connect(raw_db); out = sqlite3.connect(persistent_db)
    raw.row_factory = sqlite3.Row
    raw_cols = BUSINESS + ["source_file_name", "source_file_path", "source_file_size", "source_file_modified_ts", "source_file_checksum", "ingestion_batch_id", "ingestion_ts"]
    raw.execute('CREATE TABLE IF NOT EXISTS raw_cust_ci_acct (' + ','.join('"%s" TEXT'%c for c in raw_cols) + ', PRIMARY KEY (acct_id, version))')
    out.execute('CREATE TABLE IF NOT EXISTS per_cust_ci_acct (' + ','.join('"%s" %s'%(c, 'INTEGER' if c=='version' else 'TEXT') for c in BUSINESS+TECH) + ', PRIMARY KEY (acct_id))')
    out.execute('CREATE TABLE IF NOT EXISTS land_cust_ci_acct__quarantine (raw_payload TEXT, quarantine_reason TEXT, ingestion_batch_id TEXT)')
    out.execute('CREATE TABLE IF NOT EXISTS raw_cust_ci_acct__quarantine (raw_payload TEXT, quarantine_reason TEXT, ingestion_batch_id TEXT)')
    best = {}
    for row in rows:
        d = enrich(row, row['_path'], batch, row['_ingestion_ts']); d.pop('_path'); d.pop('_ingestion_ts')
        if not valid(d):
            out.execute('INSERT INTO raw_cust_ci_acct__quarantine VALUES (?,?,?)',(str(d),'invalid acct_id/version',batch)); continue
        key=(d['acct_id'],d['version']); rank=(d.get('source_file_modified_ts',''),d.get('source_file_name',''),d.get('ingestion_ts',''))
        if key not in best or rank > best[key][0]: best[key]=(rank,d)
    for _, d in best.values():
        vals=[d.get(c) for c in raw_cols]; raw.execute('INSERT OR REPLACE INTO raw_cust_ci_acct VALUES ('+','.join('?'*len(vals))+')',vals)
    for _, d in best.values():
        current = out.execute('SELECT version, persistent_insert_ts FROM per_cust_ci_acct WHERE acct_id=?', (d['acct_id'],)).fetchone()
        if current and int(d['version']) <= int(current[0]):
            continue
        now = utc_now()
        insert_ts = current[1] if current else now  # preserve original insert timestamp on updates
        vals = [d.get(c) for c in BUSINESS] + ['SharePoint.CI_ACCT', d.get('source_file_name'), d.get('ingestion_batch_id'), d.get('ingestion_ts'), insert_ts, now]
        if current:
            # UPDATE: set all BUSINESS cols (including acct_id) and all TECH cols except persistent_insert_ts
            update_cols = BUSINESS + [c for c in TECH if c != 'persistent_insert_ts']
            update_vals = [d.get(c) for c in BUSINESS] + ['SharePoint.CI_ACCT', d.get('source_file_name'), d.get('ingestion_batch_id'), d.get('ingestion_ts'), now]
            out.execute('UPDATE per_cust_ci_acct SET ' + ','.join('"%s"=?' % c for c in update_cols) + ' WHERE acct_id=?', update_vals + [d['acct_id']])
        else:
            out.execute('INSERT INTO per_cust_ci_acct VALUES (' + ','.join('?' * len(vals)) + ')', vals)
    raw.commit(); out.commit(); raw.close(); out.close()
