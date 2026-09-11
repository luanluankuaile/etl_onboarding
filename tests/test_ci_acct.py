import csv
from pathlib import Path
from etl_framework.ci_acct import run, BUSINESS


def row(acct, version, **kw):
    d={c:'' for c in BUSINESS}; d.update(acct_id=acct, version=str(version), **kw); return d


def test_ci_acct_scd1_dq_dedup_and_incremental(tmp_path: Path):
    src=tmp_path/'CI_ACCT_1.csv'; src.write_text('')
    rows=[{**row('A',1,attr_01='old'), '_path':src, '_ingestion_ts':'2024-01-01T00:00:00Z'},
          {**row('A',2,attr_01='new'), '_path':src, '_ingestion_ts':'2024-01-01T00:00:01Z'},
          {**row('A',1,attr_01='duplicate'), '_path':src, '_ingestion_ts':'2024-01-01T00:00:02Z'},
          {**row('',1), '_path':src, '_ingestion_ts':'2024-01-01T00:00:03Z'},
          {**row('B','x'), '_path':src, '_ingestion_ts':'2024-01-01T00:00:04Z'},
          {**row('C',-1), '_path':src, '_ingestion_ts':'2024-01-01T00:00:05Z'}]
    run(tmp_path/'raw.db',tmp_path/'persistent.db',rows,'batch-1')
    import sqlite3
    db=sqlite3.connect(tmp_path/'persistent.db')
    assert db.execute('select acct_id,version,attr_01 from per_cust_ci_acct').fetchall()==[('A',2,'new')]
    assert db.execute('select count(*) from raw_cust_ci_acct__quarantine').fetchone()[0]==3
    db.close()


def test_higher_version_wins_on_later_batch(tmp_path: Path):
    src=tmp_path/'CI_ACCT_1.csv'; src.write_text('')
    run(tmp_path/'r.db',tmp_path/'p.db',[{**row('A',1), '_path':src, '_ingestion_ts':'1'}],'b1')
    run(tmp_path/'r.db',tmp_path/'p.db',[{**row('A',0), '_path':src, '_ingestion_ts':'2'}, {**row('A',3), '_path':src, '_ingestion_ts':'3'}],'b2')
    import sqlite3
    db=sqlite3.connect(tmp_path/'p.db'); assert db.execute('select version from per_cust_ci_acct').fetchone()[0]==3
