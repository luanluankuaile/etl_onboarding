import csv
import sqlite3
from pathlib import Path
from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.runner import ETLRunner

ROOT = Path(__file__).parents[1]


def _row(version, flag="N", acct_id="00001"):
    return {"acct_id": acct_id, "bill_cyc_cd": "M", "setup_dt": "1/2/2024", "currency_cd": "USD", "acct_mgmt_grp": "GRP1", "bill_after_dt": "1/5/2024", "protect_cyc_sw": "N", "cis_division": "D1", "mailing_prem_id": "1001", "protect_prem_sw": "Y", "coll_cl_cd": "A", "cr_review_dt": "2/1/2024", "postpone_cr_rvw_dt": "", "int_cr_review_sw": "Y", "cust_cl_cd": "R", "bill_prt_intercept": "N", "no_dep_rvw_sw": flag, "version": version}


def test_ci_acct_first_file_wins_regardless_of_name(tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    fields = list(_row(1).keys())
    for name, flag in (("file_z.csv", "N"), ("file_a.csv", "Y")):
        with (landing / name).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerow(_row(1, flag))

    context = RuntimeContext("cross-file", landing_dir=landing, raw_db=tmp_path / "raw.sqlite", persistent_db=tmp_path / "persistent.sqlite", consumption_db=tmp_path / "consumption.sqlite", control_db=tmp_path / "control.sqlite")
    metadata = load_metadata(ROOT / "metadata/ci_acct.yml")
    ETLRunner(metadata, context).run()

    db = sqlite3.connect(context.persistent_db)
    assert db.execute("SELECT no_dep_rvw_sw FROM per_cust_ci_acct WHERE acct_id = '00001'").fetchone()[0] == "N"
    assert db.execute("SELECT _ingestion_timestamp FROM raw_cust_ci_acct").fetchone()[0]
    db.close()
