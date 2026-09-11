import csv
import sqlite3
from pathlib import Path
from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.runner import ETLRunner

ROOT = Path(__file__).parents[1]


def _run(tmp_path, rows, run_id):
    landing = tmp_path / "landing"
    landing.mkdir(exist_ok=True)
    with (landing / f"ci_acct_{run_id}.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0])
        writer.writeheader(); writer.writerows(rows)
    db_root = tmp_path
    context = RuntimeContext(run_id, landing_dir=landing, raw_db=db_root/"raw.sqlite", persistent_db=db_root/"persistent.sqlite", consumption_db=db_root/"consumption.sqlite", control_db=db_root/"control.sqlite")
    ETLRunner(load_metadata(ROOT / "metadata/ci_acct.yml"), context).run()
    return context


def _row(version, flag="N"):
    return {"acct_id": "00001", "bill_cyc_cd": "M", "setup_dt": "1/2/2024", "currency_cd": "USD", "acct_mgmt_grp": "GRP1", "bill_after_dt": "1/5/2024", "protect_cyc_sw": "N", "cis_division": "D1", "mailing_prem_id": "1001", "protect_prem_sw": "Y", "coll_cl_cd": "A", "cr_review_dt": "2/1/2024", "postpone_cr_rvw_dt": "", "int_cr_review_sw": "Y", "cust_cl_cd": "R", "bill_prt_intercept": "N", "no_dep_rvw_sw": flag, "version": version}


def _persistent(context):
    db = sqlite3.connect(context.persistent_db)
    row = db.execute("select version, no_dep_rvw_sw, run_id, latest_insert_datetime, latest_update_datetime from per_cust_ci_acct where acct_id='00001'").fetchone()
    db.close()
    return row


def test_ci_acct_idempotent_reprocessing(tmp_path):
    context = _run(tmp_path, [_row("1")], "first")
    before = _persistent(context)
    # Re-running the same physical file is blocked by the control manifest.
    ETLRunner(load_metadata(ROOT / "metadata/ci_acct.yml"), RuntimeContext("replay", landing_dir=tmp_path / "landing", raw_db=tmp_path/"raw.sqlite", persistent_db=tmp_path/"persistent.sqlite", consumption_db=tmp_path/"consumption.sqlite", control_db=tmp_path/"control.sqlite")).run()
    after = _persistent(context)
    assert after == before


def test_ci_acct_older_version_not_overwrite(tmp_path):
    _run(tmp_path, [_row("2", "Y")], "v2")
    context = _run(tmp_path, [_row("1", "N")], "v1")
    assert _persistent(context)[:2] == (2, "Y")


def test_ci_acct_newer_version_update(tmp_path):
    first = _run(tmp_path, [_row("1", "N")], "v1")
    created = _persistent(first)[3]
    second = _run(tmp_path, [_row("2", "Y")], "v2")
    current = _persistent(second)
    assert current[:2] == (2, "Y")
    assert current[2] == "v1"
    assert current[3] == created
    assert current[4] >= created


def test_ci_acct_invalid_version_quarantine(tmp_path):
    rows = [_row("", "N"), _row("not-a-number", "N"), _row("-1", "N")]
    context = _run(tmp_path, rows, "invalid")
    db = sqlite3.connect(context.raw_db)
    assert db.execute("select count(*) from raw_cust_ci_acct__quarantine where row_status='invalid'").fetchone()[0] == 3
    assert db.execute("select count(*) from per_cust_ci_acct").fetchone()[0] == 0
    db.close()
