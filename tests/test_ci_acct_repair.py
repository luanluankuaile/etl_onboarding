import csv
import sqlite3
from pathlib import Path
from etl_framework.control import ControlService
from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.runner import ETLRunner

ROOT = Path(__file__).parents[1]
COLUMNS = ["acct_id", "bill_cyc_cd", "setup_dt", "currency_cd", "acct_mgmt_grp", "bill_after_dt", "protect_cyc_sw", "cis_division", "mailing_prem_id", "protect_prem_sw", "coll_cl_cd", "cr_review_dt", "postpone_cr_rvw_dt", "int_cr_review_sw", "cust_cl_cd", "bill_prt_intercept", "no_dep_rvw_sw", "version"]


def test_legacy_manifest_migrates_and_watermark(tmp_path):
    db = tmp_path / "control.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE file_manifests (path TEXT PRIMARY KEY, size INTEGER, modified REAL, processed_at TEXT, run_id TEXT)")
    control = ControlService(db)
    control.set_watermark("ci_acct", "v1")
    assert control.watermark("ci_acct") == "v1"
    assert control.manifest_eligible("x", 1, 1.0, "abc")


def test_bom_leading_zero_and_text_version(tmp_path):
    landing = tmp_path / "landing"; landing.mkdir()
    row = {c: "" for c in COLUMNS}; row.update(acct_id="00001", setup_dt="1/2/2024", version="001", bill_cyc_cd="M")
    with (landing / "input.csv").open("w", newline="", encoding="utf-8") as handle:
        handle.write("\ufeff" + ",".join(COLUMNS) + "\n")
        csv.DictWriter(handle, fieldnames=COLUMNS).writerows([row])
    context = RuntimeContext("run", landing_dir=landing, raw_db=tmp_path/"raw.sqlite", persistent_db=tmp_path/"persistent.sqlite", consumption_db=tmp_path/"consumption.sqlite", control_db=tmp_path/"control.sqlite")
    ETLRunner(load_metadata(ROOT / "metadata/ci_acct.yml"), context).run()
    with sqlite3.connect(context.persistent_db) as conn:
        assert conn.execute("select acct_id, version from per_cust_ci_acct").fetchone() == ("00001", "001")


def test_manifest_not_recorded_before_success(tmp_path):
    landing = tmp_path / "landing"; landing.mkdir()
    (landing / "bad.csv").write_text("wrong,header\n1,2\n", encoding="utf-8")
    context = RuntimeContext("run", landing_dir=landing, raw_db=tmp_path/"raw.sqlite", persistent_db=tmp_path/"persistent.sqlite", consumption_db=tmp_path/"consumption.sqlite", control_db=tmp_path/"control.sqlite")
    try:
        ETLRunner(load_metadata(ROOT / "metadata/ci_acct.yml"), context).run()
    except ValueError:
        pass
    with sqlite3.connect(context.control_db) as conn:
        assert conn.execute("select count(*) from file_manifests").fetchone()[0] == 0
