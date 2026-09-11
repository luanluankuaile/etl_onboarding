import sqlite3
from pathlib import Path
from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.runner import ETLRunner


def test_end_to_end(tmp_path: Path):
    landing = tmp_path / "landing"; landing.mkdir()
    (landing / "customers.csv").write_text("customer_id,name,email\n1,Ada,a@example.com\n1,Dup,a2@example.com\n,Invalid,\n", encoding="utf-8")
    metadata = load_metadata(Path(__file__).parents[1] / "metadata/demo.yml")
    context = RuntimeContext("run-1", landing_dir=landing, raw_db=landing / "raw.sqlite", persistent_db=landing / "persistent.sqlite", consumption_db=landing / "consumption.sqlite", control_db=landing / "control.sqlite")
    ETLRunner(metadata, context).run()
    db = sqlite3.connect(context.persistent_db)
    assert db.execute("select count(*) from customers").fetchone()[0] == 1
    assert db.execute("select count(*) from customers__quarantine").fetchone()[0] == 1
    db.close()


def test_metadata_loader():
    metadata = load_metadata(Path(__file__).parents[1] / "metadata/demo.yml")
    assert metadata.persistent[0].keys == ["customer_id"]


def test_ci_acct_end_to_end(tmp_path: Path):
    """End-to-end test for CI_ACCT onboarding through Landing/Raw/Persistent."""
    landing = tmp_path / "landing"; landing.mkdir()
    # Create CI_ACCT CSV with sample data
    csv_content = "acct_id,bill_cyc_cd,setup_dt,currency_cd,acct_mgmt_grp,bill_after_dt,protect_cyc_sw,cis_division,mailing_prem_id,protect_prem_sw,coll_cl_cd,cr_review_dt,postpone_cr_rvw_dt,int_cr_review_sw,cust_cl_cd,bill_prt_intercept,no_dep_rvw_sw,version\n"
    csv_content += "A001,CYCA,2024-01-01,USD,GRP1,2024-02-01,Y,DIV1,PREM1,N,CL1,2024-03-01,2024-03-15,Y,CCL1,N,N,1\n"
    csv_content += "A002,CYCB,2024-01-02,USD,GRP2,2024-02-02,N,DIV2,PREM2,Y,CL2,2024-03-02,2024-03-16,N,CCL2,Y,Y,1\n"
    (landing / "CI_ACCT_1.csv").write_text(csv_content, encoding="utf-8")
    
    metadata = load_metadata(Path(__file__).parents[1] / "metadata/ci_acct.yml")
    context = RuntimeContext("run-1", landing_dir=landing, raw_db=landing / "raw.sqlite", persistent_db=landing / "persistent.sqlite", consumption_db=landing / "consumption.sqlite", control_db=landing / "control.sqlite")
    ETLRunner(metadata, context).run()
    
    db = sqlite3.connect(context.persistent_db)
    # Verify 2 accounts inserted
    assert db.execute("select count(*) from per_cust_ci_acct").fetchone()[0] == 2
    # Verify accounts have correct acct_id
    accounts = db.execute("select acct_id, version from per_cust_ci_acct order by acct_id").fetchall()
    assert accounts == [("A001", 1), ("A002", 1)]
    db.close()
