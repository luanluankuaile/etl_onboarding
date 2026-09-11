import sqlite3
from pathlib import Path
from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.runner import ETLRunner


def test_ci_acct_end_to_end(tmp_path: Path):
    landing = tmp_path / "landing"; landing.mkdir()
    fixture = Path(__file__).parent / "fixtures/ci_acct_sample.csv"
    (landing / "ci_acct.csv").write_bytes(fixture.read_bytes())
    root = tmp_path
    context = RuntimeContext("ci-run", landing_dir=landing, raw_db=root/"raw.sqlite", persistent_db=root/"persistent.sqlite", consumption_db=root/"consumption.sqlite", control_db=root/"control.sqlite")
    ETLRunner(load_metadata(Path(__file__).parents[1] / "metadata/ci_acct.yml"), context).run()
    db = sqlite3.connect(context.persistent_db)
    assert db.execute("select count(*) from per_cust_ci_acct").fetchone()[0] == 4
    assert db.execute("select no_dep_rvw_sw from per_cust_ci_acct where acct_id='00003'").fetchone()[0] == "Y"
    assert db.execute("select count(*) from raw_cust_ci_acct where row_status='duplicate'").fetchone()[0] == 1
    db.close()


def test_ci_acct_date_and_blank_handling(tmp_path: Path):
    landing = tmp_path / "landing"; landing.mkdir()
    (landing / "one.csv").write_text("acct_id,setup_dt,version\n00001,1/2/2024,1\n", encoding="utf-8")
    root = tmp_path
    context = RuntimeContext("date-run", landing_dir=landing, raw_db=root/"raw.sqlite", persistent_db=root/"persistent.sqlite", consumption_db=root/"consumption.sqlite", control_db=root/"control.sqlite")
    ETLRunner(load_metadata(Path(__file__).parents[1] / "metadata/ci_acct.yml"), context).run()
    db = sqlite3.connect(context.persistent_db)
    assert db.execute("select setup_dt from per_cust_ci_acct").fetchone()[0] == "2024-01-02"
