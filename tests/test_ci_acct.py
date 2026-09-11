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


def test_ci_acct_metadata_and_manifest_contract(tmp_path: Path):
    metadata = load_metadata(Path(__file__).parents[1] / "metadata/ci_acct.yml")
    mapping = metadata.persistent[0]
    assert metadata.landing["table"] == "land_cust_ci_acct"
    assert metadata.raw["table"] == "raw_cust_ci_acct"
    assert mapping.target_table == "per_cust_ci_acct"
    assert mapping.keys == ["acct_id"]
    assert mapping.watermark_column is None
    assert len(mapping.columns) == 18
    assert mapping.columns[0].nullable is False
    assert [c.data_type for c in mapping.columns if c.data_type == "DATE"] == ["DATE"] * 4

    landing = tmp_path / "landing"; landing.mkdir()
    fixture = Path(__file__).parent / "fixtures/ci_acct_sample.csv"
    (landing / "ci_acct.csv").write_bytes(fixture.read_bytes())
    root = tmp_path
    context = RuntimeContext("manifest-run-1", landing_dir=landing, raw_db=root/"raw.sqlite", persistent_db=root/"persistent.sqlite", consumption_db=root/"consumption.sqlite", control_db=root/"control.sqlite")
    ETLRunner(metadata, context).run()
    ETLRunner(metadata, RuntimeContext("manifest-run-2", landing_dir=landing, raw_db=root/"raw.sqlite", persistent_db=root/"persistent.sqlite", consumption_db=root/"consumption.sqlite", control_db=root/"control.sqlite")).run()
    db = sqlite3.connect(context.raw_db)
    assert db.execute("select count(*) from land_cust_ci_acct").fetchone()[0] == 5
    db.close()
