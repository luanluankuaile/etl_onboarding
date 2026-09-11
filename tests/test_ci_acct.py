import csv
import sqlite3
from pathlib import Path
import pytest
from etl_framework.context import RuntimeContext
from etl_framework.metadata import load_metadata
from etl_framework.runner import ETLRunner

ROOT = Path(__file__).parents[1]
META = load_metadata(ROOT / "metadata/ci_acct.yml")
FIXTURES = ROOT / "tests/fixtures"


def run_file(tmp_path, filename, run_id="run-1"):
    landing = tmp_path / "landing"
    landing.mkdir(exist_ok=True)
    (landing / filename).write_bytes((FIXTURES / filename).read_bytes())
    context = RuntimeContext(run_id, environment="test", landing_dir=landing,
        raw_db=tmp_path / "raw.sqlite", persistent_db=tmp_path / "persistent.sqlite",
        consumption_db=tmp_path / "consumption.sqlite", control_db=tmp_path / "control.sqlite")
    ETLRunner(META, context).run()
    return context


def db(path):
    return sqlite3.connect(path)


def test_metadata_loading():
    mapping = META.persistent[0]
    assert len(mapping.columns) == 18
    assert mapping.keys == ["acct_id"]
    assert mapping.deduplicate_by == ["acct_id", "version"]
    assert mapping.watermark_column == "version"
    assert next(c for c in mapping.columns if c.name == "acct_id").nullable is False


def test_landing_ingestion(tmp_path):
    context = run_file(tmp_path, "ci_acct_sample.csv")
    with db(context.raw_db) as conn:
        assert len(conn.execute("pragma table_info(ci_acct)").fetchall()) == 18
        assert conn.execute("select count(*) from ci_acct").fetchone()[0] == 3


def test_raw_cast_and_deduplication(tmp_path):
    context = run_file(tmp_path, "ci_acct_sample.csv")
    with db(context.persistent_db) as conn:
        assert conn.execute("select setup_dt from ci_acct where acct_id='123'").fetchone()[0] == "2024-01-15"
        assert conn.execute("select count(*) from ci_acct").fetchone()[0] == 3


def test_null_acct_id_is_quarantined(tmp_path):
    context = run_file(tmp_path, "ci_acct_invalid.csv")
    with db(context.persistent_db) as conn:
        assert conn.execute("select count(*) from ci_acct").fetchone()[0] == 2
        assert conn.execute("select count(*) from ci_acct__quarantine").fetchone()[0] == 1
        assert conn.execute("select acct_id from ci_acct__quarantine").fetchone()[0] is None


def test_incremental_watermark_and_latest_version(tmp_path):
    context = run_file(tmp_path, "ci_acct_sample.csv")
    # Use a new file name so the control manifest treats this as a second delivery.
    landing = context.landing_dir
    (landing / "ci_acct_incremental.csv").write_bytes((FIXTURES / "ci_acct_incremental.csv").read_bytes())
    ETLRunner(META, context.__class__("run-2", "test", landing, context.raw_db,
        context.persistent_db, context.consumption_db, context.control_db)).run()
    with db(context.control_db) as conn, db(context.persistent_db) as persistent:
        assert conn.execute("select value from watermarks where source_table='ci_acct'").fetchone()[0] == "2"
        assert persistent.execute("select bill_cyc_cd from ci_acct where acct_id='123'").fetchone()[0] == "04"
        assert persistent.execute("select count(*) from ci_acct").fetchone()[0] == 4


def test_scd1_upsert_semantics(tmp_path):
    context = run_file(tmp_path, "ci_acct_sample.csv")
    with db(context.persistent_db) as conn:
        assert conn.execute("select count(*) from ci_acct where acct_id='123'").fetchone()[0] == 1
        assert conn.execute("select count(*) from ci_acct where acct_id='123'").fetchone()[0] == 1


def test_consumption_publishing(tmp_path):
    context = run_file(tmp_path, "ci_acct_sample.csv")
    with db(context.consumption_db) as conn:
        assert conn.execute("select count(*) from csp_dim_account").fetchone()[0] == 3
        assert len(conn.execute("pragma table_info(csp_dim_account)").fetchall()) == 22


def test_audit_and_lineage(tmp_path):
    context = run_file(tmp_path, "ci_acct_sample.csv")
    with db(context.persistent_db) as conn:
        row = conn.execute("select run_id, environment, latest_update_datetime, latest_insert_datetime from ci_acct limit 1").fetchone()
        assert row[0] == "run-1" and row[1] == "test" and row[2] and row[3]
    with db(context.control_db) as conn:
        assert conn.execute("select status from runs where run_id='run-1'").fetchone()[0] == "SUCCEEDED"


def test_idempotency(tmp_path):
    context = run_file(tmp_path, "ci_acct_sample.csv")
    before = db(context.persistent_db).execute("select count(*) from ci_acct").fetchone()[0]
    ETLRunner(META, context).run()
    with db(context.persistent_db) as conn:
        assert conn.execute("select count(*) from ci_acct").fetchone()[0] == before
    with db(context.control_db) as conn:
        assert conn.execute("select count(*) from file_manifests").fetchone()[0] == 1


def test_malformed_csv_rejected(tmp_path):
    landing = tmp_path / "landing"; landing.mkdir()
    (landing / "bad.csv").write_text("acct_id,version\n1,1\n")
    context = RuntimeContext("bad", "test", landing, tmp_path/"raw.sqlite", tmp_path/"persistent.sqlite", tmp_path/"consumption.sqlite", tmp_path/"control.sqlite")
    with pytest.raises(ValueError, match="expected columns"):
        ETLRunner(META, context).run()


def test_type_conversion_error_is_quarantined(tmp_path):
    context = run_file(tmp_path, "ci_acct_invalid.csv")
    with db(context.persistent_db) as conn:
        assert conn.execute("select count(*) from ci_acct__quarantine").fetchone()[0] == 1
