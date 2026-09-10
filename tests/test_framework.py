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
