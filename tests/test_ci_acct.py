from pathlib import Path

import pytest

from etl_framework.ci_acct_pipeline import process_ci_acct, raw_select


def write_csv(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_latest_version_and_scd1_protection(tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "acct_id,version,name\n A1 ,1,old\nA1,3,new\nA1,2,stale\nA2,1,second\n")
    target = {}
    watermarks = []
    assert process_ci_acct([source], target, None, watermarks.append) == (4, 2, 0)
    assert target["A1"]["version"] == 3
    source2 = tmp_path / "source2.csv"
    write_csv(source2, "acct_id,version,name\nA1,2,older\nA1,4,newest\n")
    process_ci_acct([source2], target, 3, watermarks.append)
    assert target["A1"]["version"] == 4
    assert watermarks == [3, 4]


def test_acct_id_null_is_rejected(tmp_path):
    source = tmp_path / "source.csv"
    write_csv(source, "acct_id,version\n,1\nA1,2\n")
    target = {}
    process_ci_acct([source], target, None, lambda value: None)
    assert list(target) == ["A1"]


def test_schema_validation(tmp_path):
    source = tmp_path / "bad.csv"
    write_csv(source, "acct_id,name\nA1,x\n")
    with pytest.raises(ValueError, match="missing required columns"):
        process_ci_acct([source], {}, None, lambda value: None)


def test_raw_cast_and_watermark():
    assert raw_select([{"acct_id": " A1 ", "version": "2", "dq_status": "VALID"}], 1)[0]["version"] == 2
