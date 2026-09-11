import pytest
from etl_framework.ci_acct_landing import validate_file


def test_landing_accepts_csv_edge_cases(tmp_path):
    path = tmp_path / "acct.csv"
    path.write_text("acct_id,version,source_column_03\nA,1,\"quoted, value\"\nB,2,\"line1\nline2\"\n", encoding="utf-8")
    assert len(validate_file(path, ["acct_id", "version", "source_column_03"])) == 2


def test_landing_rejects_header_and_malformed_rows(tmp_path):
    path = tmp_path / "acct.csv"
    path.write_text("acct_id,version\nA,1,extra\n", encoding="utf-8")
    with pytest.raises(ValueError):
        validate_file(path, ["acct_id", "version", "source_column_03"])
