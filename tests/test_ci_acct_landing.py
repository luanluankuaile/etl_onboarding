import pytest
from etl_framework.ci_acct_landing import validate_file


def test_landing_rejects_header_mismatch(tmp_path):
    path = tmp_path / "acct.csv"
    path.write_text("acct_id,version\nA,1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="header mismatch"):
        validate_file(path, ["acct_id", "version", "source_column_03"])
