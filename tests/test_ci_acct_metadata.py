from pathlib import Path
from etl_framework.metadata import load_metadata


def test_ci_acct_registers_eighteen_columns_and_layers():
    metadata = load_metadata(Path(__file__).parents[1] / "metadata/ci_acct_config.yml")
    assert len(metadata.persistent[0].columns) == 18
    assert metadata.persistent[0].keys == ["acct_id"]
    assert metadata.landing["table"] == "land_cust_ci_acct"
    assert metadata.consumption[0]["name"] == "csp_dim_account"
