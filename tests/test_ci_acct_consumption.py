from etl_framework.ci_acct_consumption import create_view_sql


def test_consumption_view_targets_persistent_table():
    assert "per_cust_ci_acct" in create_view_sql()
    assert "csp_dim_account" in create_view_sql()
