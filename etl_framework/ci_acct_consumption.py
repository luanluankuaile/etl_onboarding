"""Consumption publication definition for the current CI_ACCT state."""

CONSUMPTION_OBJECT = "csp_dim_account"


def create_view_sql(source_table: str = "per_cust_ci_acct", view_name: str = CONSUMPTION_OBJECT) -> str:
    return f'CREATE VIEW IF NOT EXISTS "{view_name}" AS SELECT * FROM "{source_table}";'
