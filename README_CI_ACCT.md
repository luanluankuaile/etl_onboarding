# CI_ACCT onboarding

CI_ACCT is configured as a metadata-driven daily incremental SCD1 dimension. CSV files matching `*.csv` are discovered in Landing, loaded to raw SQLite, validated and merged into persistent `ci_acct`, then published as `csp_dim_account`.

## Run

```bash
python -m etl_framework metadata/ci_acct.yml --landing tests/fixtures --environment test
pytest tests/test_ci_acct.py -v
```

The runtime creates raw, persistent, consumption, and control SQLite databases beside the landing directory. The control database records run status, row counts, manifests, and the `version` watermark. Records failing the non-null `acct_id` rule or type conversion are written to `ci_acct__quarantine`.

Human approval is required before production source connectivity, scheduling, and deployment. The sample fixtures are for automated validation only.
