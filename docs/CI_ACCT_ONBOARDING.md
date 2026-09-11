# CI_ACCT onboarding

## Scope

This change registers the SharePoint CSV source and the Landing → Raw → Persistent → Consumption path for `CI_ACCT`. It uses the repository's existing YAML metadata and SQLite/control-service conventions; it does not add orchestration or credentials.

## Processing contract

- Landing: discover all unseen `*.csv` files, validate UTF-8-SIG/comma format and the registered header, calculate SHA-256, and attach the nine required technical fields.
- Raw: cast `version` to integer, apply the approved `(acct_id, version)` deduplication ordering, calculate `record_hash`, and reject null/invalid keys.
- Persistent: SCD1 keyed by `acct_id`; insert new accounts and update only when incoming `version` is greater. Deletes are not performed.
- Consumption: expose the Persistent current state as `csp_dim_account`.

## Metadata and approval gates

`metadata/ci_acct_config.yml` contains dataset, connection, 18-column mapping, incremental, SCD, DQ, and lineage registrations. The 18 entries include the two approved key fields and sixteen provisional `source_column_*` placeholders. Before deployment, the data owner must replace those placeholders with the exact source contract, confirm datatypes/nullability/classification, and approve the version ordering semantics.

The SharePoint connection is referenced by `SHAREPOINT_CI_ACCT_CONNECTION`; secrets must be provisioned in the runtime secret manager. File arrival, checksum, schema, encoding, duplicate, row-count, and operational status controls remain mandatory promotion checks.

## Test and rollout

Run `pytest`. Promote metadata first, then deploy code, then execute a controlled sample-file run in a non-production environment. Validate row counts, rejected records, audit/control records, and table/column lineage. Production scheduling at 04:00 and consumption publication require human release approval.

## Rollback

Disable the dataset schedule, retain control/audit records, and revert the feature branch/metadata registration. Do not delete Persistent rows as part of rollback; use the platform's approved restore procedure.