# CI_ACCT onboarding

## Scope

This change registers the SharePoint CSV source and the Landing → Raw → Persistent → Consumption path for `CI_ACCT`. It uses the repository's existing YAML metadata and SQLite/control-service conventions; it does not add orchestration or credentials.

## Processing contract

- Landing: discover all unseen `*.csv` files, validate UTF-8-SIG/comma format and the registered header, calculate SHA-256, and attach the nine required technical fields.
- Raw: cast `version` to integer, apply the approved `(acct_id, version)` deduplication ordering, calculate `record_hash`, and reject null/invalid keys.
- Persistent: SCD1 keyed by `acct_id`; insert new accounts and update only when incoming `version` is greater. Deletes are not performed.
- Consumption: expose the Persistent current state as `csp_dim