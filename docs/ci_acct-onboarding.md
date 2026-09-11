# CI_ACCT onboarding notes

## Scope

This is a local metadata fixture and test path for EPMLDATAAI-94. It preserves the existing demo and uses the framework's file-manifest incremental discovery. A file is processed once according to the control database manifest; no `watermark_column` is configured because no source watermark was provided.

The logical lineage is:

`SharePoint source (unresolved runtime integration) -> land_cust_ci_acct -> raw_cust_ci_acct -> per_cust_ci_acct -> csp_dim_account`

## Unresolved decisions requiring human approval

- Source update and delete semantics are not supplied. The fixture does not claim CDC, delete propagation, or tombstone behavior.
- SharePoint runtime integration, authentication, secret provisioning, path access, and operational failure handling are deployment concerns; no production connector code is added.
- Schedule timezone is unspecified. The existing schedule value must not be promoted until its timezone is confirmed.
- File delimiter and date format require source-owner confirmation. The local processor currently assumes CSV parsing and `M/d/yyyy` dates only for the fixture.
- SCD behavior is unresolved. The local persistent processor currently uses the existing version-winner/overwrite convention and must not be treated as an approved production SCD policy.

## Quality and governance
- SCD behavior is unresolved. `version` is preserved as opaque source TEXT and is not a watermark. The local processor uses only a deterministic lexical comparison of version text to choose a duplicate winner; this is an implementation tie-break, not a claim about source version ordering or recency, and must not be treated as an approved production SCD policy.
All 18 columns are declared. `acct_id` is the key and not-null violations are quarantined. Human review is required for metadata, source-to-target mapping, data quality, security classification, lineage, runtime integration, and release approval before promotion.
