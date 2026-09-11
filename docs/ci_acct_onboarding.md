# CI_ACCT onboarding

This configuration implements the approved `cust` flow from SharePoint CSV to
`land_cust_ci_acct`, `raw_cust_ci_acct`, and `per_cust_ci_acct`.

## Runtime sequence

1. Discover CSVs through the approved SharePoint connector and calculate SHA-256 checksums.
2. Preserve source rows and ingestion audit fields; reject files missing `acct_id` or `version`.
3. Apply `acct_id NOT NULL` quarantine, trim values, cast `version` to integer, and select the highest version per account above the control watermark.
4. Merge current state using SCD1 semantics. Equal or lower versions cannot overwrite a persistent row.
5. Advance the watermark only after the persistent merge succeeds. A failed stage leaves the prior watermark unchanged.

The orchestration file is a deployment contract; connector credentials, production
storage, scheduling, and approval integration remain deployment-owned and must be
configured through approved platform controls. Production promotion requires data
owner/governance approval.
