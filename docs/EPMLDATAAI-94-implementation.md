# EPMLDATAAI-94 implementation notes

## Sequencing
1. Obtain human approval for SharePoint connection registration and secret reference.
2. Deploy metadata and job to a non-production environment.
3. Run the five-record integration fixture and validate control, DQ, and lineage outputs.
4. Obtain data-owner and release approval before enabling the 04:00 schedule.
5. Promote through the normal dev/test/release workflow. Production deployment is explicitly out of scope.

## Controls and assumptions
- No credentials are stored in this repository; the runtime resolves `SHAREPOINT_CI_ACCT_CONNECTION`.
- `version` is used only as the provisional duplicate winner ordering; it is not configured or advanced as a source watermark. Confirm version and watermark semantics with the source owner before production use.
- Landing is append-only by checksum/batch and retains source values plus audit metadata.
- Invalid rows are quarantined; valid rows are deduplicated by highest version before the Type 1 merge.
- `csp_dim_account` is not changed by this onboarding.

## Rollback
Disable the schedule, preserve control/audit records, and revert the feature branch or remove only the CI_ACCT configuration. Persistent Type 1 changes are reversible only by restoring a previously approved source snapshot; no production rollback is performed by this change.
