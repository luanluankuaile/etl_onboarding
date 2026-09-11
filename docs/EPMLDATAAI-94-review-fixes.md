# EPMLDATAAI-94 review corrections

## Implementation
- `version` is mandatory and validated as a non-negative integer before a row enters the valid/raw path.
- Invalid version rows are written to `raw_cust_ci_acct__quarantine` with `row_status=invalid`.
- Persistent merges compare incoming and stored versions and skip older replays.
- New records use INSERT; accepted updates use UPDATE, preserving `run_id` and `latest_insert_datetime` while refreshing `latest_update_datetime` and `_record_hash`.
- File manifests remain the human/operator control for same-file retry prevention.

## Validation
The CI_ACCT tests cover same-file idempotency, older-version rejection, newer-version update, and null/non-numeric/negative version quarantine.

## Approval gate
Do not merge or promote until the Review Agent and required data-owner/release approvers approve the corrected code and test evidence.
