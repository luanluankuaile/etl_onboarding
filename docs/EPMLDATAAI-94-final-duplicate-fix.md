# EPMLDATAAI-94: Final same-file duplicate clarification

## Implementation
- Added explicit CI_ACCT within-file deduplication before persistent merge.
- Rows are ordered by source file and descending SQLite `rowid`; the highest rowid is retained for each `(file, acct_id, version)` key.
- Cross-file and cross-run persistence retains the first equal-version record through the `<=` guard.
- CI_ACCT is routed through its source-specific processor while other metadata mappings retain the generic runner path.

## Validation
- Same-file equal-version duplicates expect the last row (`no_dep_rvw_sw=Y`).
- Equal-version records across runs expect the first persisted row (`no_dep_rvw_sw=N`).
- Existing version, replay, quarantine, and test-isolation coverage remains in `tests/test_ci_acct.py`; each test uses pytest `tmp_path`.

## Governance gate
Review Agent approval and required human data-owner/release approvals remain mandatory before merge or promotion to `dev` or downstream environments. No credentials are stored in code; the metadata references the external connection secret.
