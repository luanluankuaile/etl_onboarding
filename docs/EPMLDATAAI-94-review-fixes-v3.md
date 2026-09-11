# EPMLDATAAI-94: Equal-version and atomicity review corrections

## Implementation
- Persistent merge now skips incoming records whose version is less than or equal to the stored version.
- Same-version records across files/batches are replay-safe; the first persisted value is preserved.
- Duplicate rows in one batch are resolved deterministically by the last row (highest rowid), as required by the existing duplicate pattern.
- Empty account IDs and missing, non-numeric, or negative versions are quarantined as invalid.

## Atomicity strategy
The implementation uses documented replay-safe semantics rather than a cross-database transaction. The manifest is recorded after the persistent commit, so a crash in that narrow interval may cause a file replay. The `<=` comparison makes replay idempotent and prevents equal-version payload replacement. A future explicit transaction would require a shared transaction boundary across persistent and control stores and is outside this correction.

## Validation
Regression coverage includes equal-version conflicts across runs, duplicate equal-version rows within one file, null account ID, and null version quarantine, in addition to existing version and replay tests.

## Approval gate
Do not merge or promote until Review Agent approval and required human data-owner/release approvals are obtained.
