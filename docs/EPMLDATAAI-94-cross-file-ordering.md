# EPMLDATAAI-94: Cross-file ordering fix

## Implementation
- Propagates Landing `_ingestion_timestamp` into Raw and quarantine tables.
- Uses ingestion timestamp, then SQLite rowid, for deterministic first-file-wins selection independent of filename.
- Adds regression coverage for `file_z.csv` ingested before `file_a.csv`.

## Validation and governance
Run the full pytest suite and review the Raw schema and persistent output before merge. Review Agent approval and required human data-owner/release approvals remain mandatory. No credentials are hardcoded.
