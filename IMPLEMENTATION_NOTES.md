# Implementation notes

- The Jira design defines 18 source attributes; date attributes are retained as ISO-8601 TEXT because SQLite has no native DATE storage class while metadata identifies them as DATE.
- Persistent uses `acct_id` as the physical primary key, implementing SCD1 replacement. `deduplicate_by: [acct_id, version]` removes duplicate source versions within a delivery.
- `version` is a lexical TEXT watermark. This assumes source versions are consistently comparable strings (the supplied values are numeric strings). Production must confirm ordering semantics and late-arriving-record policy.
- Invalid non-null key records and conversion failures are quarantined. Invalid dates are treated as conversion failures.
- File manifests make reruns idempotent by path. Operational deployment must use unique delivered filenames or an approved manifest retention/reprocessing procedure.
- Consumption SQL publishes from persistent through the framework's attached `source` database.

## Approval checkpoints

1. Data owner confirms all 18 mappings, date format, version ordering, and SCD1 behavior.
2. Data governance/security approves source location, access, retention, and quarantine handling.
3. Operations approves schedule, monitoring thresholds, replay procedure, and production rollout.
4. CI and human review must pass before merging; production deployment remains a controlled approval step.

## Known framework limitation

The framework's generic physical table name is `ci_acct`; environment-specific Landing/Raw/Persistent names from the design document are represented by metadata source/target mappings only and should be reviewed if physical layer prefixes are mandatory.
