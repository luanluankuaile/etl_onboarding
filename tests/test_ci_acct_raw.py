from etl_framework.ci_acct_raw import deduplicate


def test_deduplicate_keeps_latest_file_metadata():
    rows = [
        {"acct_id": "A", "version": "1", "source_file_modified_ts": "2024-01-01", "ingestion_ts": "2024-01-02", "source_row_number": 1},
        {"acct_id": "A", "version": "1", "source_file_modified_ts": "2024-01-03", "ingestion_ts": "2024-01-04", "source_row_number": 1},
    ]
    result = deduplicate(rows)
    assert len(result) == 1
    assert result[0]["source_file_modified_ts"] == "2024-01-03"
    assert result[0]["dq_status"] == "VALID"


def test_invalid_keys_are_rejected():
    result = deduplicate([{"acct_id": None, "version": "-1"}])
    assert result[0]["dq_status"] == "REJECTED"
