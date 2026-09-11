from etl_framework.ci_acct_raw import deduplicate


def test_duplicate_composite_key_is_audited():
    audit = []
    result = deduplicate([{"acct_id": "A", "version": "1", "source_row_number": 1}, {"acct_id": "A", "version": "1", "source_row_number": 2}], audit=audit)
    assert len(result) == 1
    assert audit[0]["rule_id"] == "CI_ACCT_DQ_003"


def test_invalid_keys_are_rejected():
    assert deduplicate([{"acct_id": None, "version": "-1"}])[0]["dq_status"] == "REJECTED"
