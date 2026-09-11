from etl_framework.ci_acct_persistent import merge_scd1, reduce_to_latest


def test_scd1_reduces_three_versions_and_blocks_stale():
    target = {"A": {"acct_id": "A", "version": 2}}
    rows = [{"acct_id": "A", "version": 1, "dq_status": "VALID"}, {"acct_id": "A", "version": 3, "dq_status": "VALID"}, {"acct_id": "A", "version": 2, "dq_status": "VALID"}]
    assert len(reduce_to_latest(rows)) == 1
    assert merge_scd1(target, rows) == (0, 1)
    assert target["A"]["version"] == 3


def test_equal_version_is_deterministic():
    rows = [{"acct_id": "A", "version": 1, "source_row_number": 1}, {"acct_id": "A", "version": 1, "source_row_number": 2}]
    assert reduce_to_latest(rows)[0]["source_row_number"] == 2
