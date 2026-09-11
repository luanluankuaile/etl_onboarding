from etl_framework.ci_acct_persistent import merge_scd1


def test_scd1_inserts_and_updates_only_newer_versions():
    target = {"A": {"acct_id": "A", "version": 2, "name": "old"}}
    inserted, updated = merge_scd1(target, [
        {"acct_id": "B", "version": 1, "dq_status": "VALID"},
        {"acct_id": "A", "version": 1, "name": "stale", "dq_status": "VALID"},
        {"acct_id": "A", "version": 3, "name": "new", "dq_status": "VALID"},
    ])
    assert (inserted, updated) == (1, 1)
    assert target["A"]["name"] == "new"
    assert target["B"]["version"] == 1
