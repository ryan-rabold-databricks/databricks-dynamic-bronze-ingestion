"""Unit tests for the stable pipeline_group assignment.

Run: python -m pytest tests/ -q   (or: python tests/test_assign_pipeline_group.py)
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "control"))

from assign_pipeline_group import all_pipeline_groups, assign_pipeline_group  # noqa: E402


def test_single_shard_uses_plain_domain_name():
    assert assign_pipeline_group("customers", "crm", 1) == "pg_crm"
    assert assign_pipeline_group("orders", "erp", 1) == "pg_erp"


def test_assignment_is_deterministic():
    # Same inputs -> same group, always (sha256, not salted hash()).
    a = assign_pipeline_group("orders", "erp", 8)
    b = assign_pipeline_group("orders", "erp", 8)
    assert a == b


def test_known_shard_values_are_pinned():
    # Regression pins against sha256(source_id) % shards. If these change, group ownership
    # would silently move tables across pipelines -> a forbidden full-refresh event.
    assert assign_pipeline_group("customers", "crm", 4) == "pg_crm_00"
    assert assign_pipeline_group("orders", "erp", 4) == "pg_erp_01"
    assert assign_pipeline_group("claims", "ins", 8) == "pg_ins_04"
    assert assign_pipeline_group("payments", "fin", 8) == "pg_fin_04"


def test_shard_is_within_range():
    for shards in (2, 4, 16, 64):
        for sid in ("a", "b", "longer_source_name", "x" * 50):
            g = assign_pipeline_group(sid, "d", shards)
            shard = int(g.rsplit("_", 1)[1])
            assert 0 <= shard < shards


def test_all_pipeline_groups_enumeration():
    assert all_pipeline_groups({"crm": 1, "erp": 2}) == ["pg_crm", "pg_erp_00", "pg_erp_01"]
    assert all_pipeline_groups({"crm": 1, "erp": 1}) == ["pg_crm", "pg_erp"]


def test_invalid_inputs_raise():
    for bad in (0, -1):
        try:
            assign_pipeline_group("s", "d", bad)
            assert False, "expected ValueError"
        except ValueError:
            pass
    try:
        assign_pipeline_group("", "d", 1)
        assert False, "expected ValueError"
    except ValueError:
        pass


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as e:
                failures += 1
                print(f"FAIL {name}: {e}")
    sys.exit(1 if failures else 0)
