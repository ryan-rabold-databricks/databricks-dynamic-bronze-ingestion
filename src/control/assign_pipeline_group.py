"""
Deterministic source -> pipeline_group assignment.

Why this exists: an SDP streaming table is owned by exactly one pipeline and CANNOT be moved to
another pipeline without a full refresh (loss of checkpoint + full re-ingest). So the group a
source lands in must be decided ONCE, deterministically, at onboarding time -- never reshuffled.

A stable hash (sha256, not Python's salted hash()) guarantees the same source_id always maps to
the same group across processes, languages, and runs. Sharding per domain lets a busy domain span
multiple pipelines while respecting the ~30-50 objects-per-pipeline guideline, without anyone
hand-picking groups (which drifts and causes accidental moves).

Scheme:
  shards_per_domain == 1  ->  "pg_<domain>"               (e.g. pg_crm)
  shards_per_domain  > 1  ->  "pg_<domain>_<NN>"          (e.g. pg_crm_03)

Onboarding a source = compute its group with this function and INSERT the config row. The group's
pipeline must exist (see all_pipeline_groups() to generate the full set for databricks.yml).
"""

import hashlib


def assign_pipeline_group(source_id: str, domain: str, shards_per_domain: int = 1) -> str:
    """Return the stable pipeline_group for a source.

    Args:
        source_id: unique logical source id (the hash key -- keep it immutable).
        domain: business domain (also the landing-bucket grouping).
        shards_per_domain: how many pipelines this domain is spread across (>= 1).
    """
    if shards_per_domain < 1:
        raise ValueError("shards_per_domain must be >= 1")
    if not source_id or not domain:
        raise ValueError("source_id and domain are required")
    if shards_per_domain == 1:
        return f"pg_{domain}"
    digest = hashlib.sha256(source_id.encode("utf-8")).hexdigest()
    shard = int(digest, 16) % shards_per_domain
    return f"pg_{domain}_{shard:02d}"


def all_pipeline_groups(domain_shards: dict) -> list:
    """Enumerate every pipeline_group implied by a {domain: shards} map.

    Use this to keep databricks.yml's pipeline blocks and the orchestrator's --pipeline-map in
    sync with the sharding config. Example:
        all_pipeline_groups({"crm": 1, "erp": 2}) -> ["pg_crm", "pg_erp_00", "pg_erp_01"]
    """
    groups = []
    for domain in sorted(domain_shards):
        shards = domain_shards[domain]
        if shards < 1:
            raise ValueError(f"shards for domain '{domain}' must be >= 1")
        if shards == 1:
            groups.append(f"pg_{domain}")
        else:
            groups.extend(f"pg_{domain}_{i:02d}" for i in range(shards))
    return groups


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Compute the stable pipeline_group for a source")
    ap.add_argument("--source-id", required=True)
    ap.add_argument("--domain", required=True)
    ap.add_argument("--shards", type=int, default=1)
    a = ap.parse_args()
    print(assign_pipeline_group(a.source_id, a.domain, a.shards))
