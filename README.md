# Dynamic Bronze Ingestion Framework

A config-driven, **no-deploy** landing → bronze ingestion framework on Databricks, built entirely
from native components: **Auto Loader** for incremental file discovery, **Spark Declarative
Pipelines (SDP)** for the managed ingest, and a small **central-routing orchestrator** that turns
cloud file events into precise pipeline triggers.

Onboard a new source that fits an existing pattern by inserting **one row** into a config table — no
pipeline code change, no redeploy.

> ## ⚠️ Disclaimer
>
> This project is provided **for demonstration and educational purposes only**. It is **not an
> officially supported Databricks product or solution**, carries **no warranty or support** (express
> or implied), and is licensed **as-is** under Apache-2.0. It is not covered by any Databricks Service
> Level Agreement or support agreement. Review, harden, security-test, and adapt it to your own
> standards before any production use. You are solely responsible for any deployment and its costs.

This reference implementation ingests **two sources of two different formats from two domains**:

| Source      | Domain | Format  | Landing bucket                       | Bronze target                     | Pipeline group |
|-------------|--------|---------|--------------------------------------|-----------------------------------|----------------|
| `customers` | `crm`  | CSV     | `s3://<crm-bucket>/...`              | `banner_bronze.crm.customers_raw` | `pg_crm`       |
| `orders`    | `erp`  | Parquet | `s3://<erp-bucket>/...`              | `banner_bronze.erp.orders_raw`    | `pg_erp`       |

Landing layout: `s3://<domain-bucket>/{source}/{table}/{batch_date}/{files}`.

## The core idea (why event-driven ingestion gets simpler, not harder)

```
  S3 (crm bucket) ──┐                                   file-arrival waker (per domain)
                    ├─> SNS ─> SQS ─> Orchestrator <──── wakes orchestrator on new files
  S3 (erp bucket) ──┘   (events)      (Python)
                                          │ reads keys ONLY to route prefix -> pipeline_group,
                                          │ dedupes, then start_update() on affected pipelines
                                          ▼
                       ┌──────────────────────────────────────┐
                       │ SDP pipeline pg_crm  (1 shared .py)   │  metaprograms 1 Auto Loader
                       │ SDP pipeline pg_erp  (same .py)       │  streaming table per config row
                       └──────────────────────────────────────┘
                                          │ Auto Loader rediscovers new files from its OWN
                                          ▼ checkpoint — NOT from the event payload
                       banner_bronze.<domain>.<table>_raw
```

The orchestrator reads S3 object keys **only to decide which pipeline group to wake** — never to tell
a pipeline which file to read. Auto Loader's checkpoint already knows what's new, so intermittent
sources, replays, and at-least-once event delivery are all handled for free. See
[docs/architecture.md](docs/architecture.md) for the full rationale.

## Components

| Path | What it is |
|------|------------|
| `databricks.yml` | Asset Bundle: SDP pipelines + orchestrator + per-domain file-arrival waker jobs |
| `src/pipeline/dynamic_bronze_pipeline.py` | **Single** SDP file; metaprograms one Auto Loader streaming table per active config row for its `pipeline_group` |
| `src/orchestrator/orchestrate.py` | Drains SQS, routes keys → pipeline groups via config, triggers only affected pipelines |
| `src/control/assign_pipeline_group.py` | Stable (sha256) source → pipeline_group assignment with per-domain sharding |
| `src/setup/01_create_uc_assets.sql` | Catalog, schemas, metadata volume, control config table |
| `src/setup/02_seed_source_config.py` | Seeds sources into `source_config` (groups via stable hash) |
| `aws/setup_aws.sh` | Creates landing buckets, SNS, SQS, notifications, and the orchestrator IAM role for a UC service credential |
| `aws/setup_uc_storage.py` | Creates UC storage credential + external locations (the IAM-trust handshake) |
| `aws/teardown_aws.sh` | Tears down the AWS resources |
| `data/generate_and_land.py` | Generates CSV + Parquet sample files and lands them (fires events) |
| `tests/` | Unit tests for the stable-hash assignment |

## Control plane: `source_config`

One row per `source → bronze table`. Onboarding a source that fits an existing pattern = **insert
one row**. Adding a new pipeline group = add one ~6-line pipeline block (and one waker) to
`databricks.yml`. Group assignment is computed by a **stable sha256 hash** so a source never silently
moves pipelines (which would force a full refresh) — see
[docs/architecture.md](docs/architecture.md#stable-pipeline-group-assignment-and-the-rebalancing-trap).

## Triggering

Two options, both included:
- **File-arrival wakers (default).** One lightweight job per landing external location with a
  `file_arrival` trigger; it wakes the orchestrator. Event-driven, no idle cost.
- **Cron schedule.** Swap the waker `trigger` for a job `schedule` if you prefer simple polling.

## Deploy order

Set your values first: edit `databricks.yml` (`var.account_id`, the landing URLs, workspace host in
`targets`). Then:

1. `aws/setup_aws.sh` — provision S3/SNS/SQS + the orchestrator IAM role (note the queue URL and
   the printed `ORCH_ROLE_ARN`). Pass `UC_MASTER_ROLE_ARN` and `UC_EXTERNAL_ID` (see the script header).
2. `aws/setup_uc_storage.py` — create the UC storage credential + external locations.
3. Create the UC **SERVICE credential** for the orchestrator (name must match `var.service_credential`,
   default `banner_bronze_orchestrator`) and grant the orchestrator job's run-as identity `ACCESS` on it.
   The exact `databricks credentials create-credential` + `databricks grants update` commands are
   printed by `setup_aws.sh`. No static AWS keys or secret scope are needed.
4. Create UC assets + config table (`src/setup/01_create_uc_assets.sql`).
5. `databricks bundle deploy` — deploy pipelines, orchestrator, and wakers.
6. Seed config (`src/setup/02_seed_source_config.py` or the `seed_source_config` job).
7. `data/generate_and_land.py` — land sample files. Wakers fire → orchestrator routes → bronze fills.

A verified end-to-end walk-through (with routing logs) is in [docs/example_run.md](docs/example_run.md).

## Tests

```bash
python -m pytest tests/ -q      # or: python tests/test_assign_pipeline_group.py
```

## What this intentionally does NOT do

- **No per-file fan-out** — routes per pipeline *group*, not per file.
- **No automatic cross-pipeline rebalancing** — SDP streaming tables are owned by one pipeline;
  moving one is a full refresh. Balance up front via `shards_per_domain` + stable hashing.
- **No Excel in the generic path** — Auto Loader reads json/csv/parquet/avro/orc/text/binaryFile;
  normalize Excel upstream and set `file_format` in config.
