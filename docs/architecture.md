# Architecture & design decisions

This is a config-driven, **no-deploy** landing -> bronze ingestion framework built from Databricks
native components: **Auto Loader** for file discovery, **Spark Declarative Pipelines (SDP)** for the
managed ingest, and a small **central-routing orchestrator** that turns cloud file events into
precise pipeline triggers.

## The key insight: decouple "wake up" from "what to ingest"

A common stumbling block when building event-driven ingestion on Databricks is trying to read the
*specific* changed object key out of a job trigger so you can tell a pipeline exactly what to load.

You don't need to. **Auto Loader already tracks which files are new**, in its own checkpoint state.
The trigger's only job is to *wake the pipeline*; Auto Loader then rediscovers everything new since
its last checkpoint and ingests exactly that.

So object keys are used for exactly one thing in this design: **routing** — deciding *which pipeline*
to wake — never *which file* to read. That makes intermittent sources, replays, and at-least-once
event delivery all safe by construction.

```
  S3 landing (bucket per domain)
        │  ObjectCreated events
        ▼
  SNS topic ──> SQS queue
        │
        ▼
  Orchestrator (Python)            reads keys ONLY to map prefix -> pipeline_group (routing),
   - drain queue                   dedupes, then start_update() on just the affected pipelines
   - route by (bucket, prefix)
   - trigger affected pipelines
        │
        ├──> SDP pipeline  pg_<domainA>   ┐  one shared Python file;
        └──> SDP pipeline  pg_<domainB>   ┘  metaprograms 1 Auto Loader streaming
                                             table per active config row
        ▼
  bronze.<domain>.<table>_raw   (Auto Loader checkpoint rediscovers new files)
```

## Components

| Component | Responsibility |
|-----------|----------------|
| `source_config` control table | One row per source -> bronze table. Onboard a source = INSERT a row. |
| `dynamic_bronze_pipeline.py` | One file, deployed to every pipeline. Reads config for its `pipeline_group`, builds one streaming table per active source. |
| `orchestrate.py` | Drains SQS, routes keys to pipeline groups, triggers only affected pipelines. |
| `assign_pipeline_group.py` | Stable (sha256) source -> group assignment with per-domain sharding. |
| File-arrival waker jobs | One per landing external location; fire on new files and wake the orchestrator. |

## Triggering: file-arrival wakers vs. cron

The orchestrator can be woken two ways:

1. **File-arrival wakers (default here).** One lightweight job per landing external location with a
   `file_arrival` trigger; it `run_job_task`s the orchestrator. Event-driven, no idle cost. Because
   a job's file-arrival trigger watches exactly one URL and you have a bucket per domain, you get one
   waker per domain.
2. **Cron schedule.** Simpler, but adds latency and runs even when idle. Good for a quick start.

A third option at high scale is a continuous long-polling SQS dispatcher.

## Stable pipeline-group assignment (and the rebalancing trap)

An SDP streaming table is **owned by exactly one pipeline and cannot be moved** to another without a
full refresh (loss of checkpoint + full re-ingest). So a source's `pipeline_group` must be decided
**once, deterministically**, and never reshuffled.

`assign_pipeline_group(source_id, domain, shards_per_domain)` uses a **sha256** hash (not Python's
salted `hash()`), so the same source always maps to the same group across runs, processes, and
languages:

- `shards_per_domain == 1` -> `pg_<domain>` (e.g. `pg_crm`)
- `shards_per_domain  > 1` -> `pg_<domain>_<NN>` (e.g. `pg_crm_03`)

Sharding lets a busy domain span multiple pipelines while respecting the ~30-50 objects-per-pipeline
guideline — without anyone hand-picking groups (which drifts and causes accidental, expensive moves).

**Do not "rebalance" by moving tables between pipelines automatically.** Balance up front via
`shards_per_domain` and stable hashing. A genuine rebalance is a deliberate, scheduled, full-refresh
migration — not an automatic behavior.

## Deliberate non-goals

- **No per-file fan-out.** Routing is per pipeline group, not per file.
- **No Excel in the generic path.** Auto Loader reads json/csv/parquet/avro/orc/text/binaryFile — not
  Excel. Normalize upstream and set `file_format` in config.
- **Glob, not full regex, for path scoping.** Auto Loader path filtering is glob-based; use the
  optional `path_regex` column for a true-regex filter on `_metadata.file_path` when a source needs it.
