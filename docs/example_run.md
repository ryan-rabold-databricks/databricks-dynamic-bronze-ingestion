# Example run (verified behavior)

Two sources, two formats, two domains: a CSV `customers` source (domain `crm`) and a Parquet
`orders` source (domain `erp`). Account-specific values below are redacted to `<ACCT>`.

## Run 1 — both sources land

Files land in both buckets, firing S3 -> SNS -> SQS events. The orchestrator drains the queue and
routes. Abbreviated orchestrator stdout:

```
[init ] loaded 2 active route(s): [
          ('banner-landing-crm-<ACCT>', 'customers/customer_master/', 'pg_crm'),
          ('banner-landing-erp-<ACCT>', 'orders/sales_orders/',       'pg_erp')]
[route] s3://banner-landing-crm-<ACCT>/customers/customer_master/2026-06-24/customers_2026-06-24.csv -> pg_crm
[route] s3://banner-landing-erp-<ACCT>/orders/sales_orders/2026-06-24/part-00000-....snappy.parquet  -> pg_erp
[route] s3://banner-landing-erp-<ACCT>/orders/sales_orders/2026-06-24/part-00001-....snappy.parquet  -> pg_erp
   ...
[skip ] no route for s3://banner-landing-crm-<ACCT>/validate_credential_...   <- probe file, correctly ignored
[drain] processed 33 message(s); groups with work: ['pg_crm', 'pg_erp']
[fire ] group=pg_crm pipeline=<id> update=<update_id>
[fire ] group=pg_erp pipeline=<id> update=<update_id>
[done ] triggered groups: ['pg_crm', 'pg_erp']
```

Note: **33 events deduped to 2 pipeline triggers.** Keys were used only to decide which groups had
work; the pipelines were never told which files to read.

Result:

| Table | Rows |
|-------|------|
| `banner_bronze.crm.customers_raw` | 100 |
| `banner_bronze.erp.orders_raw` | 100 |

Each row carries lineage columns: `_source_id`, `_pipeline_group`, `_source_file`, `_ingested_at`.

## Run 2 — CRM-only second batch (checkpoint retention + routing precision)

A second CSV batch lands in the CRM bucket only.

- Only `pg_crm` is triggered; `pg_erp` is **not** (routing precision).
- `customers_raw` grows **100 -> 150** incrementally — Auto Loader's checkpoint persisted, so only
  the new file was read; batch 1 was not reprocessed.
- `orders_raw` stays at **100**.

This demonstrates the two properties that matter most for intermittent, multi-source landing zones:
checkpoints survive across runs, and only the pipelines with new data are woken.
