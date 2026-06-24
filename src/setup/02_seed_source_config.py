"""
Seed the two demo sources into banner_bronze.control.source_config.

Idempotent MERGE keyed on source_id, so re-running updates in place. Bucket names embed the AWS
account id, so they are passed in rather than hardcoded.

Run as a Databricks job task / notebook, or:
  databricks bundle run seed_source_config -- --account-id 000000000000
"""

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession

# Stable, deterministic source -> pipeline_group assignment (sibling module).
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "control"))
from assign_pipeline_group import assign_pipeline_group  # noqa: E402


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--config-table", default="banner_bronze.control.source_config")
    p.add_argument("--account-id", required=True, help="AWS account id used in bucket names")
    p.add_argument("--crm-bucket", default=None)
    p.add_argument("--erp-bucket", default=None)
    p.add_argument(
        "--domain-shards",
        default='{"crm": 1, "erp": 1}',
        help="JSON {domain: shards_per_domain}; controls stable-hash group assignment",
    )
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    crm_bucket = args.crm_bucket or f"banner-landing-crm-{args.account_id}"
    erp_bucket = args.erp_bucket or f"banner-landing-erp-{args.account_id}"
    domain_shards = json.loads(args.domain_shards)

    spark = SparkSession.builder.getOrCreate()

    # Source 1: CRM customers, CSV. Source 2: ERP orders, Parquet.
    # landing layout: s3://<bucket>/{source}/{table}/{batch_date}/{files}
    rows = [
        {
            "source_id": "customers",
            "domain": "crm",
            "landing_bucket": crm_bucket,
            "landing_prefix": "customers/customer_master/",
            "landing_path": f"s3://{crm_bucket}/customers/customer_master/",
            "path_regex": None,
            "file_format": "csv",
            "reader_options": {"header": "true"},
            "schema_hints": "customer_id STRING, full_name STRING, email STRING, signup_date DATE, lifetime_value DOUBLE",
            "schema_evolution_mode": "addNewColumns",
            "target_catalog": "banner_bronze",
            "target_schema": "crm",
            "target_table": "customers_raw",
            "pipeline_group": assign_pipeline_group("customers", "crm", domain_shards.get("crm", 1)),
            "cadence": "weekly",
            "t_shirt_size": "S",
            "is_active": True,
        },
        {
            "source_id": "orders",
            "domain": "erp",
            "landing_bucket": erp_bucket,
            "landing_prefix": "orders/sales_orders/",
            "landing_path": f"s3://{erp_bucket}/orders/sales_orders/",
            "path_regex": None,
            "file_format": "parquet",
            "reader_options": {},
            "schema_hints": None,
            "schema_evolution_mode": "addNewColumns",
            "target_catalog": "banner_bronze",
            "target_schema": "erp",
            "target_table": "orders_raw",
            "pipeline_group": assign_pipeline_group("orders", "erp", domain_shards.get("erp", 1)),
            "cadence": "monthly",
            "t_shirt_size": "M",
            "is_active": True,
        },
    ]

    df = spark.createDataFrame(rows).selectExpr(
        "source_id", "domain", "landing_bucket", "landing_prefix", "landing_path",
        "path_regex", "file_format", "reader_options", "schema_hints",
        "schema_evolution_mode", "target_catalog", "target_schema", "target_table",
        "pipeline_group", "cadence", "t_shirt_size", "is_active",
        "current_timestamp() as created_at", "current_timestamp() as updated_at",
    )
    df.createOrReplaceTempView("_seed_source_config")

    spark.sql(f"""
        MERGE INTO {args.config_table} AS t
        USING _seed_source_config AS s
        ON t.source_id = s.source_id
        WHEN MATCHED THEN UPDATE SET
            domain = s.domain, landing_bucket = s.landing_bucket,
            landing_prefix = s.landing_prefix, landing_path = s.landing_path,
            path_regex = s.path_regex, file_format = s.file_format,
            reader_options = s.reader_options, schema_hints = s.schema_hints,
            schema_evolution_mode = s.schema_evolution_mode,
            target_catalog = s.target_catalog, target_schema = s.target_schema,
            target_table = s.target_table, pipeline_group = s.pipeline_group,
            cadence = s.cadence, t_shirt_size = s.t_shirt_size,
            is_active = s.is_active, updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Seeded {args.config_table}:")
    spark.table(args.config_table).select(
        "source_id", "domain", "landing_bucket", "landing_prefix",
        "file_format", "pipeline_group", "is_active"
    ).show(truncate=False)


if __name__ == "__main__":
    main()
