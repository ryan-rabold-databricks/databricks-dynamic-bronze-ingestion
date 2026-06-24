"""
Generate sample CSV (CRM customers) + Parquet (ERP orders) files and upload them to the landing
buckets, following s3://<bucket>/{source}/{table}/{batch_date}/{files}. Uploading fires S3
ObjectCreated events -> SNS -> SQS, which the orchestrator then routes.

Generates locally to a temp dir, then uploads via boto3. Run multiple times with different
--batch-date values to prove checkpoint retention (each run = a new batch the pipeline ingests
incrementally).

Usage:
  python data/generate_and_land.py \
    --aws-profile aws-sandbox-field-eng_databricks-sandbox-admin \
    --account-id 000000000000 --region us-east-2 \
    --batch-date 2026-06-24 --rows 100 --sources both
"""

import argparse
import csv
import io
import random
import uuid
from datetime import date, timedelta

import boto3


def gen_customers_csv(rows, seed):
    rnd = random.Random(seed)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["customer_id", "full_name", "email", "signup_date", "lifetime_value"])
    first = ["Ava", "Liam", "Noah", "Mia", "Zoe", "Eli", "Ivy", "Leo", "Aria", "Kai"]
    last = ["Patel", "Nguyen", "Garcia", "Smith", "Khan", "Cohen", "Brown", "Lee", "Diaz", "Ito"]
    base = date(2022, 1, 1)
    for _ in range(rows):
        cid = str(uuid.UUID(int=rnd.getrandbits(128)))
        name = f"{rnd.choice(first)} {rnd.choice(last)}"
        email = name.lower().replace(" ", ".") + "@example.com"
        signup = (base + timedelta(days=rnd.randint(0, 1200))).isoformat()
        ltv = round(rnd.uniform(0, 9000), 2)
        writer.writerow([cid, name, email, signup, ltv])
    return buf.getvalue().encode("utf-8")


def gen_orders_parquet(rows, seed):
    # pyarrow only (no pandas dependency).
    import pyarrow as pa
    import pyarrow.parquet as pq

    rnd = random.Random(seed)
    order_ids = [str(uuid.UUID(int=rnd.getrandbits(128))) for _ in range(rows)]
    customer_ids = [str(uuid.UUID(int=rnd.getrandbits(128))) for _ in range(rows)]
    skus = [f"SKU-{rnd.randint(1000, 9999)}" for _ in range(rows)]
    qtys = [rnd.randint(1, 12) for _ in range(rows)]
    amounts = [round(rnd.uniform(5, 800), 2) for _ in range(rows)]
    base = date(2025, 1, 1)
    order_ts = [
        (base + timedelta(days=rnd.randint(0, 500))).isoformat() for _ in range(rows)
    ]
    table = pa.table(
        {
            "order_id": order_ids,
            "customer_id": customer_ids,
            "sku": skus,
            "quantity": qtys,
            "amount": amounts,
            "order_date": order_ts,
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--aws-profile", required=True)
    ap.add_argument("--account-id", required=True)
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--batch-date", default=date.today().isoformat())
    ap.add_argument("--rows", type=int, default=100)
    ap.add_argument("--sources", choices=["crm", "erp", "both"], default="both")
    args = ap.parse_args()

    s3 = boto3.Session(profile_name=args.aws_profile, region_name=args.region).client("s3")
    seed = abs(hash(args.batch_date)) % (2**31)

    if args.sources in ("crm", "both"):
        bucket = f"banner-landing-crm-{args.account_id}"
        key = f"customers/customer_master/{args.batch_date}/customers_{args.batch_date}.csv"
        s3.put_object(Bucket=bucket, Key=key, Body=gen_customers_csv(args.rows, seed))
        print(f"uploaded s3://{bucket}/{key} ({args.rows} rows, CSV)")

    if args.sources in ("erp", "both"):
        bucket = f"banner-landing-erp-{args.account_id}"
        key = f"orders/sales_orders/{args.batch_date}/orders_{args.batch_date}.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=gen_orders_parquet(args.rows, seed + 1))
        print(f"uploaded s3://{bucket}/{key} ({args.rows} rows, Parquet)")


if __name__ == "__main__":
    main()
