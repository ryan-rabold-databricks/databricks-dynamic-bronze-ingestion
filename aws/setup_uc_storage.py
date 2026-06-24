"""
Create the UC storage credential + external locations for the two landing buckets.

Implements the standard Databricks <-> AWS IAM trust handshake:
  1. Create an IAM role with an S3 access policy for the landing buckets (+ self-assume).
  2. Create a Databricks storage credential pointing at the role ARN.
  3. Read back the credential's external_id + Databricks principal ARN.
  4. Rewrite the role trust policy to trust that principal with the external_id (+ self-assume).
  5. Create one external location per bucket and validate.

Idempotent and retried (IAM changes take seconds to propagate).

Usage:
  python aws/setup_uc_storage.py \
    --databricks-profile banner-bronze \
    --aws-profile aws-sandbox-field-eng_databricks-sandbox-admin \
    --account-id 000000000000 --region us-east-2
"""

import argparse
import json
import time

import boto3
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AwsIamRole

ROLE_NAME = "banner-bronze-uc-access"
CRED_NAME = "banner_bronze_landing"


def s3_access_policy(buckets, role_arn):
    resources = []
    for b in buckets:
        resources += [f"arn:aws:s3:::{b}", f"arn:aws:s3:::{b}/*"]
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                    "s3:ListBucket", "s3:GetBucketLocation",
                ],
                "Resource": resources,
            },
            {"Effect": "Allow", "Action": ["sts:AssumeRole"], "Resource": role_arn},
        ],
    }


def trust_policy(principal_arns, external_id):
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": principal_arns},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
            }
        ],
    }


def ensure_role(iam, role_arn, account_id):
    """Create the role with a bootstrap trust (self) if missing; return its ARN."""
    bootstrap_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    try:
        iam.get_role(RoleName=ROLE_NAME)
        print(f"  role {ROLE_NAME} exists")
    except iam.exceptions.NoSuchEntityException:
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(bootstrap_trust),
            Description="Unity Catalog storage credential role for Banner bronze landing buckets",
        )
        print(f"  created role {ROLE_NAME}")
    return role_arn


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--databricks-profile", required=True)
    ap.add_argument("--aws-profile", required=True)
    ap.add_argument("--account-id", required=True)
    ap.add_argument("--region", default="us-east-2")
    args = ap.parse_args()

    crm_bucket = f"banner-landing-crm-{args.account_id}"
    erp_bucket = f"banner-landing-erp-{args.account_id}"
    buckets = [crm_bucket, erp_bucket]
    role_arn = f"arn:aws:iam::{args.account_id}:role/{ROLE_NAME}"

    session = boto3.Session(profile_name=args.aws_profile, region_name=args.region)
    iam = session.client("iam")
    w = WorkspaceClient(profile=args.databricks_profile)

    print("-- IAM role")
    ensure_role(iam, role_arn, args.account_id)
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="s3-landing-access",
        PolicyDocument=json.dumps(s3_access_policy(buckets, role_arn)),
    )
    print("  attached S3 access policy")

    print("-- Databricks storage credential")
    existing = {c.name: c for c in w.storage_credentials.list()}
    if CRED_NAME in existing:
        cred = w.storage_credentials.get(name=CRED_NAME)
        print(f"  credential {CRED_NAME} exists")
    else:
        cred = w.storage_credentials.create(
            name=CRED_NAME,
            aws_iam_role=AwsIamRole(role_arn=role_arn),
            comment="Access to Banner bronze landing buckets",
        )
        print(f"  created credential {CRED_NAME}")

    external_id = cred.aws_iam_role.external_id
    uc_principal = cred.aws_iam_role.unity_catalog_iam_arn
    print(f"  external_id={external_id}")
    print(f"  uc_principal={uc_principal}")

    print("-- finalize role trust (trust Databricks principal + self, gated by external id)")
    iam.update_assume_role_policy(
        RoleName=ROLE_NAME,
        PolicyDocument=json.dumps(trust_policy([uc_principal, role_arn], external_id)),
    )

    print("-- external locations (with retry for IAM propagation)")
    locations = {
        "banner_landing_crm": f"s3://{crm_bucket}/",
        "banner_landing_erp": f"s3://{erp_bucket}/",
    }
    existing_locs = {l.name for l in w.external_locations.list()}
    for name, url in locations.items():
        if name in existing_locs:
            print(f"  external location {name} exists")
            continue
        last_err = None
        for attempt in range(1, 11):
            try:
                w.external_locations.create(name=name, url=url, credential_name=CRED_NAME)
                print(f"  created external location {name} -> {url}")
                last_err = None
                break
            except Exception as e:  # noqa: BLE001 - retry IAM propagation / validation errors
                last_err = e
                print(f"  attempt {attempt}/10 for {name} failed: {e}")
                time.sleep(15)
        if last_err:
            raise last_err

    print("DONE.")


if __name__ == "__main__":
    main()
