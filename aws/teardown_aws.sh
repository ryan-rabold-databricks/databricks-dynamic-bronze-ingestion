#!/usr/bin/env bash
# Tear down the AWS resources created by setup_aws.sh. Buckets are emptied first.
# Usage: AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin ./teardown_aws.sh
set -uo pipefail

PROFILE="${AWS_PROFILE:-aws-sandbox-field-eng_databricks-sandbox-admin}"
REGION="${AWS_REGION:-us-east-2}"
ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --profile "$PROFILE" --query Account --output text)}"

CRM_BUCKET="banner-landing-crm-${ACCOUNT_ID}"
ERP_BUCKET="banner-landing-erp-${ACCOUNT_ID}"
QUEUE_NAME="banner-bronze-landing-queue"
TOPIC_NAME="banner-bronze-landing-events"
IAM_USER="banner-bronze-orchestrator"

awscli() { aws --profile "$PROFILE" --region "$REGION" "$@"; }

for b in "$CRM_BUCKET" "$ERP_BUCKET"; do
  echo "emptying + deleting $b"
  awscli s3 rm "s3://$b" --recursive 2>/dev/null
  awscli s3api delete-bucket --bucket "$b" 2>/dev/null
done

QUEUE_URL=$(awscli sqs get-queue-url --queue-name "$QUEUE_NAME" --query QueueUrl --output text 2>/dev/null)
[ -n "${QUEUE_URL:-}" ] && awscli sqs delete-queue --queue-url "$QUEUE_URL" && echo "deleted queue"

TOPIC_ARN=$(awscli sns list-topics --query "Topics[?contains(TopicArn, '$TOPIC_NAME')].TopicArn | [0]" --output text 2>/dev/null)
[ -n "${TOPIC_ARN:-}" ] && [ "$TOPIC_ARN" != "None" ] && awscli sns delete-topic --topic-arn "$TOPIC_ARN" && echo "deleted topic"

for k in $(awscli iam list-access-keys --user-name "$IAM_USER" --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null); do
  awscli iam delete-access-key --user-name "$IAM_USER" --access-key-id "$k"
done
awscli iam delete-user-policy --user-name "$IAM_USER" --policy-name sqs-read-delete 2>/dev/null
awscli iam delete-user --user-name "$IAM_USER" 2>/dev/null && echo "deleted iam user"

echo "Note: UC storage credential / external locations / catalog are NOT deleted here."
echo "Remove via Databricks if desired: external locations banner_landing_*, credential banner_bronze_landing."
