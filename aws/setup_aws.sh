#!/usr/bin/env bash
#
# Provision the landing-side AWS resources for the dynamic bronze framework:
#   - 2 landing buckets (one per domain): banner-landing-crm-<acct>, banner-landing-erp-<acct>
#   - 1 SNS topic that both buckets publish ObjectCreated events to
#   - 1 SQS queue subscribed to the topic (raw message delivery)
#   - bucket notifications -> SNS
#   - 1 IAM user for the orchestrator with least-privilege SQS access (prints access keys)
#
# Idempotent: safe to re-run. All names are derived from ACCOUNT_ID so they are globally unique.
#
# Usage:
#   AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin ./setup_aws.sh
set -euo pipefail

PROFILE="${AWS_PROFILE:-aws-sandbox-field-eng_databricks-sandbox-admin}"
REGION="${AWS_REGION:-us-east-2}"
ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --profile "$PROFILE" --query Account --output text)}"

CRM_BUCKET="banner-landing-crm-${ACCOUNT_ID}"
ERP_BUCKET="banner-landing-erp-${ACCOUNT_ID}"
TOPIC_NAME="banner-bronze-landing-events"
QUEUE_NAME="banner-bronze-landing-queue"
IAM_USER="banner-bronze-orchestrator"

awscli() { aws --profile "$PROFILE" --region "$REGION" "$@"; }

echo "== Account=$ACCOUNT_ID region=$REGION =="

create_bucket() {
  local b="$1"
  if awscli s3api head-bucket --bucket "$b" 2>/dev/null; then
    echo "  bucket $b exists"
  else
    awscli s3api create-bucket --bucket "$b" \
      --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
    echo "  created bucket $b"
  fi
}
echo "-- buckets"
create_bucket "$CRM_BUCKET"
create_bucket "$ERP_BUCKET"

echo "-- SNS topic"
TOPIC_ARN=$(awscli sns create-topic --name "$TOPIC_NAME" --query TopicArn --output text)
echo "  $TOPIC_ARN"

echo "-- SQS queue"
QUEUE_URL=$(awscli sqs create-queue --queue-name "$QUEUE_NAME" \
  --attributes "ReceiveMessageWaitTimeSeconds=10,VisibilityTimeout=120,MessageRetentionPeriod=345600" \
  --query QueueUrl --output text)
QUEUE_ARN=$(awscli sqs get-queue-attributes --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
echo "  $QUEUE_URL"
echo "  $QUEUE_ARN"

echo "-- SQS access policy (allow SNS to send)"
SQS_ATTRS=$(mktemp)
QUEUE_ARN="$QUEUE_ARN" TOPIC_ARN="$TOPIC_ARN" python3 - >"$SQS_ATTRS" <<'PY'
import json, os
policy = {"Version":"2012-10-17","Statement":[{"Sid":"AllowSNS","Effect":"Allow",
  "Principal":{"Service":"sns.amazonaws.com"},"Action":"sqs:SendMessage",
  "Resource":os.environ["QUEUE_ARN"],
  "Condition":{"ArnEquals":{"aws:SourceArn":os.environ["TOPIC_ARN"]}}}]}
print(json.dumps({"Policy": json.dumps(policy)}))
PY
awscli sqs set-queue-attributes --queue-url "$QUEUE_URL" --attributes "file://$SQS_ATTRS" >/dev/null
rm -f "$SQS_ATTRS"

echo "-- subscribe SQS to SNS (raw delivery)"
SUB_ARN=$(awscli sns subscribe --topic-arn "$TOPIC_ARN" --protocol sqs \
  --notification-endpoint "$QUEUE_ARN" --attributes RawMessageDelivery=true \
  --return-subscription-arn --query SubscriptionArn --output text)
echo "  $SUB_ARN"

echo "-- SNS topic policy (allow both buckets to publish)"
SNS_POLICY=$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Sid":"AllowS3Publish","Effect":"Allow","Principal":{"Service":"s3.amazonaws.com"},"Action":"sns:Publish","Resource":"$TOPIC_ARN","Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:s3:::banner-landing-*-${ACCOUNT_ID}"},"StringEquals":{"aws:SourceAccount":"$ACCOUNT_ID"}}}]}
JSON
)
awscli sns set-topic-attributes --topic-arn "$TOPIC_ARN" \
  --attribute-name Policy --attribute-value "$SNS_POLICY" >/dev/null

put_notification() {
  local b="$1"
  awscli s3api put-bucket-notification-configuration --bucket "$b" \
    --notification-configuration "{\"TopicConfigurations\":[{\"TopicArn\":\"$TOPIC_ARN\",\"Events\":[\"s3:ObjectCreated:*\"]}]}"
  echo "  notification set on $b"
}
echo "-- bucket notifications"
put_notification "$CRM_BUCKET"
put_notification "$ERP_BUCKET"

echo "-- orchestrator IAM user + least-privilege SQS policy"
awscli iam get-user --user-name "$IAM_USER" >/dev/null 2>&1 || awscli iam create-user --user-name "$IAM_USER" >/dev/null
ORCH_POLICY=$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:DeleteMessageBatch","sqs:GetQueueAttributes"],"Resource":"$QUEUE_ARN"}]}
JSON
)
awscli iam put-user-policy --user-name "$IAM_USER" --policy-name sqs-read-delete \
  --policy-document "$ORCH_POLICY"

echo
echo "============================================================"
echo "DONE. Save these for the bundle + secret scope:"
echo "  AWS_REGION   = $REGION"
echo "  QUEUE_URL    = $QUEUE_URL"
echo "  CRM_BUCKET   = $CRM_BUCKET"
echo "  ERP_BUCKET   = $ERP_BUCKET"
echo "  SNS_TOPIC    = $TOPIC_ARN"
echo
echo "Create orchestrator access keys (run once, store in the Databricks secret scope):"
echo "  aws --profile $PROFILE iam create-access-key --user-name $IAM_USER"
echo "============================================================"
