#!/usr/bin/env bash
#
# Provision the landing-side AWS resources for the dynamic bronze framework:
#   - 2 landing buckets (one per domain): banner-landing-crm-<acct>, banner-landing-erp-<acct>
#   - 1 SNS topic that both buckets publish ObjectCreated events to
#   - 1 SQS queue subscribed to the topic (raw message delivery)
#   - bucket notifications -> SNS
#   - 1 IAM role for the orchestrator, assumable by Unity Catalog for a SERVICE credential
#     (short-lived STS creds, no static access keys)
#
# Idempotent: safe to re-run. All names are derived from ACCOUNT_ID so they are globally unique.
#
# Usage:
#   AWS_PROFILE=aws-sandbox-field-eng_databricks-sandbox-admin \
#   UC_MASTER_ROLE_ARN=arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-XXXX \
#   UC_EXTERNAL_ID=<metastore-external-id> \
#     ./setup_aws.sh
#
# Get UC_MASTER_ROLE_ARN + UC_EXTERNAL_ID from any existing UC credential in the metastore:
#   databricks credentials list-credentials -o json
#   (fields: aws_iam_role.unity_catalog_iam_arn and aws_iam_role.external_id)
set -euo pipefail

PROFILE="${AWS_PROFILE:-aws-sandbox-field-eng_databricks-sandbox-admin}"
REGION="${AWS_REGION:-us-west-2}"
ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --profile "$PROFILE" --query Account --output text)}"

# Unity Catalog trust inputs (required for the service-credential role trust policy).
UC_MASTER_ROLE_ARN="${UC_MASTER_ROLE_ARN:?set UC_MASTER_ROLE_ARN (aws_iam_role.unity_catalog_iam_arn from an existing UC credential)}"
UC_EXTERNAL_ID="${UC_EXTERNAL_ID:?set UC_EXTERNAL_ID (aws_iam_role.external_id / metastore external id)}"

CRM_BUCKET="banner-landing-crm-${ACCOUNT_ID}"
ERP_BUCKET="banner-landing-erp-${ACCOUNT_ID}"
TOPIC_NAME="banner-bronze-landing-events"
QUEUE_NAME="banner-bronze-landing-queue"
ORCH_ROLE="banner-bronze-orchestrator-svccred"
ORCH_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ORCH_ROLE}"

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

echo "-- orchestrator IAM role (assumable by Unity Catalog) + least-privilege SQS policy"
# Self-assuming trust: UC master role AND the role's own ARN, gated by the metastore external id.
TRUST_POLICY=$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
  "Principal":{"AWS":["$UC_MASTER_ROLE_ARN","$ORCH_ROLE_ARN"]},
  "Action":["sts:AssumeRole","sts:TagSession"],
  "Condition":{"StringEquals":{"sts:ExternalId":"$UC_EXTERNAL_ID"}}}]}
JSON
)
if awscli iam get-role --role-name "$ORCH_ROLE" >/dev/null 2>&1; then
  awscli iam update-assume-role-policy --role-name "$ORCH_ROLE" --policy-document "$TRUST_POLICY"
  echo "  updated trust on $ORCH_ROLE"
else
  awscli iam create-role --role-name "$ORCH_ROLE" --assume-role-policy-document "$TRUST_POLICY" >/dev/null
  echo "  created role $ORCH_ROLE"
fi
ORCH_POLICY=$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:DeleteMessageBatch","sqs:GetQueueAttributes"],"Resource":"$QUEUE_ARN"}]}
JSON
)
awscli iam put-role-policy --role-name "$ORCH_ROLE" --policy-name sqs-read-delete \
  --policy-document "$ORCH_POLICY"

echo
echo "============================================================"
echo "DONE. Save these for the bundle:"
echo "  AWS_REGION     = $REGION"
echo "  QUEUE_URL      = $QUEUE_URL"
echo "  CRM_BUCKET     = $CRM_BUCKET"
echo "  ERP_BUCKET     = $ERP_BUCKET"
echo "  SNS_TOPIC      = $TOPIC_ARN"
echo "  ORCH_ROLE_ARN  = $ORCH_ROLE_ARN"
echo
echo "Create the UC SERVICE credential (name must match var.service_credential in databricks.yml)"
echo "and grant the orchestrator's run-as identity ACCESS on it:"
echo "  databricks credentials create-credential --json '{\"name\":\"banner_bronze_orchestrator\",\"purpose\":\"SERVICE\",\"aws_iam_role\":{\"role_arn\":\"$ORCH_ROLE_ARN\"}}'"
echo "  databricks grants update credential banner_bronze_orchestrator --json '{\"changes\":[{\"principal\":\"<orchestrator-run-as>\",\"add\":[\"ACCESS\"]}]}'"
echo "============================================================"
