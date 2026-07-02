"""
Central routing orchestrator (Option B).

Triggered on a short schedule (or by a file-arrival trigger). On each run it:

  1. Drains the SQS queue that receives S3 ObjectCreated events from every landing bucket.
  2. Parses each event to (bucket, object_key).
  3. Maps each key to a `pipeline_group` using the control config table (bucket + key-prefix match).
  4. Dedupes to the DISTINCT set of pipeline groups that have pending work.
  5. Calls start_update() on ONLY those pipelines.
  6. Deletes processed messages from SQS.

What it deliberately does NOT do: it never passes file keys to the pipelines. Auto Loader
rediscovers new files from its own checkpoint. The keys are used purely for routing -- which is
the whole point of "central routing" and the reason the original file-event-key blocker is moot.

Idempotency / safety:
  - At-least-once SQS delivery is fine: re-triggering a pipeline is a no-op for already-ingested
    files (Auto Loader checkpoint dedups).
  - If a pipeline update is already running, start_update is skipped (logged), not failed.
  - Messages are deleted only after their keys have been routed.

Auth:
  - Databricks: uses the job's run-as identity (default WorkspaceClient()).
  - AWS SQS: a Unity Catalog SERVICE credential vends short-lived, auto-refreshed STS
    credentials (no static keys). Requires serverless environment version 3+ (which the
    orchestrator job uses). The job's run-as identity must have ACCESS on the credential.

Args:
  --queue-url, --config-table, --pipeline-map (JSON {group: pipeline_id}),
  --region, --service-credential, [--max-messages], [--wait-seconds], [--visibility-timeout]
"""

import argparse
import json
import sys
from urllib.parse import unquote_plus


def parse_args(argv):
    p = argparse.ArgumentParser(description="SQS -> pipeline-group routing orchestrator")
    p.add_argument("--queue-url", required=True)
    p.add_argument("--config-table", required=True)
    p.add_argument("--pipeline-map", required=True, help='JSON {"pg_crm":"<id>","pg_erp":"<id>"}')
    p.add_argument("--region", required=True)
    p.add_argument("--service-credential", required=True,
                   help="UC SERVICE credential name that grants the orchestrator SQS access")
    p.add_argument("--max-messages", type=int, default=2000)
    p.add_argument("--wait-seconds", type=int, default=10)
    p.add_argument("--visibility-timeout", type=int, default=120)
    p.add_argument("--empty-polls-before-exit", type=int, default=2)
    return p.parse_args(argv)


def load_routes(config_table):
    """Return list of {landing_bucket, landing_prefix, pipeline_group} for active sources.

    Read via Spark (available in a serverless/classic spark_python_task)."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    rows = (
        spark.read.table(config_table)
        .where("is_active = true")
        .select("landing_bucket", "landing_prefix", "pipeline_group")
        .collect()
    )
    routes = [
        {
            "landing_bucket": r["landing_bucket"],
            "landing_prefix": (r["landing_prefix"] or "").lstrip("/"),
            "pipeline_group": r["pipeline_group"],
        }
        for r in rows
    ]
    # Longest prefix first so the most specific route wins.
    routes.sort(key=lambda r: len(r["landing_prefix"]), reverse=True)
    return routes


def extract_s3_objects(body):
    """Yield (bucket, key) tuples from an SQS message body.

    Handles both raw S3 -> SQS delivery and S3 -> SNS -> SQS (SNS envelope), and skips
    S3 test events."""
    try:
        payload = json.loads(body)
    except (ValueError, TypeError):
        return

    # SNS envelope: the S3 event is a JSON string in payload["Message"].
    if isinstance(payload, dict) and payload.get("Type") == "Notification" and "Message" in payload:
        try:
            payload = json.loads(payload["Message"])
        except (ValueError, TypeError):
            return

    if not isinstance(payload, dict):
        return
    if payload.get("Event") == "s3:TestEvent":
        return

    for record in payload.get("Records", []):
        s3 = record.get("s3", {})
        bucket = s3.get("bucket", {}).get("name")
        key = s3.get("object", {}).get("key")
        if bucket and key:
            yield bucket, unquote_plus(key)


def match_group(routes, bucket, key):
    for route in routes:
        if route["landing_bucket"] == bucket and key.startswith(route["landing_prefix"]):
            return route["pipeline_group"]
    return None


def drain_queue(sqs, queue_url, routes, args):
    """Drain the queue, returning (groups_with_work, processed_message_count)."""
    groups_with_work = set()
    processed = 0
    empty_polls = 0
    pending_deletes = []

    def flush_deletes():
        # SQS DeleteMessageBatch accepts up to 10 entries.
        for i in range(0, len(pending_deletes), 10):
            chunk = pending_deletes[i : i + 10]
            sqs.delete_message_batch(QueueUrl=queue_url, Entries=chunk)
        pending_deletes.clear()

    while empty_polls < args.empty_polls_before_exit and processed < args.max_messages:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=args.wait_seconds,
            VisibilityTimeout=args.visibility_timeout,
        )
        messages = resp.get("Messages", [])
        if not messages:
            empty_polls += 1
            continue
        empty_polls = 0

        for msg in messages:
            for bucket, key in extract_s3_objects(msg.get("Body", "")):
                group = match_group(routes, bucket, key)
                if group:
                    groups_with_work.add(group)
                    print(f"[route] s3://{bucket}/{key} -> {group}")
                else:
                    print(f"[skip ] no route for s3://{bucket}/{key}")
            pending_deletes.append(
                {"Id": str(len(pending_deletes)), "ReceiptHandle": msg["ReceiptHandle"]}
            )
            processed += 1
            if len(pending_deletes) >= 10:
                flush_deletes()

    flush_deletes()
    return groups_with_work, processed


def trigger_pipeline(w, pipeline_id, group):
    """Start a triggered update; skip cleanly if one is already running."""
    from databricks.sdk.errors import DatabricksError

    try:
        resp = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
        print(f"[fire ] group={group} pipeline={pipeline_id} update={resp.update_id}")
        return True
    except DatabricksError as e:
        msg = str(e).lower()
        if "active update" in msg or "already" in msg or "in progress" in msg:
            print(f"[busy ] group={group} pipeline={pipeline_id} already updating -- skipped")
            return False
        raise


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()  # still used to trigger pipeline updates (run-as identity)

    import boto3

    # UC service credential -> short-lived, auto-refreshed STS creds. No static keys.
    # dbutils is not a global in a spark_python_task, so import it from the runtime.
    from databricks.sdk.runtime import dbutils

    sqs = boto3.Session(
        botocore_session=dbutils.credentials.getServiceCredentialsProvider(args.service_credential),
        region_name=args.region,
    ).client("sqs")

    routes = load_routes(args.config_table)
    print(f"[init ] loaded {len(routes)} active route(s): "
          f"{[(r['landing_bucket'], r['landing_prefix'], r['pipeline_group']) for r in routes]}")

    pipeline_map = json.loads(args.pipeline_map)

    groups_with_work, processed = drain_queue(sqs, args.queue_url, routes, args)
    print(f"[drain] processed {processed} message(s); "
          f"groups with work: {sorted(groups_with_work)}")

    triggered = []
    for group in sorted(groups_with_work):
        pipeline_id = pipeline_map.get(group)
        if not pipeline_id:
            print(f"[warn ] no pipeline mapped for group '{group}' -- check --pipeline-map")
            continue
        if trigger_pipeline(w, pipeline_id, group):
            triggered.append(group)

    print(f"[done ] triggered groups: {triggered}")


if __name__ == "__main__":
    main()
