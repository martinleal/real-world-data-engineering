"""
Minimal uploader that writes a dummy JSON to S3.
- Works as AWS Lambda handler: handler(event, context)
- Works locally: python upload_dummy_to_s3.py --bucket my-bucket

Environment variables used (optional):
- DATA_BUCKET or BUCKET_NAME: target S3 bucket
- STAGE: stage name to include in object key (default 'dev')

Note: boto3 is available in the Lambda Python runtime. For local testing, ensure AWS credentials are configured.
"""
import os
import json
import uuid
import datetime
import logging
import argparse

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _get_bucket_name():
    # Prefer explicit env var names used in Terraform module
    return os.getenv("DATA_BUCKET") or os.getenv("BUCKET_NAME")


def _make_payload():
    return {
        "source": "dummy",
        "created_at": datetime.datetime.utcnow().isoformat() + "Z",
        "payload_id": str(uuid.uuid4()),
        "values": {"pm25": 12.3, "pm10": 21.7}
    }


def upload_json_to_s3(bucket: str, obj_key: str, payload: dict) -> None:
    s3 = boto3.client("s3")
    body = json.dumps(payload).encode("utf-8")
    try:
        s3.put_object(Bucket=bucket, Key=obj_key, Body=body, ContentType="application/json")
        log.info("Uploaded to s3://%s/%s", bucket, obj_key)
    except ClientError as e:
        log.exception("Failed uploading to S3: %s", e)
        raise


def build_key(stage: str = "dev") -> str:
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return f"ingestion/{stage}/dummy_{ts}_{uuid.uuid4().hex}.json"


def handler(event, context):
    """AWS Lambda handler-compatible entrypoint."""
    bucket = _get_bucket_name()
    if not bucket:
        raise ValueError("Bucket name not configured. Set DATA_BUCKET or BUCKET_NAME environment variable.")
    stage = os.getenv("STAGE", "dev")
    payload = _make_payload()
    key = build_key(stage)
    upload_json_to_s3(bucket, key, payload)
    return {"status": "ok", "bucket": bucket, "key": key}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload a dummy JSON to S3 (for testing).")
    parser.add_argument("--bucket", help="S3 bucket name (overrides env vars)")
    parser.add_argument("--stage", default=None, help="Stage to use in S3 key (overrides STAGE env var)")
    args = parser.parse_args()

    bucket = args.bucket or _get_bucket_name()
    if not bucket:
        parser.error("Bucket name required via --bucket or DATA_BUCKET/BUCKET_NAME env var")

    # If --bucket was provided, expose it to the handler via env var so the
    # same handler code works both as CLI and as Lambda (which reads env vars).
    if args.bucket:
        os.environ["DATA_BUCKET"] = args.bucket

    if args.stage:
        os.environ["STAGE"] = args.stage

    result = handler({}, None)
    print(json.dumps(result, indent=2))
