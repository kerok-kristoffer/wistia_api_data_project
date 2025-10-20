#!/usr/bin/env python3
from __future__ import annotations
import argparse
import os
import sys
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


def account_id() -> str:
    return boto3.client("sts").get_caller_identity()["Account"]


def bucket_exists(s3, name: str) -> bool:
    try:
        s3.head_bucket(Bucket=name)
        return True
    except ClientError:
        return False


def ensure_bucket(s3, name: str, region: str):
    if bucket_exists(s3, name):
        print(f"✓ {name} exists")
        return
    kwargs = {"Bucket": name}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)
    print(f"+ created {name}")
    # Block public access
    s3.put_public_access_block(
        Bucket=name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    # Default encryption (SSE-S3 or SSE-KMS)
    kms_arn = os.getenv("KMS_KEY_ARN")
    if kms_arn:
        algo = {"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": kms_arn}
    else:
        algo = {"SSEAlgorithm": "AES256"}
    s3.put_bucket_encryption(
        Bucket=name,
        ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": algo}]
        },
    )


def main():
    load_dotenv(override=False)
    ap = argparse.ArgumentParser("Bootstrap S3 buckets")
    ap.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    ap.add_argument("--env", default=os.getenv("ENV", "dev"))
    ap.add_argument("--project", default=os.getenv("PROJECT_SLUG", "wistia-pipeline"))
    ap.add_argument("--prefix", default=os.getenv("S3_PREFIX", "wistia/"))
    # Optional explicit names (override computed)
    ap.add_argument("--raw-bucket", default=os.getenv("RAW_BUCKET") or "")
    ap.add_argument("--silver-bucket", default=os.getenv("SILVER_BUCKET") or "")
    ap.add_argument("--gold-bucket", default=os.getenv("GOLD_BUCKET") or "")
    ap.add_argument("--glue-temp-bucket", default=os.getenv("GLUE_TEMP_BUCKET") or "")
    ap.add_argument(
        "--athena-results-bucket", default=os.getenv("ATHENA_RESULTS_BUCKET") or ""
    )
    args = ap.parse_args()

    acct = account_id()

    def name(default_tier: str, override: str) -> str:
        return (
            override or f"{args.project}-{args.env}-{default_tier}-{acct}-{args.region}"
        )

    buckets = {
        "raw": name("raw", args.raw_bucket),
        "silver": name("silver", args.silver_bucket),
        "gold": name("gold", args.gold_bucket),
        "glue_temp": name("gluetemp", args.glue_temp_bucket),
        "athena": name("athenares", args.athena_results_bucket),
    }

    s3 = boto3.client("s3", region_name=args.region)
    for b in buckets.values():
        ensure_bucket(s3, b, args.region)

    # Create useful prefixes
    s3r = boto3.resource("s3", region_name=args.region)
    for tier, b in buckets.items():
        for p in [args.prefix, f"{args.prefix}{tier}/", f"{args.prefix}{tier}/_logs/"]:
            s3r.Object(b, p).put(Body=b"")  # creates empty keys as folder markers
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
