# src/transforms/jobs/silver_events.py
import argparse
import json
import os
import sys
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType

from transforms.utils.ip_hash import hmac_sha256_hex


def _build_spark():
    return (
        SparkSession.builder.appName("silver-wistia-…")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # S3A filesystem + default creds chain (reads ~/.aws/credentials)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        # Pull the S3 jars automatically:
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .getOrCreate()
    )


def _resolve_ip_key(cli_key: Optional[str]) -> Optional[str]:
    # Priority: CLI -> ENV -> Secrets Manager (WISTIA_SECRET_ARN.ip_hash_key)
    if cli_key:
        return cli_key
    k = os.getenv("VISITOR_IP_HMAC_KEY")
    if k:
        return k
    arn = os.getenv("WISTIA_SECRET_ARN")
    if not arn:
        return None
    try:
        import boto3

        sm = boto3.client("secretsmanager")
        payload = sm.get_secret_value(SecretId=arn).get("SecretString") or "{}"
        data = json.loads(payload)
        return data.get("ip_hash_key")
    except Exception:
        return None


def _input_glob(input_uri: str, day: str) -> str:
    return f"{input_uri.rstrip('/')}/events/dt={day}/media_id=*/page=*.jsonl"


def _with_media_from_path(df: DataFrame) -> DataFrame:
    if "media_id" in df.columns:
        return df
    return df.withColumn(
        "media_id", F.regexp_extract(F.input_file_name(), r"media_id=([^/]+)", 1)
    )


def _transform(df: DataFrame, day: str, ip_key: Optional[str]) -> DataFrame:
    # Optional casts (keep schema flexible; do light typing here)
    # 'at' can be string; try casting to timestamp if ISO8601
    if "at" in df.columns:
        df = df.withColumn("at", F.to_timestamp("at"))
    if "position_s" in df.columns:
        df = df.withColumn("position_s", F.col("position_s").cast(DoubleType()))

    # IP privacy
    if "ip" in df.columns:
        if ip_key:
            hash_udf = F.udf(lambda v: hmac_sha256_hex(v, ip_key), StringType())
            df = df.withColumn("ip_hmac", hash_udf(F.col("ip")))
        else:
            df = df.withColumn("ip_hmac", F.lit(None).cast("string"))
        df = df.drop("ip")
    else:
        df = df.withColumn("ip_hmac", F.lit(None).cast("string"))

    df = _with_media_from_path(df)
    df = df.withColumn("dt", F.lit(day)).withColumn(
        "processed_at", F.current_timestamp()
    )
    return df


def _write(df: DataFrame, output_uri: str):
    dest = f"{output_uri.rstrip('/')}/events"
    (
        df.repartition("dt", "media_id")
        .write.mode("overwrite")
        .format("parquet")
        .partitionBy("dt", "media_id")
        .save(dest)
    )


def main(argv=None):
    ap = argparse.ArgumentParser(description="Silver transform for Wistia events")
    ap.add_argument(
        "--input-uri",
        required=True,
        help="s3://<bucket>/<prefix>/wistia or file:///...",
    )
    ap.add_argument(
        "--output-uri",
        required=True,
        help="s3://<bucket>/<prefix>/wistia or file:///...",
    )
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--ip-hash-key", required=False, help="Override for VISITOR_IP_HMAC_KEY/Secrets"
    )
    args = ap.parse_args(argv or sys.argv[1:])

    spark = _build_spark()
    ip_key = _resolve_ip_key(args.ip_hash_key)

    src = _input_glob(args.input_uri, args.day)
    try:
        df = spark.read.json(src)
    except Exception:
        print(f"[silver_events] No input found for day={args.day} at {src}")
        return

    if df.rdd.isEmpty():
        print(f"[silver_events] No input found for day={args.day} at {src}")
        return

    out = _transform(df, day=args.day, ip_key=ip_key)
    _write(out, args.output_uri)


if __name__ == "__main__":
    main()
