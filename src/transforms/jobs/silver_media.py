from __future__ import annotations

try:
    from pyspark.errors import AnalysisException
except ImportError:
    from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    input_file_name,
    to_timestamp,
    current_timestamp,
    regexp_extract,
    to_date,
    lit,
)

import argparse
from typing import Dict, Optional


def build_spark(
    app_name: str,
    *,
    use_s3a: bool = True,
    extra_confs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Standardized SparkSession:
      - dynamic partition overwrite
      - S3A filesystem + default AWS creds chain (OIDC / instance / env / ~/.aws)
      - Optional extra configs per job
    """
    builder = SparkSession.builder.appName(app_name).config(
        "spark.sql.sources.partitionOverwriteMode", "dynamic"
    )

    if use_s3a:
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
        )

    if extra_confs:
        for k, v in extra_confs.items():
            builder = builder.config(k, v)

    return builder.getOrCreate()


def read_raw(spark: SparkSession, input_uri: str, day: str) -> DataFrame:
    base = f"{input_uri.rstrip('/')}/media/dt={day}"
    df = spark.read.json(base).withColumn("_source_file", input_file_name())
    rx = r".*[/\\]media_id=([^/\\]+)[/\\].*"
    df = df.withColumn("media_id", regexp_extract(col("_source_file"), rx, 1))
    df = df.withColumn("dt", to_date(lit(day)))
    return df


def project(df: DataFrame) -> DataFrame:
    return df.select(
        col("media_id").alias("media_id"),
        col("name").alias("name"),
        col("duration").cast("double").alias("duration_s"),
        to_timestamp(col("created")).alias("created_at"),
        to_timestamp(col("updated")).alias("updated_at"),
        col("status").alias("status"),
        col("archived").cast("boolean").alias("archived"),
        col("section").alias("section"),
        col("project.id").cast("long").alias("project_id"),
        col("project.name").alias("project_name"),
        col("thumbnail.url").alias("thumbnail_url"),
        col("progress").cast("double").alias("progress"),
        col("dt").alias("dt"),
    ).withColumn("ingested_at", current_timestamp())


def write_history(df: DataFrame, output_uri: str, day: str):
    dest = f"{output_uri.rstrip('/')}/media_history"
    (
        df.repartition("dt", "media_id")
        .write.mode("overwrite")
        .partitionBy("dt", "media_id")
        .option("replaceWhere", f"dt = DATE '{day}'")
        .parquet(dest)
    )


def write_snapshot(df: DataFrame, output_uri: str):
    # Take latest by updated_at within each media_id for this batch
    # (If multiple days land together, you might window; here batch is one day.)
    dest = f"{output_uri.rstrip('/')}/media_snapshot"
    # Simple approach: overwrite snapshot entirely for now (small dimension).
    (df.drop("dt").dropDuplicates(["media_id"]).write.mode("overwrite").parquet(dest))


def run_job(args_list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-uri", required=True)
    parser.add_argument("--output-uri", required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument(
        "--no-snapshot", action="store_true", help="Skip snapshot write"
    )

    # Let Glue’s own flags (like --JOB_NAME, --enable-metrics, etc.) pass through
    args, unknown = parser.parse_known_args(args_list)
    if unknown:
        print(f"[silver_media] Ignoring unknown args from Glue: {unknown}")

    spark = build_spark("silver-wistia-media")

    try:
        raw = read_raw(spark, args.input_uri, args.day)
    except AnalysisException as e:
        # Handle the "path does not exist" case as a clean no-op
        if "Path does not exist" in str(e):
            print(f"[silver_media] No raw path for day={args.day}, skipping.")
            return
        raise

    proj = project(raw)
    write_history(proj, args.output_uri, args.day)
    if not args.no_snapshot:
        write_snapshot(proj, args.output_uri)
    raw = read_raw(spark, args.input_uri, args.day)
    if raw.rdd.isEmpty():
        return

    proj = project(raw)
    write_history(proj, args.output_uri, args.day)
    if not args.no_snapshot:
        write_snapshot(proj, args.output_uri)


if __name__ == "__main__":
    run_job()
