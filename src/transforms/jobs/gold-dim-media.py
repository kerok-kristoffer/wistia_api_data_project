from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from typing import Dict, Optional
import argparse


def build_spark(
    app_name: str,
    *,
    use_s3a: bool = True,
    extra_confs: Optional[Dict[str, str]] = None,
) -> SparkSession:
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


def read_media_snapshot(spark: SparkSession, silver_uri: str) -> DataFrame:
    path = f"{silver_uri.rstrip('/')}/media_snapshot"
    df = spark.read.parquet(path)
    return df


def to_dim_media(df: DataFrame) -> DataFrame:
    """
    Map silver snapshot -> dim_media schema.

    Based on your silver media snapshot:
      media_id, name, duration_s, created_at, updated_at, status,
      archived, section, project_id, project_name, thumbnail_url,
      progress, ingested_at
    """
    dim = df.select(
        col("media_id").alias("media_id"),
        col("name").alias("title"),
        col("duration_s").alias("duration_s"),
        col("created_at").alias("created_at"),
        col("updated_at").alias("updated_at"),
        col("status").alias("status"),
        col("archived").alias("archived"),
        col("section").alias("section"),
        col("project_id").alias("project_id"),
        col("project_name").alias("project_name"),
        col("thumbnail_url").alias("thumbnail_url"),
        col("progress").alias("progress"),
        col("ingested_at").alias("silver_ingested_at"),
    ).withColumn("gold_ingested_at", current_timestamp())
    return dim


def write_dim_media(df: DataFrame, gold_uri: str):
    dest = f"{gold_uri.rstrip('/')}/dim_media"
    # For small-ish dim, overwrite the whole thing each run
    df.coalesce(1).write.mode("overwrite").parquet(dest)


def run_job(args_list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-uri", required=True)
    parser.add_argument("--gold-uri", required=True)
    # `--day` is accepted but not used directly; kept for API symmetry
    parser.add_argument("--day", required=True)

    args, unknown = parser.parse_known_args(args_list)
    if unknown:
        print(f"[gold_dim_media] Ignoring unknown args from Glue: {unknown}")

    spark = build_spark("wistia-gold-dim-media")

    snap = read_media_snapshot(spark, args.silver_uri)
    if snap.rdd.isEmpty():
        print("[gold_dim_media] Snapshot is empty, nothing to write.")
        return

    dim = to_dim_media(snap)
    write_dim_media(dim, args.gold_uri)


if __name__ == "__main__":
    run_job()
