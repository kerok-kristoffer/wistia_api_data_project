# src/jobs/gold/wistia-gold-fact-media-daily.py

from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    sum as ssum,
    countDistinct,
    when,
    lit,
    current_timestamp,
)
from pyspark.sql.utils import AnalysisException
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


def read_silver_visitors_for_day(
    spark: SparkSession, silver_uri: str, day: str
) -> DataFrame:
    path = f"{silver_uri.rstrip('/')}/visitors/dt={day}"
    print(f"[gold_fact_media_daily] Reading visitors from {path}")
    df = spark.read.parquet(path)
    return df


def aggregate_fact(df: DataFrame) -> DataFrame:
    """
    Input schema (from silver visitors):
      dt, media_id, visitor_key, load_count, play_count, name, email, ingested_at

    We aggregate to (dt, media_id).
    """
    agg = (
        df.groupBy("dt", "media_id")
        .agg(
            ssum("load_count").alias("loads"),
            ssum("play_count").alias("plays"),
            countDistinct("visitor_key").alias("unique_visitors"),
        )
        .withColumn(
            "play_rate",
            when(col("loads") > lit(0), col("plays") / col("loads")).otherwise(
                lit(0.0)
            ),
        )
        .withColumn("gold_ingested_at", current_timestamp())
        .select(
            col("dt"),
            col("media_id"),
            col("loads"),
            col("plays"),
            col("unique_visitors"),
            col("play_rate"),
            col("gold_ingested_at"),
        )
    )
    return agg


def write_fact(df: DataFrame, gold_uri: str, day: str):
    dest = f"{gold_uri.rstrip('/')}/fact_media_daily"
    (
        df.repartition("dt")
        .write.mode("overwrite")
        .partitionBy("dt")
        .option("replaceWhere", f"dt = DATE '{day}'")
        .parquet(dest)
    )
    print(f"[gold_fact_media_daily] Wrote fact rows for dt={day} to {dest}")


def run_job(args_list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-uri", required=True)
    parser.add_argument("--gold-uri", required=True)
    parser.add_argument("--day", required=True)

    args, unknown = parser.parse_known_args(args_list)
    if unknown:
        print(f"[gold_fact_media_daily] Ignoring unknown args from Glue: {unknown}")

    spark = build_spark("wistia-gold-fact-media-daily")

    try:
        visitors = read_silver_visitors_for_day(spark, args.silver_uri, args.day)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            print(
                f"[gold_fact_media_daily] No visitors path for dt={args.day}, skipping."
            )
            return
        raise

    if visitors.rdd.isEmpty():
        print(f"[gold_fact_media_daily] Empty visitors for dt={args.day}, skipping.")
        return

    fact = aggregate_fact(visitors)
    write_fact(fact, args.gold_uri, args.day)


if __name__ == "__main__":
    run_job()
