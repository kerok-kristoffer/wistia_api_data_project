from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    input_file_name,
    current_timestamp,
    lit,
    coalesce,
    regexp_extract,
    max as smax,
    to_date,
)
from pyspark.sql.types import LongType, StringType
import argparse

from transforms.utils.spark import build_spark


def read_raw(spark: SparkSession, input_uri: str, day: str) -> DataFrame:
    # Load the raw JSONL for a single day
    base = f"{input_uri.rstrip('/')}/visitors/dt={day}"
    df = spark.read.json(base).withColumn("_source_file", input_file_name())  # JSONL ok
    # Pull media_id from path
    rx = r".*[/\\]media_id=([^/\\]+)[/\\].*"
    df = df.withColumn("media_id", regexp_extract(col("_source_file"), rx, 1))
    df = df.withColumn("dt", to_date(lit(day)))
    return df


def transform(df: DataFrame) -> DataFrame:
    # Flatten identity; coalesce counts
    df1 = (
        df.withColumn("visitor_key", col("visitor_key").cast(StringType()))
        .withColumn("load_count", coalesce(col("load_count").cast(LongType()), lit(0)))
        .withColumn("play_count", coalesce(col("play_count").cast(LongType()), lit(0)))
        .withColumn("name", col("visitor_identity.name").cast(StringType()))
        .withColumn("email", col("visitor_identity.email").cast(StringType()))
        .withColumn("ingested_at", current_timestamp())
    )

    # Deduplicate by visitor_key within (dt, media_id)
    # Choose max counts as a simple aggregator
    win_agg = df1.groupBy("dt", "media_id", "visitor_key").agg(
        smax("load_count").alias("load_count"),
        smax("play_count").alias("play_count"),
        smax("name").alias("name"),
        smax("email").alias("email"),
    )
    out = win_agg.withColumn("ingested_at", current_timestamp()).select(
        "dt",
        "media_id",
        "visitor_key",
        "load_count",
        "play_count",
        "name",
        "email",
        "ingested_at",
    )
    return out


def write(out: DataFrame, output_uri: str, day: str):
    dest = f"{output_uri.rstrip('/')}/visitors"
    # Overwrite only the day partition (and media_id subpartitions)
    (
        out.repartition("dt", "media_id")
        .write.mode("overwrite")
        .partitionBy("dt", "media_id")
        .option("replaceWhere", f"dt = DATE '{day}'")
        .parquet(dest)
    )


def run_job(args_list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-uri", required=True)
    parser.add_argument("--output-uri", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args(args_list)

    spark = build_spark("silver-wistia-visitors")
    raw = read_raw(spark, args.input_uri, args.day)
    if raw.rdd.isEmpty():
        # No-op write (keeps pipeline green)
        return

    out = transform(raw)
    write(out, args.output_uri, args.day)


if __name__ == "__main__":
    run_job()
