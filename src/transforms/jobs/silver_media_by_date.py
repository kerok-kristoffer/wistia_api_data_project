import argparse
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, DoubleType


def _build_spark():
    return (
        SparkSession.builder.appName("silver-wistia-media-by-date")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def _input_glob(input_uri: str, day: str) -> str:
    # RAW layout: .../media_by_date/dt=YYYY-MM-DD/media_id=*/page=*.jsonl
    return f"{input_uri.rstrip('/')}/media_by_date/dt={day}/media_id=*/page=*.jsonl"


def _with_media_from_path(df: DataFrame) -> DataFrame:
    if "media_id" in df.columns:
        return df
    return df.withColumn(
        "media_id", F.regexp_extract(F.input_file_name(), r"media_id=([^/]+)", 1)
    )


def _transform(df: DataFrame, day: str) -> DataFrame:
    # Source has "date" as string YYYY-MM-DD; map to typed "day" DATE
    df = df.withColumn("day", F.to_date("date"))

    # Cast KPIs with safe nulls if missing
    def cast_nullable(colname, dtype):
        return (
            F.col(colname).cast(dtype)
            if colname in df.columns
            else F.lit(None).cast(dtype)
        )

    df = _with_media_from_path(df)

    # If 'day' already exists, drop it to avoid ambiguity.
    if "day" in df.columns:
        df = df.drop("day")

    # Build typed 'day' from source 'date'
    df = df.withColumn("day", cast_nullable("date", DateType()))

    # (Optional) drop the original 'date' if you don't want it downstream
    df = df.drop("date")
    df = (
        df.withColumn("load_count", cast_nullable("load_count", LongType()))
        .withColumn("play_count", cast_nullable("play_count", LongType()))
        .withColumn("play_rate", cast_nullable("play_rate", DoubleType()))
        .withColumn("hours_watched", cast_nullable("hours_watched", DoubleType()))
        .withColumn("engagement", cast_nullable("engagement", DoubleType()))
        .withColumn("visitors", cast_nullable("visitors", LongType()))
    )

    # Partition + metadata
    df = df.withColumn("dt", F.lit(day)).withColumn(
        "processed_at", F.current_timestamp()
    )

    # Keep only the curated columns + partitions to stabilize schema
    return df.select(
        "day",
        "load_count",
        "play_count",
        "play_rate",
        "hours_watched",
        "engagement",
        "visitors",
        "processed_at",
        "dt",
        "media_id",
    )


def _write(df: DataFrame, output_uri: str):
    dest = f"{output_uri.rstrip('/')}/media_by_date"
    # Ensure partition column type matches test expectation (STRING)
    df2 = df.withColumn("dt", F.col("dt").cast("string"))

    (
        df2.repartition("dt", "media_id")
        .write.mode("overwrite")
        .partitionBy("dt", "media_id")
        .parquet(dest)  # same as .format("parquet").save(dest)
    )


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Silver transform for Wistia media-by-date KPIs"
    )
    ap.add_argument(
        "--input-uri",
        required=True,
        help="s3://.../raw/wistia or file:///.../raw/wistia",
    )
    ap.add_argument(
        "--output-uri",
        required=True,
        help="s3://.../silver/wistia or file:///.../silver/wistia",
    )
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    args = ap.parse_args(argv or sys.argv[1:])

    spark = _build_spark()
    src = _input_glob(args.input_uri, args.day)
    try:
        df = spark.read.json(src)
    except Exception:
        print(f"[silver_media_by_date] No input found for day={args.day} at {src}")
        return

    if df.rdd.isEmpty():
        print(f"[silver_media_by_date] No input found for day={args.day} at {src}")
        return

    out = _transform(df, day=args.day)
    _write(out, args.output_uri)


if __name__ == "__main__":
    main()
