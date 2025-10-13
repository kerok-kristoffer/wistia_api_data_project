"""Sanity tests for PySpark functionality."""

from pyspark.sql import SparkSession


def test_spark_creates_df():
    """Sanity check that Spark can create a small DataFrame."""
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    assert df.count() == 2
    spark.stop()
