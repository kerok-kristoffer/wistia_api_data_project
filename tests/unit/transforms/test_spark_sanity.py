"""Sanity tests for PySpark functionality."""

from transforms import _ping


def test_spark_creates_df(spark):
    assert _ping() == "ok"  # remove when actual code is tested in transform
    """Sanity check that Spark can create a small DataFrame."""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    assert df.count() == 2
    spark.stop()
