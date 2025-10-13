import pyspark
from pyspark.sql import SparkSession

def test_spark_creates_df():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    assert df.count() == 2
    spark.stop()