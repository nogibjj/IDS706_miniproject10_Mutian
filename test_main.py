from main import spark_session

from pyspark.sql import SparkSession


def test_initialize_spark():
    spark = spark_session()
    assert isinstance(spark, SparkSession)
