from main import *


def test_initialize_spark():
    spark = spark_session()
    assert isinstance(spark, SparkSession)
