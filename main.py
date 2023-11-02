from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg
import sys
from io import StringIO
import contextlib


@contextlib.contextmanager
def capture_output():
    new_stdout, new_stderr = StringIO(), StringIO()
    old_stdout, old_stderr = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_stdout, new_stderr
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr


def save_markdown(output, filename="output.md"):
    with open(filename, "w") as f:
        f.write("# PySpark Output\n\n")
        f.write("```\n")
        f.write(output)
        f.write("\n```\n")


def spark_session():
    spark = SparkSession.builder.appName("project10").master("local[*]").getOrCreate()
    return spark


def load_dataset(spark, file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


def data_transformation(df):
    # Use withColumn to create a new column 'buyoversell'
    df = df.withColumn("buyoversell", when(col("buy_sell_ratio") > 1, 1).otherwise(0))
    res = df.groupBy("buyoversell").agg(
        avg("buy_sell_ratio").alias("avg_buy_sell_ratio")
    )
    return res


def spark_query(spark, df):
    df.createOrReplaceTempView("stock")
    res = spark.sql(
        """
        SELECT * 
        FROM stock
        WHERE buy_sell_ratio > 2
        """
    )
    return res


def main():
    spark = spark_session()
    df = load_dataset(spark, "./taker_buy_sell_volume.csv")
    res = data_transformation(df)
    query_res = spark_query(spark, df)

    with capture_output() as (out, _):
        # show the dataframe schema
        df.printSchema()
        # show the dataframe
        df.show()

        # show the average buy sell ratio with buyoversell ?>1
        res.show()
        # show the query result
        query_res.show()

    save_markdown(out.getvalue())
    spark.stop()


if __name__ == "__main__":
    main()
