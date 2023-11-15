import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count


def func(data_path: str):
    spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()
    print(data_path)
    mnm_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(data_path)
    )

    count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )

    count_mnm_df.show(n=60, truncate=False)
    print(f"Total Rows = {count_mnm_df.count()}")

    ca_count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=True)
    )

    ca_count_mnm_df.show(n=10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    print(f"Usage: mnmncount {sys.stderr}")
    mnm_file = sys.argv[1]
    print(mnm_file)
    func(mnm_file)
