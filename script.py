from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main(): 
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("csv").option('header', 'True').load("data/cars.csv")
    
    output = (
        df
        .groupBy("manufacturer_name")
        .agg(
            F.count("manufacturer_name").alias("count"), 
            F.round(F.avg("year_produced")).cast('int').alias("avg_year"), 
            F.min(F.col("price_usd").cast('float')).alias("min_price"), 
            F.max(F.col("price_usd").cast('float')).alias("max_price")
        )
    )

    output.show(5)
    output.coalesce(1).write.mode("overwrite").format('json').save('data/result.json')
main()