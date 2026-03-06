import pyspark
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('question2') \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Repartition and save
output_path = "yellow_2025_11_repartitioned"
df.repartition(4).write.mode("overwrite").parquet(output_path)

spark.stop()
