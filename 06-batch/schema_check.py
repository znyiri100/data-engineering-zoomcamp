
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('schema_check') \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
df.printSchema()

spark.stop()
