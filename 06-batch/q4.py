
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('question4') \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Calculate duration in hours
# (dropoff - pickup) in seconds / 3600
# Since they are timestamp types, subtracting them gives an interval or seconds depending on Spark version
# In Spark 3+, subtracting two timestamps returns an interval. 
# We can use unix_timestamp or cast to long to get seconds.

df_duration = df.withColumn("duration_hours", 
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
)

max_duration = df_duration.select(F.max("duration_hours")).collect()[0][0]

print(f"Longest trip duration in hours: {max_duration}")

spark.stop()
