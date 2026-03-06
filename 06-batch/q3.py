
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('question3') \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Filter for trips starting on 2025-11-15
# The column is tpep_pickup_datetime
count = df.filter(F.to_date(df.tpep_pickup_datetime) == "2025-11-15").count()

print(f"Number of taxi trips on November 15th: {count}")

spark.stop()
