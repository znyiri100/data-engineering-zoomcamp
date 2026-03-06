
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('question6') \
    .getOrCreate()

# Read the trip data
df_trips = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Read the zone lookup data
df_zones = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)

# Join the data
# Yellow data pickup location column: PULocationID
# Zone data location ID column: LocationID
df_joined = df_trips.join(df_zones, df_trips.PULocationID == df_zones.LocationID)

# Group by Zone and count
df_result = df_joined.groupBy("Zone").count().orderBy("count", ascending=True)

# Show the top result (least frequent)
df_result.show(5, truncate=False)

spark.stop()
