import json
import sys
from pathlib import Path
from time import time

import pandas as pd
from kafka import KafkaProducer

# Allow imports from the src/ directory (e.g. models.py if needed later)
sys.path.insert(0, str(Path(__file__).parent.parent))

# ---------------------------------------------------------------------------
# Data ingestion
# ---------------------------------------------------------------------------
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount',
]

print("Reading parquet file...")
df = pd.read_parquet(url, columns=columns)
print(f"Loaded {len(df):,} rows")

# ---------------------------------------------------------------------------
# Serializer
# Datetime columns must be converted to strings before JSON serialisation.
# ---------------------------------------------------------------------------
def row_serializer(row_dict):
    # Convert any Timestamp values to ISO-8601 strings
    # and replace NaN values with None for valid JSON serialization
    for key, value in row_dict.items():
        if hasattr(value, 'isoformat'):          # covers pd.Timestamp / datetime
            row_dict[key] = value.isoformat(sep=' ')
        elif pd.isna(value):                     # covers NaN/NaT
            row_dict[key] = None
    return json.dumps(row_dict).encode('utf-8')


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------
server = 'localhost:9092'
topic_name = 'green-trips'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=row_serializer,
)

# ---------------------------------------------------------------------------
# Send all rows and measure time
# ---------------------------------------------------------------------------
t0 = time()

for _, row in df.iterrows():
    producer.send(topic_name, value=row.to_dict())

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
