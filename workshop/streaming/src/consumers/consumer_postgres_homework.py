import json

import psycopg2
from kafka import KafkaConsumer, TopicPartition

# ---------------------------------------------------------------------------
# PostgreSQL connection (matches docker-compose.yml credentials)
# ---------------------------------------------------------------------------
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='postgres',
    user='postgres',
    password='postgres',
)
cur = conn.cursor()

# Create table if it doesn't exist yet
cur.execute("""
    CREATE TABLE IF NOT EXISTS green_trips (
        id                    SERIAL PRIMARY KEY,
        lpep_pickup_datetime  TEXT,
        lpep_dropoff_datetime TEXT,
        "PULocationID"        INTEGER,
        "DOLocationID"        INTEGER,
        passenger_count       DOUBLE PRECISION,
        trip_distance         DOUBLE PRECISION,
        tip_amount            DOUBLE PRECISION,
        total_amount          DOUBLE PRECISION
    );
""")
conn.commit()

# ---------------------------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------------------------
server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',   # read from the very beginning
    enable_auto_commit=False,
    group_id=None,                  # no consumer group – read all messages
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

# Determine end offsets so we stop cleanly at the last known message
consumer.poll(timeout_ms=1000)
end_offsets = consumer.end_offsets(consumer.assignment())
consumer.seek_to_beginning()

# ---------------------------------------------------------------------------
# Consume, insert, and count
# ---------------------------------------------------------------------------
INSERT_SQL = """
    INSERT INTO green_trips (
        lpep_pickup_datetime, lpep_dropoff_datetime,
        "PULocationID", "DOLocationID",
        passenger_count, trip_distance, tip_amount, total_amount
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

total_count = 0
long_trip_count = 0
BATCH_SIZE = 500   # commit to Postgres every N rows for efficiency

print(f"Reading from topic '{topic_name}' and inserting into Postgres …\n")

for message in consumer:
    rec = message.value

    cur.execute(INSERT_SQL, (
        rec.get('lpep_pickup_datetime'),
        rec.get('lpep_dropoff_datetime'),
        rec.get('PULocationID'),
        rec.get('DOLocationID'),
        rec.get('passenger_count'),
        rec.get('trip_distance'),
        rec.get('tip_amount'),
        rec.get('total_amount'),
    ))

    total_count += 1

    trip_distance = rec.get('trip_distance') or 0.0
    if trip_distance > 5.0:
        long_trip_count += 1

    # Batch commit
    if total_count % BATCH_SIZE == 0:
        conn.commit()
        print(f"  … inserted {total_count:,} rows so far")

    # Stop once all partitions are fully consumed
    current_positions = {tp: consumer.position(tp) for tp in consumer.assignment()}
    if all(current_positions[tp] >= end_offsets[tp] for tp in end_offsets):
        break

# Final commit for any remaining rows
conn.commit()
consumer.close()
cur.close()
conn.close()

print(f"\nDone.")
print(f"Total messages inserted   : {total_count:,}")
print(f"Trips with distance > 5.0 : {long_trip_count:,}")
