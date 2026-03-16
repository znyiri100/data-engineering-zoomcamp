import json

from kafka import KafkaConsumer, TopicPartition

# ---------------------------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------------------------
server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',   # read from the very beginning of the topic
    enable_auto_commit=False,       # read-only consumer, no offsets to commit
    group_id=None,                  # no consumer group – read all messages
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

# ---------------------------------------------------------------------------
# Determine end offsets so we know when to stop
# ---------------------------------------------------------------------------
consumer.poll(timeout_ms=1000)          # trigger partition assignment
end_offsets = consumer.end_offsets(consumer.assignment())
consumer.seek_to_beginning()

# ---------------------------------------------------------------------------
# Count trips with trip_distance > 5.0
# ---------------------------------------------------------------------------
total_count = 0
long_trip_count = 0

print(f"Reading messages from topic: {topic_name}")
print("(set auto_offset_reset='earliest' – consuming all historical records)\n")

for message in consumer:
    record = message.value
    total_count += 1

    trip_distance = record.get('trip_distance') or 0.0
    if trip_distance > 5.0:
        long_trip_count += 1

    # Stop as soon as every partition is consumed up to its recorded end offset
    current_positions = {
        tp: consumer.position(tp)
        for tp in consumer.assignment()
    }
    if all(current_positions[tp] >= end_offsets[tp] for tp in end_offsets):
        break

consumer.close()

print(f"Total messages consumed  : {total_count:,}")
print(f"Trips with distance > 5.0: {long_trip_count:,}")
