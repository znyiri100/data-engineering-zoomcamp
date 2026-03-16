# Homework

In this homework, we'll practice streaming with Kafka (Redpanda) and PyFlink.

We use Redpanda, a drop-in replacement for Kafka. It implements the same
protocol, so any Kafka client library works with it unchanged.

For this homework we will be using Green Taxi Trip data from October 2025:

- [green_tripdata_2025-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet)


## Setup

We'll use the same infrastructure from the [workshop](../../../07-streaming/workshop/).

Follow the setup instructions: build the Docker image, start the services:

```bash
cd 07-streaming/workshop/
docker compose build
docker compose up -d
```

This gives us:

- Redpanda (Kafka-compatible broker) on `localhost:9092`
- Flink Job Manager at http://localhost:8081
- Flink Task Manager
- PostgreSQL on `localhost:5432` (user: `postgres`, password: `postgres`)

If you previously ran the workshop and have old containers/volumes,
do a clean start:

```bash
docker compose down -v
docker compose build
docker compose up -d
```

Note: the container names (like `workshop-redpanda-1`) assume the
directory is called `workshop`. If you renamed it, adjust accordingly.

```
(streaming) me@me:/home/me/tmp/DataTalks/znyiri100/data-engineering-zoomcamp/workshop/streaming
> docker ps
CONTAINER ID   IMAGE                              COMMAND                  CREATED        STATUS        PORTS                                                                                                                                                                                                            NAMES
95ccb4a60414   pyflink-workshop                   "/docker-entrypoint.…"   8 hours ago    Up 8 hours    6121-6123/tcp, 8081/tcp                                                                                                                                                                                          streaming-taskmanager-1
24573a41757e   pyflink-workshop                   "/docker-entrypoint.…"   8 hours ago    Up 8 hours    6123/tcp, 0.0.0.0:8081->8081/tcp, [::]:8081->8081/tcp                                                                                                                                                            streaming-jobmanager-1
7ae71d4e2874   postgres:18                        "docker-entrypoint.s…"   8 hours ago    Up 8 hours    0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp                                                                                                                                                                      streaming-postgres-1
d9891a2c4d1c   redpandadata/redpanda:v25.3.9      "/entrypoint.sh redp…"   8 hours ago    Up 8 hours    0.0.0.0:8082->8082/tcp, [::]:8082->8082/tcp, 8081/tcp, 0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp, 0.0.0.0:28082->28082/tcp, [::]:28082->28082/tcp, 9644/tcp, 0.0.0.0:29092->29092/tcp, [::]:29092->29092/tcp   streaming-redpanda-1
```

## Question 1. Redpanda version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

```bash
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
```

## Question 2. Sending data to Redpanda

Create a topic called `green-trips`:

```bash
docker exec -it streaming-redpanda-1 rpk topic create green-trips

> docker exec -it streaming-redpanda-1 rpk topic create green-trips
TOPIC        STATUS
green-trips  OK

> docker exec -it streaming-redpanda-1 rpk topic list
NAME         PARTITIONS  REPLICAS
green-trips  1           1
rides        1           1
```

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

Convert each row to a dictionary and send it to the `green-trips` topic.
You'll need to handle the datetime columns - convert them to strings
before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:

```python
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?

- 10 seconds
- 60 seconds
- 120 seconds
- 300 seconds

```prompt
Used Claude Sonnet 4.6 (Thinking) to generate the producer_homework.py file:

create a new version of producer.py called producer_homework.py

this producer will send the green taxi data to green-trips topic.

Read the parquet file and keep only these columns:
- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

Convert each row to a dictionary and send it to the `green-trips` topic.
You'll need to handle the datetime columns - convert them to strings
before serializing to JSON.
Measure the time it takes to send the entire dataset and flush, simialrly as in producer.py
```

```bash
/home/me/tmp/DataTalks/znyiri100/data-engineering-zoomcamp/workshop/streaming
> uv run python src/producers/producer_homework.py 
Reading parquet file...
Loaded 49,416 rows
took 16.89 seconds

> docker exec -it streaming-redpanda-1 rpk topic describe green-trips
SUMMARY
=======
NAME        green-trips
PARTITIONS  1
REPLICAS    1

CONFIGS
=======
KEY                                   VALUE          SOURCE
cleanup.policy                        delete         DEFAULT_CONFIG
compression.type                      producer       DEFAULT_CONFIG
delete.retention.ms                   -1             DEFAULT_CONFIG
flush.bytes                           262144         DEFAULT_CONFIG
flush.ms                              100            DEFAULT_CONFIG
initial.retention.local.target.bytes  -1             DEFAULT_CONFIG
initial.retention.local.target.ms     -1             DEFAULT_CONFIG
max.compaction.lag.ms                 9223372036854  DEFAULT_CONFIG
max.message.bytes                     1048576        DEFAULT_CONFIG
message.timestamp.after.max.ms        3600000        DEFAULT_CONFIG
message.timestamp.before.max.ms       9223372036854  DEFAULT_CONFIG
message.timestamp.type                CreateTime     DEFAULT_CONFIG
min.cleanable.dirty.ratio             0.2            DEFAULT_CONFIG
min.compaction.lag.ms                 0              DEFAULT_CONFIG
redpanda.iceberg.mode                 disabled       DEFAULT_CONFIG
redpanda.leaders.preference           none           DEFAULT_CONFIG
redpanda.remote.allowgaps             false          DEFAULT_CONFIG
redpanda.remote.delete                true           DEFAULT_CONFIG
redpanda.remote.read                  false          DEFAULT_CONFIG
redpanda.remote.write                 false          DEFAULT_CONFIG
retention.bytes                       -1             DEFAULT_CONFIG
retention.local.target.bytes          -1             DEFAULT_CONFIG
retention.local.target.ms             86400000       DEFAULT_CONFIG
retention.ms                          604800000      DEFAULT_CONFIG
segment.bytes                         134217728      DEFAULT_CONFIG
segment.ms                            1209600000     DEFAULT_CONFIG
write.caching                         true           DEFAULT_CONFIG

> docker exec -it streaming-redpanda-1 rpk topic consume green-trips --num 1 --offsestart
{
  "topic": "green-trips",
  "value": "{\"lpep_pickup_datetime\": \"2025-10-01 00:21:47\", \"lpep_dropoff_datetime\": \"2025-10-01 00:24:37\", \"PULocationID\": 247, \"DOLocationID\": 69, \"passenger_count\": 1.0, \"trip_distance\": 0.7, \"tip_amount\": 1.7, \"total_amount\": 10.0}",
  "timestamp": 1773522471006,
  "partition": 0,
  "offset": 0
}
```

## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the `green-trips` topic
(set `auto_offset_reset='earliest'`).

Count how many trips have a `trip_distance` greater than 5.0 kilometers.

How many trips have `trip_distance` > 5?

- 6506
- 7506
- 8506
- 9506

```bash
How would you write a Kafka consumer consumer_homework.py that reads all messages from the `green-trips` topic
(set `auto_offset_reset='earliest'`).
Count how many trips have a `trip_distance` greater than 5.0 kilometers.
How many trips have `trip_distance` > 5?

> uv run python src/consumers/consumer_homework.py 
Reading messages from topic: green-trips
(set auto_offset_reset='earliest' – consuming all historical records)

Total messages consumed  : 49,416
Trips with distance > 5.0: 8,506
```

```bash
make another version of the same question, consumer_postgres_homework.py
this will insert consumed data in the database

> uv run python src/consumers/consumer_postgres_homework.py
...
  … inserted 47,000 rows so far
  … inserted 47,500 rows so far
  … inserted 48,000 rows so far
  … inserted 48,500 rows so far
  … inserted 49,000 rows so far

Done.
Total messages inserted   : 49,416
Trips with distance > 5.0 : 8,506

postgres=# SELECT COUNT(*) FROM green_trips WHERE trip_distance > 5.0;
  8506
```

## Part 2: PyFlink (Questions 4-6)

For the PyFlink questions, you'll adapt the workshop code to work with
the green taxi data. The key differences from the workshop:

- Topic name: `green-trips` (instead of `rides`)
- Datetime columns use `lpep_` prefix (instead of `tpep_`)
- You'll need to handle timestamps as strings (not epoch milliseconds)

You can convert string timestamps to Flink timestamps in your source DDL:

```sql
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Before running the Flink jobs, create the necessary PostgreSQL tables
for your results.

Important notes for the Flink jobs:

- Place your job files in `workshop/src/job/` - this directory is
  mounted into the Flink containers at `/opt/src/job/`
- Submit jobs with:
  `docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/your_job.py`
- The `green-trips` topic has 1 partition, so set parallelism to 1
  in your Flink jobs (`env.set_parallelism(1)`). With higher parallelism,
  idle consumer subtasks prevent the watermark from advancing.
- Flink streaming jobs run continuously. Let the job run for a minute
  or two until results appear in PostgreSQL, then query the results.
  You can cancel the job from the Flink UI at http://localhost:8081
- If you sent data to the topic multiple times, delete and recreate
  the topic to avoid duplicates:
  `docker exec -it workshop-redpanda-1 rpk topic delete green-trips`


## Question 4. Tumbling window - pickup location

Create a Flink job that reads from `green-trips` and uses a 5-minute
tumbling window to count trips per `PULocationID`.

Write the results to a PostgreSQL table with columns:
`window_start`, `PULocationID`, `num_trips`.

After the job processes all data, query the results:

```sql
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which `PULocationID` had the most trips in a single 5-minute window?

- 42
- 74
- 75
- 166

```bash
# create table
docker exec -i streaming-postgres-1 psql -U postgres -d postgres << 'EOF'
DROP TABLE IF EXISTS green_trips_tumbling_5min;
CREATE TABLE green_trips_tumbling_5min (
    window_start TIMESTAMP NOT NULL,
    pulocationid INT NOT NULL,
    num_trips BIGINT,
    PRIMARY KEY (window_start, pulocationid)
);
EOF

# recreate topic
docker exec -it streaming-redpanda-1 rpk topic delete green-trips
docker exec -it streaming-redpanda-1 rpk topic create green-trips
docker exec -it streaming-redpanda-1 rpk topic list

# submit flink job
docker exec -it streaming-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_window_job.py

# produce data
uv run python src/producers/producer_homework.py

# watch table
watch -n 1 'PGPASSWORD=postgres docker compose exec postgres psql -U postgres -d postgres -c "SELECT pulocationid, num_trips FROM green_trips_tumbling_5min ORDER BY num_trips DESC LIMIT 3;"'

 pulocationid | num_trips
--------------+-----------
           74 |        15
           74 |        14
           74 |        13
```

```bash
Troubleshooting:

# task manager crashed due to table was create case sensitive with uppercase letters and flink job was using lowercase letters
# restart task manager
docker compose up -d taskmanager
docker ps --format "table {{.Names}}\t{{.Status}}" | grep streaming

# The parquet data has NaN values for passenger_count which JSON represents literally as NaN — but Flink's JSON parser rejects it.
# Fixing and Running Producer to solve NaN issue
# I've updated producer_homework.py to handle NaN by converting it to None (which results in null in JSON). Now I am deleting and recreating the green-trips topic to remove broken records, then re-producing the complete dataset. Once the topic is clean and populated with valid JSON, I will resubmit the Flink job.
```

## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap
on `PULocationID`, using `lpep_pickup_datetime` as the event time
with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other.
When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the `PULocationID`
with the longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- 31
- 51
- 81

```bash
# peer help from https://datatalks-club.slack.com/archives/C01FABYF2RG/p1773335702069479
# adtnl prompt: should we Partition by PULocationID in the SESSION() clause.  Without partition by PULocationID,  flink will combine session from all locations, and it'll be super long. You will have hunddres of number of trips in your aggregate table.

# new table
PGPASSWORD=postgres psql -h localhost -U postgres -d postgres -c "CREATE TABLE IF NOT EXISTS green_trips_session_5min (window_start TIMESTAMP, window_end TIMESTAMP, pulocationid INT, num_trips BIGINT, PRIMARY KEY (window_start, window_end, pulocationid));"

# run new job
docker compose exec jobmanager flink run -py /opt/src/job/q5_session_window_job.py
docker compose logs jobmanager --tail 50

# watch table
watch -n 1 'PGPASSWORD=postgres docker compose exec postgres psql -U postgres -d postgres -c "SELECT pulocationid, num_trips FROM green_trips_session_5min ORDER BY num_trips DESC LIMIT 3;"'

 pulocationid | num_trips
--------------+-----------
           74 |        81
           74 |        72
           74 |        71           
```

## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the
total `tip_amount` per hour (across all locations).

Which hour had the highest total tip amount?

- 2025-10-01 18:00:00
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

```bash
# create new table for Q6
docker exec -i streaming-postgres-1 psql -U postgres -d postgres << 'EOF'
DROP TABLE IF EXISTS green_trips_tips_per_hour;
CREATE TABLE green_trips_tips_per_hour (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    total_tips NUMERIC,
    PRIMARY KEY (window_start, window_end)
);
EOF

# reset topic
docker exec -it streaming-redpanda-1 rpk topic delete green-trips
docker exec -it streaming-redpanda-1 rpk topic create green-trips
docker exec -it streaming-redpanda-1 rpk topic list

# create job
docker exec -d streaming-jobmanager-1 flink run -py /opt/src/job/q6_tumbling_window_tips_job.py

# produce data
uv run python src/producers/producer_homework.py

# watch table
watch -n 1 'PGPASSWORD=postgres docker compose exec postgres psql -U postgres -d postgres -c "SELECT window_start, total_tips FROM green_trips_tips_per_hour ORDER BY total_tips DESC LIMIT 3;"'
    window_start     | total_tips
---------------------+------------
 2025-10-30 16:00:00 |     494.41
 2025-10-09 18:00:00 |     472.01
 2025-10-10 17:00:00 |     470.08
```

```bash
# troubleshooting errors: taskmanager-1  | Caused by: org.postgresql.util.PSQLException: ERROR: column "num_trips" of relation "green_trips_tips_per_hour" does not exist
# root cause: your current q6_tumbling_window_tips_job.py includes num_trips in sink schema/query, but green_trips_tips_per_hour table doesn’t have that column, so JDBC sink fails and the TaskManager exits. I’ll patch Q6 to match the homework table (window_start, window_end, total_tips) now.
# solution: update job script to exclude num_trips from the sink table schema, restart taskmanager, restart job

docker compose up -d taskmanager
docker compose exec -T jobmanager flink list
docker compose ps taskmanager

docker compose exec -T jobmanager flink cancel d87f8e8916b8577bb579936c97afca2d
docker exec -d streaming-jobmanager-1 flink run -py /opt/src/job/q6_tumbling_window_tips_job.py

```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw7

