# Module 6 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2025-11 data from the official website:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/setup/)

```bash
(06-batch) me@me:/home/me/tmp/DataTalks/znyiri100/data-engineering-zoomcamp/06-batch
> uv run python test_spark.py 2>/dev/null
Spark version: 4.1.1
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
+---+
```

## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

```bash
06-batch) me@me:/home/me/tmp/DataTalks/znyiri100/data-engineering-zoomcamp/06-batch
> uv run python q2.py 2>/dev/null
(06-batch) me@me:/home/me/tmp/DataTalks/znyiri100/data-engineering-zoomcamp/06-batch
> ls -lh yellow_2025_11_repartitioned
total 98M
-rw-r--r-- 1 me me   0 Mar  6 11:52 _SUCCESS
-rw-r--r-- 1 me me 25M Mar  6 11:52 part-00000-0ba5980f-7f8b-4a65-bd3c-6694d97a0316-c000.snappy.parquet
-rw-r--r-- 1 me me 25M Mar  6 11:52 part-00001-0ba5980f-7f8b-4a65-bd3c-6694d97a0316-c000.snappy.parquet
-rw-r--r-- 1 me me 25M Mar  6 11:52 part-00002-0ba5980f-7f8b-4a65-bd3c-6694d97a0316-c000.snappy.parquet
-rw-r--r-- 1 me me 25M Mar  6 11:52 part-00003-0ba5980f-7f8b-4a65-bd3c-6694d97a0316-c000.snappy.parquet
```

## Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

- 62,610
- 102,340
- 162,604
- 225,768

```bash
(06-batch) me@me:/home/me/tmp/DataTalks/znyiri100/data-engineering-zoomcamp/06-batch
> uv run python q3.py 2>/dev/null
Number of taxi trips on November 15th: 162604
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 22.7
- 58.2
- 90.6
- 134.5

```bash
(06-batch) me@me:/home/me/tmp/DataTalks/znyiri100/data-engineering-zoomcamp/06-batch
> uv run python q4.py 2>/dev/null
Longest trip duration in hours: 90.64666666666666
```

## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

If multiple answers are correct, select any

```bash
(06-batch) me@me:/home/me/tmp/DataTalks/znyiri100/data-engineering-zoomcamp/06-batch
> uv run python q6.py 2>/dev/null
+---------------------------------------------+-----+
|Zone                                         |count|
+---------------------------------------------+-----+
|Governor's Island/Ellis Island/Liberty Island|1    |
|Eltingville/Annadale/Prince's Bay            |1    |
|Arden Heights                                |1    |
|Port Richmond                                |3    |
|Rikers Island                                |4    |
+---------------------------------------------+-----+
```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw6
- Deadline: See the website


