"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# Output columns for metadata, lineage, and quality checks.
# Raw TLC columns vary by taxi type; we normalize pickup/dropoff to a single name and add taxi_type, extracted_at.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: pickup_datetime
    type: timestamp
    description: Trip start time (normalized from tpep_pickup_datetime or lpep_pickup_datetime)
  - name: dropoff_datetime
    type: timestamp
    description: Trip end time (normalized from tpep_dropoff_datetime or lpep_dropoff_datetime)
  - name: taxi_type
    type: string
    description: Source taxi type (yellow or green)
  - name: extracted_at
    type: timestamp
    description: When this row was extracted (for lineage/debugging)
  - name: passenger_count
    type: float
    description: Number of passengers
  - name: trip_distance
    type: float
    description: Trip distance in miles
  - name: fare_amount
    type: float
    description: Fare amount in USD
  - name: total_amount
    type: float
    description: Total amount in USD
  - name: payment_type
    type: integer
    description: Payment type (1=credit, 2=cash, etc.)
  - name: PULocationID
    type: integer
    description: Pick-up location ID
  - name: DOLocationID
    type: integer
    description: Drop-off location ID

@bruin"""

import os
import json
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pandas as pd

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def _parse_bruin_dates():
    """Read BRUIN_START_DATE and BRUIN_END_DATE (YYYY-MM-DD)."""
    start = os.environ.get("BRUIN_START_DATE")
    end = os.environ.get("BRUIN_END_DATE")
    if not start or not end:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")
    return datetime.strptime(start, "%Y-%m-%d"), datetime.strptime(end, "%Y-%m-%d")


def _get_taxi_types():
    """Read taxi_types from BRUIN_VARS (JSON). Defaults to ['yellow']."""
    raw = os.environ.get("BRUIN_VARS", "{}")
    try:
        vars_ = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        vars_ = {}
    return vars_.get("taxi_types", ["yellow"])


def _normalize_trip_df(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    """Normalize yellow (tpep_*) / green (lpep_*) datetime columns to pickup_datetime, dropoff_datetime."""
    if taxi_type == "yellow":
        if "tpep_pickup_datetime" in df.columns:
            df = df.rename(columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
            })
    else:
        if "lpep_pickup_datetime" in df.columns:
            df = df.rename(columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
            })
    return df


def materialize():
    """
    Ingest NYC TLC trip data from public Parquet files.
    Uses BRUIN_START_DATE, BRUIN_END_DATE for the date range and BRUIN_VARS (taxi_types) for which types to fetch.
    Keeps data in raw form; adds taxi_type and extracted_at for lineage.
    """
    start_dt, end_dt = _parse_bruin_dates()
    taxi_types = _get_taxi_types()
    extracted_at = datetime.now(timezone.utc)

    frames = []
    current = start_dt
    while current <= end_dt:
        year, month = current.year, current.month
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}{filename}"
            try:
                df = pd.read_parquet(url)
            except Exception as e:
                # Skip missing or invalid files (e.g. future months, unavailable data)
                print(f"Skipping {url}: {e}")
                continue
            df = _normalize_trip_df(df, taxi_type)
            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            frames.append(df)
        current += relativedelta(months=1)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
