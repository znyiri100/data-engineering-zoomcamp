"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

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


def _get_cache_dir():
    """Get the path to the data cache directory."""
    # Place it in a 'data' folder next to the pipeline.yml
    return os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")


def _get_taxi_vars():
    """Read taxi_types and source_data from BRUIN_VARS (JSON)."""
    raw = os.environ.get("BRUIN_VARS", "{}")
    try:
        vars_ = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        vars_ = {}
    return vars_.get("taxi_types", ["yellow"]), vars_.get("source_data", "NYC")


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
    Ingest NYC TLC trip data from public Parquet files or DataTalksClub GitHub.
    Uses BRUIN_START_DATE, BRUIN_END_DATE for the date range and BRUIN_VARS (taxi_types, source_data).
    Keeps data in raw form; adds taxi_type and extracted_at for lineage.
    """
    start_dt, end_dt = _parse_bruin_dates()
    taxi_types, source_data = _get_taxi_vars()
    extracted_at = datetime.now(timezone.utc)

    frames = []
    current = start_dt
    while current <= end_dt:
        year, month = current.year, current.month
        for taxi_type in taxi_types:
            if source_data == "DataTalksClub":
                # Github format: green_tripdata_2019-01.csv.gz
                filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
                url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{filename}"
            else:
                # NYC TLC format: green_tripdata_2022-01.parquet
                filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
                url = f"{BASE_URL}{filename}"

            local_path = os.path.join(_get_cache_dir(), filename)

            if os.path.exists(local_path):
                print(f"Using cached file: {local_path}")
                try:
                    if filename.endswith(".parquet"):
                        df = pd.read_parquet(local_path)
                    else:
                        df = pd.read_csv(local_path, compression='gzip')
                except Exception as e:
                    print(f"Error reading cached file {local_path}, re-downloading: {e}")
                    df = None
            else:
                df = None

            if df is None:
                print(f"Downloading {url}...")
                try:
                    if url.endswith(".parquet"):
                        df = pd.read_parquet(url)
                    else:
                        df = pd.read_csv(url, compression='gzip')
                    
                    # Save to cache
                    os.makedirs(_get_cache_dir(), exist_ok=True)
                    if filename.endswith(".parquet"):
                        df.to_parquet(local_path)
                    else:
                        df.to_csv(local_path, index=False, compression='gzip')
                    print(f"Saved to cache: {local_path}")
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
