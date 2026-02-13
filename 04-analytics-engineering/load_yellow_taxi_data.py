# data upload is based on https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/setup/cloud_setup.md

import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# Change this to your bucket name
# BUCKET_NAME = "dezoomcamp_hw3_2025"
BUCKET_NAME = "zoomcamp_bucket_kestra-sandbox-8656"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "gcs.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
# client = storage.Client(project='datatalks-484013')

# BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
# BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
YEARS = [2019, 2020]
# Updated to handle years and months correctly in the loop
DOWNLOAD_DIR = "./data"

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(year, month):
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{TYPE}/{TYPE}_tripdata_{year}-{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, f"{TYPE}_tripdata_{year}-{month}.csv.gz")
    # print(url, file_path)
    # print(file_path)
    
    try:
        # print(f"Downloading {url} to {file_path}...")
        urllib.request.urlretrieve(url, file_path)
        # print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    # create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            # print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            # print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    TYPE = "green"
    for year in YEARS:
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Create a list of (year, month) tuples for the executor
            args = [(year, month) for month in MONTHS]
            file_paths = list(executor.map(lambda p: download_file(*p), args))
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    TYPE = "yellow"
    for year in YEARS:
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Create a list of (year, month) tuples for the executor
            args = [(year, month) for month in MONTHS]
            file_paths = list(executor.map(lambda p: download_file(*p), args))
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")
