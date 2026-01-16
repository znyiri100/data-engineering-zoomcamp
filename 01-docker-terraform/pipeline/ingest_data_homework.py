#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--table', default='green_tripdata', help='Target table name', show_default=True)
@click.option('--year', default=2025, help='Year of the data', show_default=True)
@click.option('--month', default=11, help='Month of the data', show_default=True)
@click.option('--user', default='postgres', help='Database user', show_default=True)
@click.option('--password', default='postgres', help='Database password', show_default=True)
@click.option('--host', default='localhost', help='Database host', show_default=True)
@click.option('--port', default=5433, help='Database port', show_default=True)
@click.option('--db', default='ny_taxi', help='Database name', show_default=True)
@click.option('--chunksize', default=10000, help='Size of data chunks', show_default=True)

def run(table, year, month, user, password, host, port, db, chunksize):

    engine = create_engine( f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print("Connection to the database established")

    table_name = f"{table}_{year}-{month:02d}"

    # we are hardcoding zones ingestion here
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    df_zones = pd.read_csv(url)
    df_zones.to_sql(name='zones', con=engine, if_exists='replace')
    print("Zones table created")

    # taxi data ingestion
    import pyarrow.parquet as pq
    import pyarrow as pa
    import requests
    import os

    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{table_name}.parquet'
    local_file = f'{table_name}.parquet'

    # Download the file if not already present
    if not os.path.exists(local_file):
        print(f"Downloading {url} ...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded to {local_file}")
    else:
        print(f"File {local_file} already exists. Skipping download.")

    parquet_file = pq.ParquetFile(local_file)

    first = True
    for batch in tqdm(parquet_file.iter_batches(batch_size=chunksize)):
        df_chunk = pa.Table.from_batches([batch]).to_pandas()
        if first:
            df_chunk.head(0).to_sql(name=f"{table_name}", con=engine, if_exists='replace')
            first = False
            print(f"Table {table_name} created")
        df_chunk.to_sql(name=f"{table_name}", con=engine, if_exists="append")
        print("Inserted:", len(df_chunk))

if __name__ == '__main__':
    run()