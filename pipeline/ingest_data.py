#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def run():

    target_table = 'yellow_taxi_data'
    year = 2021
    month = 1
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz'

    username = 'root'
    password = 'root'
    host = 'localhost'
    port = 5432
    db = 'ny_taxi'

    engine = create_engine( f'postgresql://{username}:{password}@{host}:{port}/{db}')

    first = True

    df_iter = pd.read_csv(
        url,
        iterator=True,
        chunksize=100000
    )

    # for df_chunk in df_iter:
    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql( name=target_table, con=engine, if_exists="append")

        print("Inserted:", len(df_chunk))

if __name__ == '__main__':
    run()