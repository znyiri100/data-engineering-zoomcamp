#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

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

@click.command()
@click.option('--table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, help='Year of the data')
@click.option('--month', default=1, help='Month of the data')
@click.option('--user', default='root', help='Database user')
@click.option('--password', default='root', help='Database password')
@click.option('--host', default='localhost', help='Database host')
@click.option('--port', default=5432, help='Database port')
@click.option('--db', default='ny_taxi', help='Database name')
@click.option('--chunksize', default=100000, help='Size of data chunks')

def run(table, year, month, user, password, host, port, db, chunksize):

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz'


    engine = create_engine( f'postgresql://{user}:{password}@{host}:{port}/{db}')

    first = True

    df_iter = pd.read_csv(
        url,
        iterator=True,
        chunksize=chunksize
    )

    # for df_chunk in df_iter:
    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(n=0).to_sql(name=table, con=engine, if_exists='replace')
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql( name=table, con=engine, if_exists="append")

        print("Inserted:", len(df_chunk))

if __name__ == '__main__':
    run()