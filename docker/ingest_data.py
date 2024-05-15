#!/usr/bin/env python
# coding: utf-8

from sqlalchemy import create_engine
import os
# import requests
import pandas as pd
from time import time
import pyarrow.parquet as pq
import argparse

def main (params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    pq_name = 'output.parquet'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    destination = "data/yellow_tripdata_2024-01.parquet"
    os.system(f'wget {url} -o {destination}')
    parquet_file = pq.ParquetFile(destination)
    print(parquet_file.num_row_groups)
    num_row_groups = parquet_file.num_row_groups
    chunk_size = 100000

    start_row = 0
    while start_row < num_row_groups:
        end_row = min(start_row + chunk_size, num_row_groups)
        df_iter = pd.concat([parquet_file.read_row_group(i).to_pandas() for i in range(start_row, end_row)])
        df_iter['tpep_pickup_datetime'] = pd.to_datetime(df_iter['tpep_pickup_datetime'])
        df_iter['tpep_dropoff_datetime'] = pd.to_datetime(df_iter['tpep_dropoff_datetime'])
        start = time()
        df_iter.to_sql(name={table_name}, con=engine, if_exists='append')
        end = time()
        duration = end - start
        print('Chunk from %d to %d took %.3f seconds to run.' % (start_row, end_row, duration))
        # Update start row for the next chunk
        start_row = end_row

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgress.')
    parser.add_argument('--user', help='user name for postgress')
    parser.add_argument('--password', help='password for postgress')
    parser.add_argument('--host', help='host for postgress')
    parser.add_argument('--port', help='port for postgress')
    parser.add_argument('--db', help='db name for postgress')
    parser.add_argument('--table_name', help='name of results table')
    parser.add_argument('--url', help='url of the CSV file')

    args = parser.parse_args()
    main(args)