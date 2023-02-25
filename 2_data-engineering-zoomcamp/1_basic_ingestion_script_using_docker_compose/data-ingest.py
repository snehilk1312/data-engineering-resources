import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url
    csv_name = "output.csv"

    # download the csv
    os.system(f"wget {url} -O {csv_name}.gz")
    os.system(f"gzip -d {csv_name}.gz")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')  # postgresql://user:pass@host:port/db
    # inserting data, here first 100k rows
    df_iter = pd.read_csv(csv_name, iterator=True,chunksize=100000)
    is_done = 0
    while True and is_done==0:
        try:
            time_s = time()
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=f'{table}',con=engine,if_exists='append')
            time_e = time()
            print(f'Inserted another chunk...time taken...{time_e-time_s}')
        except StopIteration:
            print('All data inserted into db')
            is_done = 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest NYC taxi data to postgres")
    # arguments - user, password, host, port, db name, table name, url of csv

    parser.add_argument("--user", help="user for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="data base name")
    parser.add_argument("--table", help="table name")
    parser.add_argument("--url", help="url of csv")
    args = parser.parse_args()
    main(args)

