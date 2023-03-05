import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True, retries=3)  #,cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)) # use cache diligently
def extract_data(url):

    # inserting data, here first 100 rows
    df = pd.read_csv(url,compression='gzip',nrows=1000)
    print(len(df))
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df

@task(log_prints=True, retries=3)
def transform_data(df):
    # remove rides where passenger count is 0
    print("total rows before removing passenger count 0: ", df.shape[0])
    df = df[df.passenger_count > 0]
    print("total rows after removing passenger count 0: ", df.shape[0])
    return df

@task(log_prints=True, retries=3)
def ingest_data(user,password,host,port,db,table,df):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')  # postgresql://user:pass@host:port/db
    df.to_sql(name=f'{table}',con=engine,if_exists='append')
    print(f'Inserted another chunk...')

@flow(name="logging_subflow",log_prints=True)
def example_subflow():
    print("I'm a subflow!") 

@flow(name="ingest_flow")
def main_flow(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url
    example_subflow()
    extract_task = extract_data(url)
    transform_task = transform_data(extract_task)
    ingest_task = ingest_data(user,password,host,port,db,table,transform_task)


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
    main_flow(args)

