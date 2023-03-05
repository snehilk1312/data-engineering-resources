from pathlib import Path
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from prefect import task, flow
from datetime import timedelta
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse

@task()
def extract_from_gcs(color, year, month):
    """download data to local from gcs bucket"""
    path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.download_object_to_path(
        path,path
    )

    return path

@task()
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f'Before transformation, no. of missing passenger:  {df.passenger_count.isna().sum()}')
    df.passenger_count.fillna(0, inplace=True)
    print(f'After transformation, no. of missing passenger:  {df.passenger_count.isna().sum()}')
    return df

@task()
def write_to_bq(df: pd.DataFrame) -> None:
    """write the transformed data to bigquery warehouse"""
    gcp_credentials_block = GcpCredentials.load("development-credentials")
    df.to_gbq(
        destination_table="bqdataset.yellow_taxi_data",
        #project_id=gcp_credentials_block.project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=100000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    """main flow for gcs bucket to bq warehouse"""
    color = "yellow"
    year = 2020
    month = 3

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_task = write_to_bq(df)

if __name__=="__main__":
    etl_gcs_to_bq()