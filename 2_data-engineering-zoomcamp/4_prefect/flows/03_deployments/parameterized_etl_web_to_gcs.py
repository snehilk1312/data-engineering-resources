from __future__ import annotations
from pathlib import Path
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from prefect import task, flow
from datetime import timedelta

@task(retries=3)
def fetch(url: str) -> pd.DataFrame:
    """Fetch data from a URL"""
    df = pd.read_csv(url,compression='gzip',index_col=False)
    print(df.head())
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dtype issues"""
    print('Before Cleaning')
    print(df.dtypes)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print('After Cleaning')
    print(df.dtypes)

    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Writes dataframe to local storage"""
    path = f"data/{color}/{dataset_file}.parquet"
    df.to_parquet(path,index=False,compression='gzip')
    return path

@task(log_prints=True, retries=3)
def write_gcs(path: Path) -> None:
    """Write from local storeage to google cloud storage"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        path,path
    )
    return

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,color,dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(year: int=2021, months: list=[1,2], color: str="yellow"):
    for month in months:
        print(month)
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    year = 2021
    months = [1,2,3]
    color = "yellow"
    etl_parent_flow(year, months, color)

# prefect deployment build ./parameterized_etl_web_to_gcs.py:etl_parent_flow -n etl_web_gcs
# prefect deployment apply etl_parent_flow-deployment.yaml