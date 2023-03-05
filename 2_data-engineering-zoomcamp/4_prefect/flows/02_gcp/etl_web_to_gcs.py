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
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2020
    month = 3
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,color,dataset_file)
    write_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs()