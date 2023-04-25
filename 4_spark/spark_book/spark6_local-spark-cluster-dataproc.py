import pyspark
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('color')
parser.add_argument('year')

args = parser.parse_args()


color = args.color
year = args.year

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

df = spark.read.parquet(f'gs://snehil-devbucket/pq/{color}/{year}/*')

print('*'*150)
print(df.count())
print('*'*150)

# use dataproc ui or gcloud dataproc cli