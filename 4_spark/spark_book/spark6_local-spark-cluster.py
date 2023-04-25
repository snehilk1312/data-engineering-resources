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

df = spark.read.parquet(f'data/pq/{color}/{year}/*')

print('*'*150)
print(df.count())
print('*'*150)

# ./start-master.sh
# go to localhost:7077, get url , eg: "spark://MSI.localdomain:7077"
# ./start-worker.sh spark://MSI.localdomain:7077 
# spark-submit --master=spark://MSI.localdomain:7077 spark6_spark-submit.py green 2021