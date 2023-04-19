import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName('mnm').getOrCreate()

df = spark.read.csv('data/mnm.csv',header='True',inferSchema=True)

df = df.groupBy('State','Color').agg(count("Count").alias('num')).orderBy('State','Color')

df.show(truncate=False)

df_CA = df.where(df.State=='CA')
df_CA.show()

spark.stop()