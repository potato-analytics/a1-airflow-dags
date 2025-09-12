import sys


sc_url = sys.argv[1]



from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession.builder.remote(sc_url) \
    .appName("Airflow_SparkConnect_Read_From_other_S3_bucket") \
    .getOrCreate()

spark.sql("select 1").show(1,0)

spark.sparkContext.stop()