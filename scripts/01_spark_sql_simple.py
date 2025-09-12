import sys

sc_url = sys.argv[1]

from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf

spark = SparkSession.builder.remote("sc://10.200.0.240:15002") \
    .appName("Airflow_SparkConnect_Read_From_other_S3_bucket") \
    .getOrCreate()

spark.sql("CREATE TABLE default.spark_connect_example (id INT)")

spark.sql("select * from default.spark_connect_example")

spark.sparkContext.stop()