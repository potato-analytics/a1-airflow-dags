import sys

sc_url = sys.argv[1]

from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession.builder.remote(sc_url) \
    .appName("Airflow_SparkConnect_Read_From_other_S3_bucket") \
    .getOrCreate()

spark.sql("CREATE TABLE default.spark_connect_example (id INT)").show(1,0)

spark.sql("select * from default.spark_connect_example").show(1,0)

spark.sparkContext.stop()