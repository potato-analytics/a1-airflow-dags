from pyspark.sql import SparkSession
from pyspark import SparkConf

endpoint_url_src = 'https://hb.ru-msk.vkcs.cloud'
access_key_src = 'fcztu19kAqsh5zA9z3gvcg'
secret_key_src = 'gbEYnvDsuDDeKZynNnNfuk4KEMnBaBb9VXf12smtBswD'

spark = SparkSession.builder \
    .appName("JupyterHub_Spark_Read_From_other_S3_bucket") \
    .getOrCreate()

# Settings access to S3
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

# Settings access to source S3
hadoop_conf.set("fs.s3a.bucket.c-2cc54d52-e5ff-4f88-a33c-a315f9516d54.access.key", access_key_src)
hadoop_conf.set("fs.s3a.bucket.c-2cc54d52-e5ff-4f88-a33c-a315f9516d54.secret.key", secret_key_src)
hadoop_conf.set("fs.s3a.bucket.c-2cc54d52-e5ff-4f88-a33c-a315f9516d54.endpoint", endpoint_url_src)

# Read data from source S3
df = spark.read.format("iceberg").load("s3a://c-2cc54d52-e5ff-4f88-a33c-a315f9516d54/light-metastore/tpcds_1000.db/call_center/metadata/00000-6a21b37c-89fb-4066-b5b9-97fa81f1f9ff.metadata.json")

# Show one line from source data
df.show(10,0)

spark.sparkContext.stop()