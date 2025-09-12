from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import json

s3_connection = BaseHook.get_connection('sel-dev-s3-logs')

access_key = s3_connection.login
secret_key = s3_connection.password
extra_data = json.loads(s3_connection.extra)
endpoint = extra_data.get("endpoint")

s3_external_connection = BaseHook.get_connection('vk-external-s3')
external_access_key = s3_external_connection.login
external_secret_key = s3_external_connection.password
external_extra_data = json.loads(s3_external_connection.extra)
external_endpoint = external_extra_data.get("endpoint")
external_bucket = external_extra_data.get("bucket")

sc_spark_connection = BaseHook.get_connection('sel-dev-spark-connect')
print("=====================")
print(BaseHook.get_connection('sel-dev-spark-connect'))
print("=====================")
sc_spark_connection_extra_data = json.loads(sc_spark_connection.extra)
sc_url = external_extra_data.get("sc_url")
print(sc_url)
print("=====================")

with DAG(
    dag_id='spark_connect_s3_iceberg',
    start_date=datetime(2023, 1, 1),
    schedule='@once',
    catchup=False,
) as dag:
    submit_spark_job = SparkSubmitOperator(
        task_id='spark_connect_s3_iceberg_read',
        application='s3a://demo-bucket/pyspark/01_spark_read_iceberg.py',
        conn_id='sel-dev-spark-connect',
        conf={'spark.hadoop.fs.s3a.access.key': access_key,
              "spark.hadoop.fs.s3a.secret.key": secret_key,
              "spark.hadoop.fs.s3a.endpoint": endpoint},
        application_args=[external_access_key,
                          external_secret_key,
                          external_endpoint,
                          external_bucket,
                          sc_url],
    )
