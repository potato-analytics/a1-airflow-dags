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

with DAG(
    dag_id='spark_connect_s3_iceberg',
    start_date=datetime(2023, 1, 1),
    schedule='@once',
    catchup=False,
) as dag:
    submit_spark_job = SparkSubmitOperator(
        task_id='spark_connect_s3_iceberg_read',
        application='s3a://demo-bucket/pyspark/01_spark_read_iceberg.py',  # Or .jar file
        conn_id='sel-dev-spark-connect',  # The Connection Id you defined
        conf={'spark.hadoop.fs.s3a.access.key': access_key,
              "spark.hadoop.fs.s3a.secret.key": secret_key,
              "spark.hadoop.fs.s3a.endpoint": endpoint},  # Optional Spark configurations
        #application_args=['arg1', 'arg2'],  # Arguments for your Spark application
    )
