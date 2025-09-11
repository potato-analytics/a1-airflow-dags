from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from airflow.hooks.base import BaseHook
s3_connection = BaseHook.get_connection('sel-dev-s3-logs')
print("==============")
print(s3_connection.login)
print(s3_connection.password)
print("==============")

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
        #conf={'spark.executor.memory': '4g'},  # Optional Spark configurations
        #application_args=['arg1', 'arg2'],  # Arguments for your Spark application
    )
