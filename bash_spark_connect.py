from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.hooks.base import BaseHook
import json

s3_connection = BaseHook.get_connection('sel-dev-s3-logs')
access_key = s3_connection.login
secret_key = s3_connection.password
extra_data = json.loads(s3_connection.extra)
endpoint = extra_data.get("endpoint")

s3_external_connection = BaseHook.get_connection('vk-external-s3')
external_extra_data = json.loads(s3_external_connection.extra)
external_endpoint = external_extra_data.get("endpoint")

with DAG(
    dag_id='bash_spark_connect',
    start_date=datetime(2023, 1, 1),
    schedule='@once',
    catchup=False,
    tags=['example', 'spark_connect'],
) as dag:
    # Task 1: Execute a simple bash command
    task_spark_submit = BashOperator(
        task_id='run_simple_spark_connect',
        bash_command='spark-submit --remote sc://10.200.0.240:15002'
                     ' --conf spark.hadoop.fs.s3a.access.key=' + access_key +
                     ' --conf spark.hadoop.fs.s3a.secret.key=' + secret_key +
                     ' --conf spark.hadoop.fs.s3a.endpoint=' + endpoint +
                     ' --name arrow-spark s3a://demo-bucket/pyspark/01_spark_sql_simple.py sc://10.200.0.240:15002',
    )

    # Define task dependencies
    task_spark_submit