from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import json

s3_connection = BaseHook.get_connection('sel-dev-s3-logs')

access_key = s3_connection.login
secret_key = s3_connection.password
extra_data = json.loads(s3_connection.extra)
endpoint = extra_data.get("endpoint")

sc_spark_connection = BaseHook.get_connection('sel-dev-spark-connect')
sc_spark_connection_extra_data = json.loads(sc_spark_connection.extra)
sc_url = sc_spark_connection_extra_data.get("sc_url")

# Instantiate the DAG
with DAG(
    dag_id='bash_run_spark_connect',
    start_date=datetime(2025, 9, 9),
    schedule='@once',
    catchup=False,
    tags=['spark_connect', 'example'],
) as dag:
    run_spark_connect = BashOperator(
        task_id='bash_spark_connect_run',
        bash_command='spark-submit remote ' + sc_url +
                     ' --conf spark.hadoop.fs.s3a.access.key=' + access_key +
                     ' --conf spark.hadoop.fs.s3a.secret.key=' + secret_key +
                     ' --conf spark.hadoop.fs.s3a.endpoint=' + endpoint +
                     ' --name arrow-spark'
                     ' s3a://demo-bucket/pyspark/01_spark_sql_simple.py ' + sc_url,
    )

    # Define task dependencies
    run_spark_connect
