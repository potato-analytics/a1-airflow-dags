from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from airflow.hooks.base import BaseHook
s3_connection = BaseHook.get_connection('sel-dev-airflow-scripts')
print("==============")
print(s3_connection)
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
        packages='org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901', # Example for additional packages
        #conf={'spark.executor.memory': '4g'},  # Optional Spark configurations
        #application_args=['arg1', 'arg2'],  # Arguments for your Spark application
    )
