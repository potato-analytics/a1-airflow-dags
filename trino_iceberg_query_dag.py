from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id='trino_iceberg_query_example',
    start_date=datetime(2025, 9, 9),
    schedule='@once',
    catchup=False,
    tags=['trino', 'example'],
) as dag:
    # Define a TrinoOperator task to execute a simple SELECT query
    execute_trino_query = SQLExecuteQueryOperator(
        task_id='execute_trino_iceberg_query',
        sql="SELECT 1;",
        conn_id='sel-dev-trino-iceberg',  # Replace with your Trino connection ID
        autocommit=True,
        split_statements=True,  # Important for multiple statements
    )


    # Define task dependencies
    execute_trino_query
    #>> trino_insert_task >> trino_from_file_task
