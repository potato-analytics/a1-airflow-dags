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
    execute_drop_trino_query = SQLExecuteQueryOperator(
        task_id='drop_shcema_iceberg',
        sql="DROP SCHEMA IF EXISTS iceberg.example;",
        conn_id='sel-dev-trino-iceberg',
        autocommit=True,
        split_statements=True,
    )

    execute_create_trino_query = SQLExecuteQueryOperator(
        task_id='create_shcema_iceberg',
        sql="CREATE SCHEMA IF NOT EXISTS iceberg.example;",
        conn_id='sel-dev-trino-iceberg',
        autocommit=True,
        split_statements=True,
    )


    # Define task dependencies
    execute_drop_trino_query >> execute_create_trino_query
