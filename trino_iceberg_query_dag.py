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
    execute_drop_shema_trino_query = SQLExecuteQueryOperator(
        task_id='drop_shcema_iceberg',
        sql="DROP SCHEMA IF EXISTS iceberg.example;",
        conn_id='sel-dev-trino-iceberg',
        autocommit=True,
        split_statements=True,
    )

    execute_create_shema_trino_query = SQLExecuteQueryOperator(
        task_id='create_shcema_iceberg',
        sql="CREATE SCHEMA IF NOT EXISTS iceberg.example;",
        conn_id='sel-dev-trino-iceberg',
        autocommit=True,
        split_statements=True,
    )

    execute_drop_table_trino_query = SQLExecuteQueryOperator(
        task_id='drop_table_iceberg',
        sql="DROP TABLE IF EXISTS iceberg.example.names;",
        conn_id='sel-dev-trino-iceberg',
        autocommit=True,
        split_statements=True,
    )

    # execute_create_table_trino_query = SQLExecuteQueryOperator(
    #     task_id='create_table_iceberg',
    #     sql="""
    #     CREATE TABLE iceberg.example.names (
    #     id INT,
    #     name VARCHAR,
    #     timestamp TIMESTAMP
    #     );
    #     """,
    #     conn_id='sel-dev-trino-iceberg',
    #     autocommit=True,
    #     split_statements=True,
    # )
    #
    # execute_incert_to_table_trino_query = SQLExecuteQueryOperator(
    #     task_id='incert_to_table_iceberg',
    #     sql="""
    #     INSERT INTO iceberg.example.names VALUES
    #     (1, 'Alice', '2025-01-01 10:00:00'),
    #     (2, 'Bob', '2025-01-02 11:30:00');
    #     """,
    #     conn_id='sel-dev-trino-iceberg',
    #     autocommit=True,
    #     split_statements=True,
    # )

    # Define task dependencies
    execute_drop_shema_trino_query >> \
    execute_create_shema_trino_query >> \
    execute_drop_table_trino_query

    # execute_create_table_trino_query >> \
    # execute_incert_to_table_trino_query
