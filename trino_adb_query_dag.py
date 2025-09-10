from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id='trino_adb_query',
    start_date=datetime(2025, 9, 9),
    schedule='@once',
    catchup=False,
    tags=['trino', 'example'],
) as dag:
    execute_drop_shema_trino_query = SQLExecuteQueryOperator(
        task_id='drop_schema_adb',
        sql="DROP SCHEMA IF EXISTS adb.example CASCADE;",
        conn_id='sel-dev-trino-adb',
        autocommit=True,
        split_statements=True,
    )

    execute_create_shema_trino_query = SQLExecuteQueryOperator(
        task_id='create_schema_adb',
        sql="CREATE SCHEMA IF NOT EXISTS adb.example;",
        conn_id='sel-dev-trino-adb',
        autocommit=True,
        split_statements=True,
    )

    execute_drop_table_trino_query = SQLExecuteQueryOperator(
        task_id='drop_table_adb',
        sql="DROP TABLE IF EXISTS adb.example.names;",
        conn_id='sel-dev-trino-adb',
        autocommit=True,
        split_statements=True,
    )

    execute_create_table_trino_query = SQLExecuteQueryOperator(
        task_id='create_table_adb',
        sql="""
        CREATE TABLE adb.example.names (
        id INT,
        name VARCHAR,
        timestamp VARCHAR
        );
        """,
        conn_id='sel-dev-trino-adb',
        autocommit=True,
        split_statements=True,
    )

    # TODO https://tracker.yandex.ru/NGSOK-882
    # execute_incert_to_table_trino_query = SQLExecuteQueryOperator(
    #     task_id='incert_to_table_adb',
    #     sql="""
    #     INSERT INTO adb.example.names VALUES
    #     (1, 'Alice', '2025-01-01 10:00:00'),
    #     (2, 'Bob', '2025-01-02 11:30:00');
    #     """,
    #     conn_id='sel-dev-trino-adb',
    #     autocommit=True,
    #     split_statements=True,
    # )

    # execute_select_table_trino_query = SQLExecuteQueryOperator(
    #     task_id='select_from_table_adb',
    #     sql="SELECT * FROM adb.example.names;",
    #     conn_id='sel-dev-trino-adb',
    #     autocommit=True,
    #     split_statements=True,
    # )

    # execute_create_as_select_table_trino_query = SQLExecuteQueryOperator(
    #     task_id='create_as_select_from_table_adb',
    #     sql="CREATE TABLE adb.example.names_from AS SELECT * FROM iceberg.example.names;",
    #     conn_id='sel-dev-trino-adb',
    #     autocommit=True,
    #     split_statements=True,
    # )

    # Define task dependencies
    execute_drop_shema_trino_query >> \
    execute_create_shema_trino_query >> \
    execute_drop_table_trino_query >> \
    execute_create_table_trino_query
