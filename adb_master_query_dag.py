from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id='adb_master_query',
    start_date=datetime(2025, 9, 9),
    schedule='@once',
    catchup=False,
    tags=['adb', 'example'],
) as dag:
    execute_drop_shema_trino_query = SQLExecuteQueryOperator(
        task_id='drop_schema_adb_master',
        sql="DROP SCHEMA IF EXISTS example CASCADE;",
        conn_id='sel-dev-master-adb',
        autocommit=True,
        split_statements=True,
    )

    execute_create_shema_trino_query = SQLExecuteQueryOperator(
        task_id='create_schema_adb_master',
        sql="CREATE SCHEMA IF NOT EXISTS example;",
        conn_id='sel-dev-master-adb',
        autocommit=True,
        split_statements=True,
    )

    execute_drop_table_trino_query = SQLExecuteQueryOperator(
        task_id='drop_table_adb_master',
        sql="DROP TABLE IF EXISTS example.names;",
        conn_id='sel-dev-master-adb',
        autocommit=True,
        split_statements=True,
    )

    execute_create_table_trino_query = SQLExecuteQueryOperator(
        task_id='create_table_adb_master',
        sql="""
        CREATE TABLE example.names (
        id INT,
        name VARCHAR,
        timestamp VARCHAR
        );
        """,
        conn_id='sel-dev-master-adb',
        autocommit=True,
        split_statements=True,
    )

    execute_incert_to_table_trino_query = SQLExecuteQueryOperator(
        task_id='incert_to_table_adb_master',
        sql="""
        INSERT INTO example.names VALUES
        (1, 'Alice', '2025-01-01 10:00:00'),
        (2, 'Bob', '2025-01-02 11:30:00');
        """,
        conn_id='sel-dev-master-adb',
        autocommit=True,
        split_statements=True,
    )

    execute_select_table_trino_query = SQLExecuteQueryOperator(
        task_id='select_from_table_adb_master',
        sql="SELECT * FROM example.names;",
        conn_id='sel-dev-master-adb',
        autocommit=True,
        split_statements=True,
    )

    execute_create_as_select_table_trino_query = SQLExecuteQueryOperator(
        task_id='create_as_select_from_table_adb_master',
        sql="CREATE TABLE example.names_from AS SELECT * FROM example.names;",
        conn_id='sel-dev-master-adb',
        autocommit=True,
        split_statements=True,
    )

    # Define task dependencies
    execute_drop_shema_trino_query >> \
    execute_create_shema_trino_query >> \
    execute_drop_table_trino_query >> \
    execute_create_table_trino_query >> \
    execute_incert_to_table_trino_query >> \
    execute_select_table_trino_query >> \
    execute_create_as_select_table_trino_query
