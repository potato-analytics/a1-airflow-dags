from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

with DAG(
    dag_id='airflow_to_trino_query',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['trino', 'example'],
) as dag:
    # Define a TrinoOperator task to execute a simple SELECT query
    trino_select_task = TrinoOperator(
        task_id='execute_trino_select',
        sql="SELECT 1;",
        trino_conn_id='sel-dev-trino',  # This refers to an Airflow connection named 'trino_default'
    )

    # # Define another TrinoOperator task to execute an INSERT statement
    # trino_insert_task = TrinoOperator(
    #     task_id='execute_trino_insert',
    #     sql="INSERT INTO my_catalog.my_schema.another_table (col1, col2) VALUES ('value1', 'value2');",
    #     trino_conn_id='trino_default',
    #     autocommit=True,  # Set to True for DML statements like INSERT
    # )
    #
    # # You can also execute SQL from a file using a Jinja template
    # trino_from_file_task = TrinoOperator(
    #     task_id='execute_trino_from_file',
    #     sql='templates/my_trino_query.sql',  # Path to a .sql file in your DAGs folder
    #     trino_conn_id='trino_default',
    #     parameters={'start_date': '{{ ds }}', 'end_date': '{{ next_ds }}'}, # Example of passing parameters
    # )

    # Define task dependencies
    trino_select_task
    #>> trino_insert_task >> trino_from_file_task
