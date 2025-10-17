from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="doris_iceberg_query",
    start_date=datetime(2025, 9, 9),
    schedule="@once",
    catchup=False,
    tags=["doris", "common"],
) as dag:
    create_as_select_doris_customer_total_by_state = SQLExecuteQueryOperator(
        task_id="doris_customer_total_by_state",
        sql="""
        CREATE OR REPLACE VIEW arena_int.customer_total_by_state AS
        SELECT
            C.customer_state,
            ROUND(SUM(I.price), 2) AS total_amount_usd
        FROM iceberg_dwh.arena_iceberg.customers C
        INNER JOIN iceberg_dwh.arena_iceberg.orders O ON C.customer_id = O.customer_id
        INNER JOIN iceberg_dwh.arena_iceberg.order_items I ON O.order_id = I.order_id
        GROUP BY C.customer_state
        ORDER BY total_amount_usd DESC;
        """,
        conn_id="Doris-connect",
        autocommit=True,
        split_statements=True,
    )

create_as_select_doris_customer_total_by_state
