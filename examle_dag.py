import time
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'my_dag_2',  # Уникальный идентификатор DAG
        default_args=default_args,
        description='Мой первый DAG',
        schedule=timedelta(minutes=4),  # Как часто запускать
        start_date=datetime(2025, 1, 1),  # Дата начала выполнения
        catchup=False,  # Запускать ли пропущенные запуски
        tags=['example'],
) as dag2:
    def extract_function2():
        time.sleep(10)
        return


    def transform_function2():
        time.sleep(10)
        return


    def load_function2():
        time.sleep(10)
        return


    extract2 = PythonOperator(
        task_id='extract_data_2',
        python_callable=extract_function2,
    )

    transform2 = PythonOperator(
        task_id='transform_data_2',
        python_callable=transform_function2,
    )

    load2 = PythonOperator(
        task_id='load_data_2',
        python_callable=load_function2,
    )

extract2 >> transform2 >> load2