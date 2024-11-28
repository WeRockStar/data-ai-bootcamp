from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello World")

def print_hello_again():
    print("Hello World Again")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple DAG that prints Hello World',
    schedule_interval=timedelta(minutes=2),
    catchup=False
) as dag:

    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    hello_again_task = PythonOperator(
        task_id='print_hello_again',
        python_callable=print_hello_again,
    )
    
    hello_task >> hello_again_task
