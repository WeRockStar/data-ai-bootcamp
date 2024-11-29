from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum


def generate_data(**context):
    import random

    data = {
        "customer_id": random.randint(1, 1000),
        "amount": random.randint(100, 10000),
    }
    context["task_instance"].xcom_push(key="order_data", value=data)
    print(f"Generated order data: {data}")


def process_data(**context):
    order = context["task_instance"].xcom_pull(
        key="order_data", task_ids="generate_order"
    )
    processed_amount = order["amount"] * 1.1  # Add 10% processing fee
    context["task_instance"].xcom_push(key="processed_amount", value=processed_amount)
    print(f"Processed amount: {processed_amount}")


def send_notification(**context):
    processed_amount = context["task_instance"].xcom_pull(
        key="processed_amount", task_ids="process_order"
    )
    print(f"Sending notification for processed amount: {processed_amount}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 1, tz="Asia/Bangkok"),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": timedelta(minutes=5),
}

with DAG(
    dag_id="02_xcom_example_dag",
    default_args=default_args,
    description="A DAG demonstrating XCom usage",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
) as dag:

    generate_task = PythonOperator(
        task_id="generate_order",
        python_callable=generate_data,
    )

    process_task = PythonOperator(
        task_id="process_order",
        python_callable=process_data,
    )

    notify_task = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
    )

    generate_task >> process_task >> notify_task
