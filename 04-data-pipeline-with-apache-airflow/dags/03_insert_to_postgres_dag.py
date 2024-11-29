import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from faker import Faker
import pandas as pd
import pendulum

fake = Faker()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 1, tz="Asia/Bangkok"),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "schedule_interval": "*/5 * * * *",
    "tags": ["retail", "data_generation"],
}


def generate_customers(num_records=100, **context):
    """Generate customer data"""
    customers = []
    for _ in range(num_records):
        customer = {
            "customer_id": fake.uuid4(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "created_at": pendulum.now("Asia/Bangkok"),
            "address": fake.address(),
        }
        customers.append(customer)
    context["task_instance"].xcom_push(key="customers", value=customers)
    return customers


def load_to_postgres(**context):
    """Load DataFrame to PostgreSQL"""
    customers = context["task_instance"].xcom_pull(
        key="customers", task_ids="generate_customers"
    )
    df = pd.DataFrame(customers)
    postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
    df.to_sql(
        "customer_airflow",
        postgres_hook.get_sqlalchemy_engine(),
        index=False,
        if_exists="append",
        chunksize=1000,
    )
    print(f"Loaded {len(df)} records to customers table")


with DAG(
    dag_id="03_insert_to_postgres_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "data_generation"],
) as dag:

    # สร้างตาราง customers ถ้ายังไม่มี
    create_table = PostgresOperator(
        task_id="create_customers_table",
        postgres_conn_id="pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS customer_airflow (
                id SERIAL PRIMARY KEY,
                customer_id VARCHAR(255),
                first_name VARCHAR(100),
                last_name VARCHAR(100), 
                email VARCHAR(255),
                phone VARCHAR(50),
                created_at TIMESTAMP,
                address TEXT
            );
        """,
    )
    # Generate customers task
    gen_customers_task = PythonOperator(
        task_id="generate_customers", python_callable=generate_customers
    )

    # Load to postgres task
    load_to_pg_task = PythonOperator(
        task_id="load_to_postgres", python_callable=load_to_postgres
    )

    # Set dependencies
    create_table >> gen_customers_task >> load_to_pg_task
