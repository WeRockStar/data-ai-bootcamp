import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from faker import Faker
import pandas as pd

fake = Faker()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_customers(num_records=500):
    """Generate customer data"""
    customers = []
    for _ in range(num_records):
        customer = {
            'customer_id': fake.uuid4(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address()
        }
        customers.append(customer)
    return pd.DataFrame(customers)

def load_to_postgres(df, table_name):
    """Load DataFrame to PostgreSQL"""
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    df.to_sql(
        table_name,
        postgres_hook.get_sqlalchemy_engine(),
        if_exists='append',
        chunksize=1000
    )
    print(f"Loaded {len(df)} records to {table_name}")

def create_and_load_customers():
    df = generate_customers()
    load_to_postgres(df, 'customers')

with DAG(
    'retail_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'data_generation']
) as dag:

    # Create task
    customer_task = PythonOperator(
        task_id='create_customers',
        python_callable=create_and_load_customers
    )

    # Set dependencies
    customer_task