import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    "schedule_interval": timedelta(minutes=2),
    "catchup": False,
}


def generate_orders(num_records=1000):
    """Generate order data"""
    orders = []
    for _ in range(num_records):
        order = {
            "order_id": fake.uuid4(),
            "order_date": fake.date_between(start_date="-60d", end_date="today"),
            "order_status": random.choice(
                ["Pending", "Completed", "Cancelled", "Shipped"]
            ),
            "total_amount": round(random.uniform(10, 1000), 2),
            "customer_id": fake.uuid4(),
            "store_id": fake.uuid4(),
        }
        orders.append(order)
    return pd.DataFrame(orders)


def generate_products(num_records=200):
    """Generate product data"""
    products = []
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]
    for _ in range(num_records):
        product = {
            "product_id": fake.uuid4(),
            "product_name": fake.catch_phrase(),
            "category": random.choice(categories),
            "unit_price": round(random.uniform(5, 500), 2),
            "supplier_id": fake.uuid4(),
        }
        products.append(product)
    return pd.DataFrame(products)


def generate_customers(num_records=500):
    """Generate customer data"""
    customers = []
    for _ in range(num_records):
        customer = {
            "customer_id": fake.uuid4(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address(),
        }
        customers.append(customer)
    return pd.DataFrame(customers)


def generate_stores(num_records=50):
    """Generate store data"""
    stores = []
    regions = ["North", "South", "East", "West", "Central"]
    for _ in range(num_records):
        store = {
            "store_id": fake.uuid4(),
            "store_name": f"Store {fake.company()}",
            "region": random.choice(regions),
            "address": fake.address(),
            "manager": fake.name(),
        }
        stores.append(store)
    return pd.DataFrame(stores)


def generate_inventory(num_records=1000):
    """Generate inventory data"""
    inventory = []
    for _ in range(num_records):
        inventory_item = {
            "inventory_id": fake.uuid4(),
            "product_id": fake.uuid4(),
            "store_id": fake.uuid4(),
            "quantity": random.randint(0, 1000),
            "last_updated": fake.date_time_this_month(),
        }
        inventory.append(inventory_item)
    return pd.DataFrame(inventory)


def generate_promotions(num_records=100):
    """Generate promotion data"""
    promotions = []
    for _ in range(num_records):
        promotion = {
            "promotion_id": fake.uuid4(),
            "promotion_name": f"Promotion {fake.word()}",
            "discount_percent": round(random.uniform(0.05, 0.5), 2),
            "start_date": fake.date_this_month(),
            "end_date": fake.date_this_month(after_today=True),
            "product_id": fake.uuid4(),
        }
        promotions.append(promotion)
    return pd.DataFrame(promotions)


def load_to_postgres(df, table_name):
    """Load DataFrame to PostgreSQL"""
    postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
    df.to_sql(
        table_name,
        postgres_hook.get_sqlalchemy_engine(),
        if_exists="append",
        chunksize=1000,
        index=False,
    )
    print(f"Loaded {len(df)} records to {table_name}")


def create_and_load_orders():
    df = generate_orders()
    load_to_postgres(df, "orders05")


def create_and_load_products():
    df = generate_products()
    load_to_postgres(df, "products05")


def create_and_load_customers():
    df = generate_customers()
    load_to_postgres(df, "customers05")


def create_and_load_stores():
    df = generate_stores()
    load_to_postgres(df, "stores05")


def create_and_load_inventory():
    df = generate_inventory()
    load_to_postgres(df, "inventory05")


def create_and_load_promotions():
    df = generate_promotions()
    load_to_postgres(df, "promotions05")


with DAG(
    dag_id="05_insert_retail_data_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Bangkok"),
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    tags=["retail", "data_generation"],
) as dag:

    # Create tasks
    order_task = PythonOperator(
        task_id="create_orders", python_callable=create_and_load_orders
    )

    product_task = PythonOperator(
        task_id="create_products", python_callable=create_and_load_products
    )

    customer_task = PythonOperator(
        task_id="create_customers", python_callable=create_and_load_customers
    )

    store_task = PythonOperator(
        task_id="create_stores", python_callable=create_and_load_stores
    )

    inventory_task = PythonOperator(
        task_id="create_inventory", python_callable=create_and_load_inventory
    )

    promotion_task = PythonOperator(
        task_id="create_promotions", python_callable=create_and_load_promotions
    )

    # Set dependencies
    (
        order_task
        >> [product_task, customer_task]
        >> store_task
        >> inventory_task
        >> promotion_task
    )
