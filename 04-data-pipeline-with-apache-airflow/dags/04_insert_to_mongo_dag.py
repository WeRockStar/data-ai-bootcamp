from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from faker import Faker
import random
import pendulum

fake = Faker()

# กำหนดโครงสร้างข้อมูลสินค้า
product_schema = {
    "name": str,  # ชื่อสินค้า 
    "description": str,  # รายละเอียดสินค้า
    "price": float,  # ราคา
    "category": ["Electronics", "Clothing", "Books", "Home & Garden", "Sports"],
    "condition": ["New", "Used", "Refurbished"],
    "stock": int,  # จำนวนในคลัง
    "manufacturer": str,  # ผู้ผลิต
    "sku": str,  # รหัสสินค้า
    "created_at": "datetime",  # วันที่สร้าง
    "rating": float,  # คะแนน
    "reviews_count": int,  # จำนวนรีวิว
}

def generate_product(**context):
    """Generate product data"""
    product = {
        "name": fake.catch_phrase(),
        "description": fake.text(),
        "price": round(random.uniform(10.0, 1000.0), 2),
        "category": random.choice(product_schema["category"]),
        "condition": random.choice(product_schema["condition"]), 
        "stock": random.randint(0, 100),
        "manufacturer": fake.company(),
        "sku": fake.ean13(),
        "created_at": pendulum.now("Asia/Bangkok").strftime("%Y-%m-%d %H:%M:%S"),
        "rating": round(random.uniform(1, 5), 1),
        "reviews_count": random.randint(0, 1000),
    }
    context['task_instance'].xcom_push(key='product', value=product)
    return product

def insert_to_mongo(**context):
    """Insert product into MongoDB"""
    product = context['task_instance'].xcom_pull(key='product', task_ids='generate_product')
    hook = MongoHook(mongo_conn_id='mongo_default')
    hook.insert_one(
        mongo_collection='product_attributes',
        mongo_db='retail',
        doc=product
    )
    print(f"เพิ่มสินค้า: {product['name']}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

with DAG(
    'mongo_product_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'mongodb']
) as dag:

    # Generate product task
    gen_product_task = PythonOperator(
        task_id='generate_product',
        python_callable=generate_product
    )

    # Insert to MongoDB task
    insert_mongo_task = PythonOperator(
        task_id='insert_to_mongo',
        python_callable=insert_to_mongo
    )

    # Set dependencies
    gen_product_task >> insert_mongo_task
