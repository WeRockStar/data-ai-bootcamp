import json
import random
import traceback
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context

import pandas as pd
import numpy as np
from faker import Faker
from pymongo import MongoClient
from bson import ObjectId

# Initialize Faker
fake = Faker()

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='retail_mongo_logging',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'mongodb', 'logging', 'data_generation']
)
def retail_data_logging_etl():
    @task()
    def generate_advanced_retail_data():
        """
        Generate comprehensive retail dataset with detailed logging metadata
        """
        # Product categories with advanced metadata
        product_categories = [
            {
                'category': 'Electronics',
                'subcategories': ['Smartphones', 'Laptops', 'Accessories'],
                'price_range': (100, 2000),
                'profit_margin': (0.1, 0.3)
            },
            {
                'category': 'Fashion',
                'subcategories': ['Mens Wear', 'Womens Wear', 'Kids Wear'],
                'price_range': (20, 500),
                'profit_margin': (0.3, 0.5)
            },
            {
                'category': 'Home & Living',
                'subcategories': ['Furniture', 'Decor', 'Kitchen'],
                'price_range': (50, 1000),
                'profit_margin': (0.2, 0.4)
            }
        ]

        # Generate sales transactions with rich metadata
        sales_data = []
        for _ in range(2000):  # 2000 sales transactions
            # Select category
            category_info = random.choice(product_categories)
            
            # Generate transaction details
            base_price = round(random.uniform(*category_info['price_range']), 2)
            quantity = random.randint(1, 5)
            profit_margin = round(random.uniform(*category_info['profit_margin']), 3)
            
            # Advanced transaction metadata
            sale = {
                'transaction_id': str(ObjectId()),  # Unique MongoDB-style ID
                'timestamp': datetime.utcnow(),
                
                # Product Details
                'product': {
                    'category': category_info['category'],
                    'subcategory': random.choice(category_info['subcategories']),
                    'base_price': base_price,
                },
                
                # Sales Details
                'sales_details': {
                    'quantity': quantity,
                    'total_price': round(base_price * quantity, 2),
                    'profit_margin': profit_margin,
                    'net_profit': round(base_price * quantity * profit_margin, 2)
                },
                
                # Customer Metadata
                'customer': {
                    'id': str(ObjectId()),
                    'name': fake.name(),
                    'email': fake.email(),
                    'segment': random.choice([
                        'Retail', 'Wholesale', 'Online', 'Corporate'
                    ])
                },
                
                # Contextual Information
                'context': {
                    'channel': random.choice(['Online', 'In-Store', 'Mobile App']),
                    'device': random.choice(['Desktop', 'Mobile', 'Tablet']),
                    'location': {
                        'city': fake.city(),
                        'country': fake.country(),
                        'timezone': fake.timezone()
                    }
                },
                
                # Promotional Information
                'promotion': {
                    'applied': random.random() > 0.7,
                    'discount_percentage': round(random.uniform(0, 20), 2) if random.random() > 0.7 else 0
                },
                
                # Operational Metadata
                'metadata': {
                    'dag_run_id': get_current_context().get('dag_run').run_id,
                    'execution_date': get_current_context().get('execution_date')
                }
            }
            
            sales_data.append(sale)
        
        return sales_data

    @task()
    def log_to_mongodb(sales_data):
        """
        Log sales data to MongoDB with comprehensive error handling and logging
        """
        # Retrieve MongoDB connection from Airflow connections
        try: