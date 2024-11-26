import random
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd
import numpy as np
from faker import Faker

# Initialize Faker
fake = Faker()

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='retail_data_sqlalchemy',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'sqlalchemy', 'data_generation']
)
def retail_data_etl():
    @task()
    def generate_comprehensive_sales_data():
        """Generate comprehensive retail sales dataset."""
        # Define product categories with more detailed metadata
        product_categories = [
            {'category': 'Electronics', 'margin_range': (0.1, 0.3), 'avg_price_range': (50, 1000)},
            {'category': 'Clothing', 'margin_range': (0.3, 0.5), 'avg_price_range': (20, 500)},
            {'category': 'Home & Garden', 'margin_range': (0.2, 0.4), 'avg_price_range': (30, 300)},
            {'category': 'Sports', 'margin_range': (0.25, 0.45), 'avg_price_range': (40, 400)},
            {'category': 'Books', 'margin_range': (0.4, 0.6), 'avg_price_range': (10, 100)},
        ]
        
        # Generate sales data
        sales_data = []
        for _ in range(5000):  # 5000 sales transactions
            # Randomly select category
            cat_info = random.choice(product_categories)
            
            # Generate product details
            base_price = round(random.uniform(*cat_info['avg_price_range']), 2)
            margin = round(random.uniform(*cat_info['margin_range']), 2)
            
            sale = {
                'transaction_id': fake.uuid4(),
                'category': cat_info['category'],
                'product_name': fake.catch_phrase(),
                'base_price': base_price,
                'quantity': random.randint(1, 5),
                'margin': margin,
                'sale_date': fake.date_between(start_date='-60d', end_date='today'),
                'customer_segment': random.choice([
                    'Retail', 'Wholesale', 'Online', 'In-Store'
                ]),
                'store_region': random.choice([
                    'North', 'South', 'East', 'West', 'Central'
                ]),
                'discount_applied': round(random.uniform(0, 0.2), 2),
                'total_sale_amount': round(base_price * random.randint(1, 5) * (1 - margin), 2)
            }
            sales_data.append(sale)
        
        # Convert to DataFrame
        df = pd.DataFrame(sales_data)
        
        return df

    @task()
    def process_and_load_data(sales_df):
        """Process sales data and load to PostgreSQL using SQLAlchemy."""
        # Get PostgreSQL connection from Airflow connections
        postgres_conn = BaseHook.get_connection('postgres_default')
        
        # Construct SQLAlchemy connection string
        connection_string = (
            f"postgresql://{postgres_conn.login}:{postgres_conn.password}@"
            f"{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"
        )
        
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        try:
            # Aggregate sales by category
            category_sales = sales_df.groupby('category').agg({
                'total_sale_amount': ['sum', 'mean', 'count'],
                'quantity': 'sum',
                'base_price': 'mean'
            }).reset_index()
            category_sales.columns = [
                'category', 'total_sales', 'avg_sale_value', 
                'total_transactions', 'total_units_sold', 'avg_product_price'
            ]
            
            # Aggregate sales by region
            region_sales = sales_df.groupby('store_region').agg({
                'total_sale_amount': ['sum', 'mean', 'count'],
                'quantity': 'sum'
            }).reset_index()
            region_sales.columns = [
                'store_region', 'total_regional_sales', 
                'avg_regional_sale', 'total_regional_transactions', 
                'total_regional_units_sold'
            ]
            
            # Customer segment analysis
            segment_sales = sales_df.groupby('customer_segment').agg({
                'total_sale_amount': ['sum', 'mean', 'count'],
                'quantity': 'sum'
            }).reset_index()
            segment_sales.columns = [
                'customer_segment', 'total_segment_sales', 
                'avg_segment_sale', 'total_segment_transactions', 
                'total_segment_units_sold'
            ]
            
            # Use to_sql for efficient loading
            category_sales.to_sql(
                'category_sales', 
                engine, 
                if_exists='replace',  # Replace existing table
                index=False,
                chunksize=1000
            )
            
            region_sales.to_sql(
                'region_sales', 
                engine, 
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            
            segment_sales.to_sql(
                'segment_sales', 
                engine, 
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            
            print("Data successfully processed and loaded to PostgreSQL")
            
            # Optional: return some metrics
            return {
                'total_sales': sales_df['total_sale_amount'].sum(),
                'total_transactions': len(sales_df),
                'unique_categories': sales_df['category'].nunique()
            }
        
        except Exception as e:
            print(f"Error in data processing and loading: {e}")
            raise

    # Define task pipeline
    sales_data = generate_comprehensive_sales_data()
    process_and_load_data(sales_data)

# Instantiate the DAG
retail_sqlalchemy_dag = retail_data_etl()