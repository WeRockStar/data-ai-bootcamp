import json
import random
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

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
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='gcs_datalake_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['gcs', 'datalake', 'etl', 'google_cloud']
)
def gcs_datalake_pipeline():
    @task()
    def generate_ecommerce_data():
        """
        Generate comprehensive e-commerce dataset 
        with multiple dimensions and granular details
        """
        # Product categories with advanced metadata
        product_categories = [
            {
                'category': 'Electronics',
                'subcategories': ['Smartphones', 'Laptops', 'Accessories'],
                'brands': ['Apple', 'Samsung', 'Dell', 'Sony', 'Lenovo'],
                'price_range': (100, 2000)
            },
            {
                'category': 'Fashion',
                'subcategories': ['Mens Wear', 'Womens Wear', 'Kids Wear'],
                'brands': ['Nike', 'Adidas', 'Zara', 'H&M', 'Uniqlo'],
                'price_range': (20, 500)
            },
            {
                'category': 'Home & Living',
                'subcategories': ['Furniture', 'Decor', 'Kitchen'],
                'brands': ['IKEA', 'Wayfair', 'Williams Sonoma', 'Pottery Barn'],
                'price_range': (50, 1000)
            }
        ]

        # Generate transaction data
        transactions = []
        for _ in range(5000):  # 5000 transactions
            # Select category and product details
            category_info = random.choice(product_categories)
            
            # Transaction details
            transaction = {
                'transaction_id': fake.uuid4(),
                'timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
                
                # Product Details
                'product': {
                    'category': category_info['category'],
                    'subcategory': random.choice(category_info['subcategories']),
                    'brand': random.choice(category_info['brands']),
                    'price': round(random.uniform(*category_info['price_range']), 2)
                },
                
                # Customer Details
                'customer': {
                    'customer_id': fake.uuid4(),
                    'name': fake.name(),
                    'email': fake.email(),
                    'segment': random.choice([
                        'Individual', 'Business', 'Enterprise', 'Wholesale'
                    ])
                },
                
                # Transaction Specifics
                'transaction_details': {
                    'quantity': random.randint(1, 10),
                    'total_amount': 0,  # Will be calculated
                    'payment_method': random.choice([
                        'Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer'
                    ])
                },
                
                # Contextual Information
                'context': {
                    'channel': random.choice(['Online', 'Mobile', 'In-Store']),
                    'location': {
                        'country': fake.country(),
                        'city': fake.city(),
                        'timezone': fake.timezone()
                    }
                }
            }
            
            # Calculate total amount
            transaction['transaction_details']['total_amount'] = round(
                transaction['product']['price'] * transaction['transaction_details']['quantity'], 
                2
            )
            
            transactions.append(transaction)
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(transactions)
        
        return df

    @task()
    def prepare_and_upload_to_gcs(df):
        """
        Prepare data and upload to Google Cloud Storage
        """
        # Get GCS Hook
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        
        # Define bucket and path
        bucket_name = 'your-datalake-bucket'
        base_path = 'raw/ecommerce/transactions'
        
        # Prepare filename with timestamp
        filename = f"{base_path}/transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        
        # Convert DataFrame to JSON Lines format
        jsonl_data = df.to_json(orient='records', lines=True)
        
        try:
            # Upload to GCS
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=filename,
                data=jsonl_data.encode('utf-8')
            )
            
            # Optional: Create metadata file
            metadata = {
                'upload_timestamp': datetime.now().isoformat(),
                'total_records': len(df),
                'file_path': filename,
                'schema': df.dtypes.to_dict()
            }
            
            # Upload metadata
            metadata_filename = f"{base_path}/metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=metadata_filename,
                data=json.dumps(metadata, indent=2).encode('utf-8')
            )
            
            print(f"Successfully uploaded {len(df)} records to GCS")
            
            return {
                'bucket': bucket_name,
                'file_path': filename,
                'total_records': len(df)
            }
        
        except Exception as e:
            print(f"Error uploading to GCS: {e}")
            raise

    @task()
    def aggregate_and_transform_data(gcs_file_info):
        """
        Prepare SQL for BigQuery transformations
        """
        # BigQuery SQL for transformations
        transformation_query = f"""
        WITH raw_transactions AS (
            SELECT * FROM `{gcs_file_info['bucket']}.ecommerce_dataset.raw_transactions`
        ),
        category_aggregation AS (
            SELECT 
                JSON_EXTRACT_SCALAR(product, '$.category') AS category,
                JSON_EXTRACT_SCALAR(product, '$.subcategory') AS subcategory,
                JSON_EXTRACT_SCALAR(product, '$.brand') AS brand,
                COUNT(*) AS total_transactions,
                SUM(CAST(JSON_EXTRACT_SCALAR(transaction_details, '$.total_amount') AS FLOAT64)) AS total_revenue,
                AVG(CAST(JSON_EXTRACT_SCALAR(transaction_details, '$.quantity') AS INT64)) AS avg_quantity
            FROM raw_transactions
            GROUP BY 
                JSON_EXTRACT_SCALAR(product, '$.category'),
                JSON_EXTRACT_SCALAR(product, '$.subcategory'),
                JSON_EXTRACT_SCALAR(product, '$.brand')
        ),
        customer_segmentation AS (
            SELECT 
                JSON_EXTRACT_SCALAR(customer, '$.segment') AS customer_segment,
                COUNT(*) AS total_transactions,
                SUM(CAST(JSON_EXTRACT_SCALAR(transaction_details, '$.total_amount') AS FLOAT64)) AS total_segment_revenue
            FROM raw_transactions
            GROUP BY JSON_EXTRACT_SCALAR(customer, '$.segment')
        )
        SELECT 
            ca.category,
            ca.subcategory,
            ca.brand,
            ca.total_transactions,
            ca.total_revenue,
            ca.avg_quantity,
            cs.customer_segment,
            cs.total_segment_revenue
        FROM category_aggregation ca
        CROSS JOIN customer_segmentation cs
        """
        
        return transformation_query

    # BigQuery load operator
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_raw_data_to_bigquery',
        bucket='your-datalake-bucket',
        source_objects=['raw/ecommerce/transactions/*.jsonl'],
        destination_project_dataset_table='your-project.ecommerce_dataset.raw_transactions',
        schema_fields=[
            {'name': 'transaction_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'product', 'type': 'RECORD', 'mode': 'NULLABLE'},
            {'name': 'customer', 'type': 'RECORD', 'mode': 'NULLABLE'},
            {'name': 'transaction_details', 'type': 'RECORD', 'mode': 'NULLABLE'},
            {'name': 'context', 'type': 'RECORD', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        source_format='NEWLINE_DELIMITED_JSON'
    )

    # BigQuery transformation operator
    transform_data = BigQueryInsertJobOperator(
        task_id='transform_data_in_bigquery',
        configuration={
            'query': {
                'query': '{{ task_instance.xcom_pull(task_ids="aggregate_and_transform_data") }}',
                'destinationTable': {
                    'projectId': 'your-project',
                    'datasetId': 'ecommerce_dataset',
                    'tableId': 'transformed_analytics'
                },
                'createDisposition': 'CREATE_IF_NEEDED',
                'writeDisposition': 'WRITE_TRUNCATE'
            }
        }
    )

    # Define task pipeline
    data = generate_ecommerce_data()
    gcs_file = prepare_and_upload_to_gcs(data)
    
    # Set up task dependencies
    gcs_file >> load_to_bigquery
    gcs_file >> aggregate_and_transform_data() >> transform_data

# Instantiate the DAG
gcs_datalake_dag = gcs_datalake_pipeline()