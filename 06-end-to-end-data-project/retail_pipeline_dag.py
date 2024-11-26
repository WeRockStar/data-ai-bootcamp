# dags/retail_pipeline.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import json

# กำหนดค่า Default Arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Step 1: Extract data from API
def extract_data(**context):
    """Extract retail data from mock API"""
    # สมมติว่าเรามี API ที่ส่งข้อมูลการขายสินค้า
    data = [
        {"date": "2024-01-01", "product_id": "P001", "quantity": 5, "price": 100},
        {"date": "2024-01-01", "product_id": "P002", "quantity": 3, "price": 150},
        {"date": "2024-01-01", "product_id": "P003", "quantity": 2, "price": 200}
    ]
    
    # แปลงเป็น DataFrame
    df = pd.DataFrame(data)
    
    # บันทึกเป็น CSV
    output_path = f"/tmp/retail_data_{context['ds']}.csv"
    df.to_csv(output_path, index=False)
    
    return output_path

# Step 2: Transform data
def transform_data(**context):
    """Transform the extracted data"""
    input_path = context['task_instance'].xcom_pull(task_ids='extract_data')
    df = pd.read_csv(input_path)
    
    # เพิ่ม column ใหม่
    df['total_sales'] = df['quantity'] * df['price']
    df['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # บันทึกไฟล์
    output_path = f"/tmp/transformed_retail_{context['ds']}.csv"
    df.to_csv(output_path, index=False)
    
    return output_path

# สร้าง DAG
with DAG(
    'retail_etl_pipeline',
    default_args=default_args,
    description='A retail ETL pipeline using GCS and BigQuery',
    schedule_interval='@daily'
    ) as dag:


    # สร้าง Tasks
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    # Upload to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src="{{ task_instance.xcom_pull(task_ids='transform_data') }}",
        dst='retail/{{ ds }}/data.csv',
        bucket='your-bucket-name',
        gcp_conn_id='google_cloud_default',
    )

    # Load to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='your-bucket-name',
        source_objects=['retail/{{ ds }}/data.csv'],
        destination_project_dataset_table='your-project.retail_dataset.sales_data',
        schema_fields=[
            {'name': 'date', 'type': 'DATE'},
            {'name': 'product_id', 'type': 'STRING'},
            {'name': 'quantity', 'type': 'INTEGER'},
            {'name': 'price', 'type': 'FLOAT'},
            {'name': 'total_sales', 'type': 'FLOAT'},
            {'name': 'processed_timestamp', 'type': 'TIMESTAMP'}
        ],
        write_disposition='WRITE_APPEND',
        gcp_conn_id='google_cloud_default',
    )

    # Aggregate data in BigQuery
    aggregate_data = BigQueryExecuteQueryOperator(
        task_id='aggregate_data',
        sql="""
        SELECT 
            DATE(date) as sale_date,
            SUM(quantity) as total_quantity,
            SUM(total_sales) as daily_sales
        FROM `your-project.retail_dataset.sales_data`
        WHERE DATE(date) = '{{ ds }}'
        GROUP BY sale_date
        """,
        destination_dataset_table='your-project.retail_dataset.daily_summary',
        write_disposition='WRITE_APPEND',
        gcp_conn_id='google_cloud_default',
    )

    # กำหนด Dependencies
    extract_task >> transform_task >> upload_to_gcs >> load_to_bq >> aggregate_data