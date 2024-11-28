import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dataai',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

PROJECT_ID = "dataaibootcamp"
DATASET_ID = "dataai_name_yyyy"
BUCKET_NAME = "dataai-name-yyyy"

with DAG(
    '09_pg_to_gcs_to_bq_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # สร้างตารางใน BigQuery
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_table',
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id='customer',
        schema_fields=[
            {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        ],
        location='asia-southeast1'
    )

    # ดึงข้อมูลจาก Postgres ไป GCS
    postgres_to_gcs = PostgresToGCSOperator(
        task_id='postgres_to_gcs',
        postgres_conn_id='retail_postgres',
        sql='SELECT * FROM retail.customers',
        bucket=BUCKET_NAME,
        filename='customers/customers.csv',
        export_format='csv',
        gcp_conn_id='google_cloud_default',
        field_delimiter=','
    )

    # โหลดข้อมูลจาก GCS เข้า BigQuery
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=['customers/customers.csv'],
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.customer',
        schema_fields=[
            {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_APPEND',
        source_format='CSV',
        field_delimiter=',',
        skip_leading_rows=1,
        location='asia-southeast1'
    )

    create_table >> postgres_to_gcs >> gcs_to_bigquery
