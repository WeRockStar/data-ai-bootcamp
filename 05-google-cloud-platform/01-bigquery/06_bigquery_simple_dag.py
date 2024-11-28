import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dataai',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}
PROJECT_ID = "dataaibootcamp"
DATASET_ID = "dataai_name_yyyy"

with DAG(
    '06_bigquery_simple_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # สร้าง Task สำหรับ Query ข้อมูลจาก BigQuery
    query_task = BigQueryExecuteQueryOperator(
        task_id='query_house_price',
        sql="""
            SELECT 
                full_name,
                age
            FROM `dataaibootcamp.dataai_test_2024.mock_customer` 
            LIMIT 100
        """,
        use_legacy_sql=False,
        location='asia-southeast1'
    )

    query_task
