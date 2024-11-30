from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    'bigquery_to_xcom_dag',
    default_args=default_args,
    description='Run a BigQuery query and return result to XCom',
    schedule_interval=None,
    catchup=False
) as dag:

    # Define the BigQuery SQL query
    query = """
    SELECT 
      DATE(order_items.created_at) AS order_date,
      SUM(order_items.sale_price) AS total_revenue
    FROM 
      `bigquery-public-data.thelook_ecommerce.order_items` AS order_items
    JOIN 
      `bigquery-public-data.thelook_ecommerce.orders` AS orders 
      ON order_items.order_id = orders.order_id
    WHERE 
      DATE(order_items.created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
    GROUP BY 
      order_date
    ORDER BY 
      order_date ASC;
    """

    # Define the BigQuery task
    query_bigquery = BigQueryExecuteQueryOperator(
        task_id='query_bigquery',
        sql=query,
        use_legacy_sql=False,  # Ensure to use Standard SQL
        location='US',  # Set the location of your BigQuery dataset
        gcp_conn_id="gcp_conn",
        xcom_push=True  # Push query results to XCom
    )

    # Define a Python function to process the results
    def process_query_results(**kwargs):
        # Pull results from XCom
        records = kwargs['ti'].xcom_pull(task_ids='query_bigquery')
        print("Query Results:", records)
        # Additional processing logic can be implemented here

    # Define a Python task to process results
    process_results = PythonOperator(
        task_id='process_results',
        python_callable=process_query_results,
        provide_context=True,
    )

    # Set task dependencies
    query_bigquery >> process_results
