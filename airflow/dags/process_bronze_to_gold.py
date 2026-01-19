from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import sys

# Import the registration function - ingestion is mounted at /opt/ingestion
sys.path.append('/opt/ingestion/src')  # Add the src directory directly to path
from register_iceberg_trino_table import register_iceberg_trino_table

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")

with DAG(
    'process_bronze_to_gold',
    start_date=days_ago(1),
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    register_task = PythonOperator(
        task_id='register_bronze_files',
        python_callable=register_iceberg_trino_table,
        op_kwargs={
            "minio_endpoint": MINIO_ENDPOINT,
            "minio_access_key": MINIO_ACCESS_KEY,
            "minio_secret_key": MINIO_SECRET_KEY,
            "trino_host": TRINO_HOST,
            "trino_port": TRINO_PORT,
            "trino_user": TRINO_USER,
        }
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/dbt/meterdata && dbt run --profile meterdata --target test',
    )

    register_task >> dbt_run
