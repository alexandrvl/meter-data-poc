from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'ingest_meter_data',
    default_args=default_args,
    description='Generates and ingests meter data to MinIO Bronze bucket',
    schedule_interval='*/15 * * * *', # Run every 15 minutes
    catchup=False,
) as dag:

    # Use BashOperator to run the script inside the data_venv
    # This avoids importing 'duckdb' in the main scheduler process (which causes ModuleNotFoundError)
    ingest_task = BashOperator(
        task_id='ingest_data_to_bronze',
        bash_command='python /opt/ingestion/src/ingest.py',
        env={
            "MINIO_ENDPOINT": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
            "MINIO_ROOT_USER": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
            "MINIO_ROOT_PASSWORD": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
             # Explicitly pass these if they aren't in the global env or if ingest.py needs them
            "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        }
    )

    ingest_task
