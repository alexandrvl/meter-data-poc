import duckdb
import pandas as pd
import os
import uuid
import datetime
import random
import boto3
from botocore.client import Config

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password")
BUCKET_NAME = "bronze"

def generate_data(num_records=100):
    """Generates synthetic meter data."""
    data = []
    
    countries = ["EST", "LAT", "LIT"]
    directions = ["IMPORT", "EXPORT"]
    granularities = ["PT15M", "PT1H"]
    qualities = ["VALID", "ESTIMATED", "INVALID"]

    for _ in range(num_records):
        now = datetime.datetime.now()
        reading_time = now - datetime.timedelta(minutes=random.randint(0, 60))
        measurement_time = reading_time - datetime.timedelta(minutes=random.randint(0, 15))
        
        record = {
            "objectId": str(uuid.uuid4()),
            "customerIdentifier": f"CUST-{random.randint(1000, 9999)}",
            "country": random.choice(countries),
            "dataDirection": random.choice(directions),
            "intervalGranylarity": random.choice(granularities),
            "dataQuality": random.choice(qualities),
            "measurementTime": measurement_time,
            "readingTime": reading_time,
            "systemTime": now,
            "amount": round(random.uniform(0.0, 100.0), 3)
        }
        data.append(record)
    return data

def ingest_data():
    """Generates data and writes to MinIO via DuckDB."""
    print("Generating data...")
    data = generate_data(1000)
    data = pd.DataFrame(data)
    
    # Convert timestamps to UTC and ensure microsecond precision (pyiceberg doesn't support nanosecond)
    for col in ['measurementTime', 'readingTime', 'systemTime']:
        data[col] = pd.to_datetime(data[col], utc=True).dt.round('us')
    
    # Create ingestion path based on timestamp and UUID for uniqueness
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    ingestion_uuid = str(uuid.uuid4())
    ingestion_folder = f"{timestamp}_{ingestion_uuid}"
    filename = f"meter_data_{timestamp}.parquet"
    local_path = f"/tmp/{filename}"
    s3_path = f"s3://{BUCKET_NAME}/raw/{ingestion_folder}/{filename}"

    print(f"Writing {len(data)} records to DuckDB...")
    con = duckdb.connect()
    
    # Register data as a table
    con.execute("CREATE TABLE meter_data AS SELECT * FROM data")
    
    # Write to local parquet first (DuckDB S3 support can be tricky with custom endpoints sometimes, 
    # but let's try direct S3 write if possible. For simplicity/robustness in this POC, 
    # we'll save local and upload with boto3 or use duckdb httpfs)
    
    # Setup DuckDB for S3
    ext_dir = "/tmp/duckdb_extensions"
    os.makedirs(ext_dir, exist_ok=True)

    con.execute(f"SET extension_directory='{ext_dir}';")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT.replace('http://', '')}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    
    print(f"Copying to {s3_path}...")
    try:
        con.execute(f"COPY meter_data TO '{s3_path}' (FORMAT PARQUET);")
        print("Upload successful via DuckDB.")
    except Exception as e:
        print(f"DuckDB direct upload failed: {e}")
        print("Fallback: Writing local and uploading via boto3")
        con.execute(f"COPY meter_data TO '{local_path}' (FORMAT PARQUET);")
        
        s3 = boto3.client('s3',
                          endpoint_url=MINIO_ENDPOINT,
                          aws_access_key_id=MINIO_ACCESS_KEY,
                          aws_secret_access_key=MINIO_SECRET_KEY,
                          config=Config(signature_version='s3v4'))
                          
        with open(local_path, "rb") as f:
            s3.upload_fileobj(f, BUCKET_NAME, f"raw/{filename}")
        print("Upload successful via Boto3.")
        os.remove(local_path)

if __name__ == "__main__":
    # Ensure bucket exists
    try:
        s3 = boto3.client('s3',
                          endpoint_url=MINIO_ENDPOINT,
                          aws_access_key_id=MINIO_ACCESS_KEY,
                          aws_secret_access_key=MINIO_SECRET_KEY)
        s3.create_bucket(Bucket=BUCKET_NAME)
    except Exception:
        pass # Bucket might already exist
        
    ingest_data()
