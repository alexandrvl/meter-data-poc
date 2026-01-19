import os
import logging
import duckdb
import trino
import json
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "iceberg")
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "bronze")
TABLE_NAME = os.getenv("ICEBERG_TABLE_NAME", "meter_data_raw")

def table_exists(trino_client):
    """
    Check if the Iceberg table exists in Trino.
    """
    try:
        query = f"""
            SHOW TABLES FROM "{CATALOG_NAME}"."{NAMESPACE}"
            LIKE '{TABLE_NAME}'
        """
        logger.info(f"Checking if table {CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME} exists...")
        cursor = trino_client.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return len(result) > 0
    except Exception as e:
        logger.error(f"Error checking table existence: {e}")
    return False

def get_first_parquet_file(minio_endpoint, minio_access_key, minio_secret_key):
    """
    Get the first Parquet file from the raw directory in MinIO.
    """
    con = duckdb.connect(database=':memory:', read_only=False)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{minio_endpoint.replace('http://', '')}';")
    con.execute(f"SET s3_access_key_id='{minio_access_key}';")
    con.execute(f"SET s3_secret_access_key='{minio_secret_key}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    
    try:
        s3_raw_path = os.getenv("ICEBERG_RAW_PATH", "s3://bronze/raw/*/*.parquet")
        logger.info(f"Scanning for Parquet files in {s3_raw_path}...")
        result = con.sql(f"SELECT distinct filename FROM read_parquet('{s3_raw_path}', filename = true);").fetchall()
        
        if not result:
            logger.warning("No Parquet files found in the raw directory")
            return None
            
        logger.info(f"Found {len(result)} Parquet files. Using first file for schema inference.")
        return result[0][0]
        
    except Exception as e:
        logger.error(f"Error getting first Parquet file: {e}")
        con.close()
        raise
    finally:
        con.close()

def extract_schema_from_parquet(file_path, minio_endpoint, minio_access_key, minio_secret_key):
    """
    Extract schema from a Parquet file stored in MinIO.
    """
    logger.info(f"Extracting schema from file: {file_path}")
    
    # Configure S3FileSystem for MinIO
    scheme = 'https' if minio_endpoint.startswith('https://') else 'http'
    endpoint = minio_endpoint.replace('http://', '').replace('https://', '')
    s3 = fs.S3FileSystem(
        endpoint_override=endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        region=os.getenv("S3_REGION", "us-east-1"),
        scheme=scheme
    )

    if file_path.startswith('s3://'):
        file_path = file_path.replace('s3://', '')

    pa_schema = pq.read_schema(file_path, filesystem=s3)

    # Convert timestamp[ns] to timestamp[us] since Trino/Iceberg typically use microsecond precision
    converted_fields = []
    for field in pa_schema:
        if field.type == pa.timestamp('ns'):
            converted_fields.append(pa.field(field.name, pa.timestamp('us', tz='UTC')))
        else:
            converted_fields.append(field)
    
    converted_pa_schema = pa.schema(converted_fields)
    logger.info(f"Extracted schema: {converted_pa_schema}")
    
    return converted_pa_schema

def create_iceberg_table(trino_client, pa_schema, table_location):
    """
    Create a new Iceberg table using Trino with the given schema.
    """
    # Convert PyArrow schema to Trino SQL schema
    trino_fields = []
    for field in pa_schema:
        field_name = field.name
        field_type = field.type
        
        if pa.types.is_int8(field_type):
            trino_type = "TINYINT"
        elif pa.types.is_int16(field_type):
            trino_type = "SMALLINT"
        elif pa.types.is_int32(field_type):
            trino_type = "INTEGER"
        elif pa.types.is_int64(field_type):
            trino_type = "BIGINT"
        elif pa.types.is_float32(field_type):
            trino_type = "REAL"
        elif pa.types.is_float64(field_type):
            trino_type = "DOUBLE"
        elif pa.types.is_boolean(field_type):
            trino_type = "BOOLEAN"
        elif pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
            trino_type = "VARCHAR"
        elif pa.types.is_timestamp(field_type):
            if field_type.tz is not None:
                trino_type = "TIMESTAMP(6) WITH TIME ZONE"
            else:
                trino_type = "TIMESTAMP(6)"
        elif pa.types.is_date(field_type):
            trino_type = "DATE"
        elif pa.types.is_time(field_type):
            trino_type = "TIME"
        elif pa.types.is_binary(field_type) or pa.types.is_large_binary(field_type):
            trino_type = "VARBINARY"
        else:
            logger.warning(f"Unsupported type: {field_type}, defaulting to VARCHAR")
            trino_type = "VARCHAR"
        
        trino_fields.append(f'"{field_name}" {trino_type}')
    
    fields_sql = ", ".join(trino_fields)

    schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS "{CATALOG_NAME}"."{NAMESPACE}"
    """
    
    query = f"""
        CREATE TABLE "{CATALOG_NAME}"."{NAMESPACE}"."{TABLE_NAME}" (
            {fields_sql}
        ) WITH (
            format = 'PARQUET'
        )
    """
    
    logger.info(f"Creating Iceberg table with query: {query}")
    cursor = trino_client.cursor()
    cursor.execute(schema_sql)
    cursor.execute(query)
    logger.info(f"Successfully created Iceberg table: {CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}")

def get_registered_folders(trino_client):
    """
    Get list of already registered ingestion folders from the Iceberg table's metadata.
    
    This function uses Trino to query the Iceberg table's metadata to find
    which ingestion folders have already been added.
    """
    registered_folders = set()
    
    try:
        # Query Iceberg's metadata to get all files in the table
        query = f"""
            SELECT "$path" 
            FROM "{CATALOG_NAME}"."{NAMESPACE}"."{TABLE_NAME}"
        """
        
        logger.info(f"Querying registered files from Iceberg table...")
        cursor = trino_client.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        for row in results:
            file_path = row[0]
            # Extract ingestion folder from file path (format: s3://bronze/raw/timestamp_uuid/filename.parquet)
            if "raw/" in file_path:
                folder_part = file_path.split("raw/")[1].split("/")[0]
                registered_folders.add(folder_part)
        
        logger.info(f"Found {len(registered_folders)} registered ingestion folders")
        
    except Exception as e:
        logger.warning(f"Error getting registered folders: {e}")
        logger.info("Assuming table does not exist, returning empty registered folders set")
        return set()
        
    return registered_folders

def get_all_ingestion_folders(minio_endpoint, minio_access_key, minio_secret_key):
    """
    Get all ingestion folders from MinIO's raw directory using DuckDB.
    """
    all_folders = set()
    
    # Configure DuckDB for S3 access
    con = duckdb.connect(database=':memory:', read_only=False)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{minio_endpoint.replace('http://', '')}';")
    con.execute(f"SET s3_access_key_id='{minio_access_key}';")
    con.execute(f"SET s3_secret_access_key='{minio_secret_key}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    
    try:
        # List all Parquet files in raw directory
        s3_raw_path = os.getenv("ICEBERG_RAW_PATH", "s3://bronze/raw/*/*.parquet")
        logger.info(f"Scanning for ingestion folders in {s3_raw_path}...")
        result = con.sql(f"SELECT distinct filename FROM read_parquet('{s3_raw_path}', filename = true);").fetchall()
        
        for row in result:
            file_path = row[0]
            # Extract ingestion folder from file path
            if "raw/" in file_path:
                folder_part = file_path.split("raw/")[1].split("/")[0]
                all_folders.add(folder_part)
        
        logger.info(f"Found {len(all_folders)} ingestion folders in MinIO")
        
    except Exception as e:
        logger.error(f"Error getting ingestion folders from MinIO: {e}")
        con.close()
        raise
        
    con.close()
    return all_folders

def register_iceberg_trino_table(minio_endpoint, minio_access_key, minio_secret_key, trino_host=None, trino_port=None, trino_user=None):
    """
    Registers new ingestion folders to Iceberg table using Trino's add_files procedure.
    
    This function:
    1. Connects to Trino
    2. Checks if Iceberg table exists
    3. If table doesn't exist:
       - Finds first Parquet file in S3
       - Extracts schema from Parquet file
       - Creates new Iceberg table with the extracted schema
    4. Finds all ingestion folders in MinIO
    5. Determines which folders haven't been registered yet
    6. Uses Trino's add_files procedure to register new folders
    """
    # Configure Trino connection
    trino_host = trino_host or os.getenv("TRINO_HOST", "trino")
    trino_port = trino_port or int(os.getenv("TRINO_PORT", "8080"))
    trino_user = trino_user or os.getenv("TRINO_USER", "admin")
    
    logger.info(f"Connecting to Trino at {trino_host}:{trino_port} as {trino_user}")
    trino_client = trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user=trino_user,
        catalog=CATALOG_NAME,
        schema=NAMESPACE
    )
    
    try:
        # Check if table exists, and create it if necessary
        if not table_exists(trino_client):
            logger.info("Table does not exist. Creating new Iceberg table...")
            
            # Find first Parquet file in S3
            first_parquet_file = get_first_parquet_file(minio_endpoint, minio_access_key, minio_secret_key)
            
            if first_parquet_file is None:
                logger.warning("No Parquet files found in raw directory. Exiting.")
                trino_client.close()
                return
                
            # Extract schema from Parquet file
            pa_schema = extract_schema_from_parquet(
                first_parquet_file,
                minio_endpoint,
                minio_access_key,
                minio_secret_key
            )
            
            # Create table location
            s3_warehouse_path = os.getenv("ICEBERG_WAREHOUSE_PATH", "s3a://bronze/warehouse")
            table_location = f"{s3_warehouse_path}/{NAMESPACE}.db/{TABLE_NAME}"
            
            # Create Iceberg table
            create_iceberg_table(trino_client, pa_schema, table_location)
            
        # Get registered folders from Iceberg table
        registered_folders = get_registered_folders(trino_client)
        
        # Get all ingestion folders from MinIO
        all_folders = get_all_ingestion_folders(minio_endpoint, minio_access_key, minio_secret_key)
        
        # Determine new folders to register
        new_folders = all_folders - registered_folders
        logger.info(f"Found {len(new_folders)} new ingestion folders to register")
        
        if not new_folders:
            logger.info("No new folders to register. Exiting.")
            trino_client.close()
            return
            
        # Register new folders using Trino's add_files procedure
        cursor = trino_client.cursor()
        
        for folder in new_folders:
            # Create the location to the ingestion folder
            folder_location = f"s3://bronze/raw/{folder}/"
            logger.info(f"Registering folder: {folder_location}")
            
            # Use Trino's add_files procedure
            # Reference: https://trino.io/docs/current/sql/procedures/iceberg.html#add_files
            try:
                query = f"""
                    ALTER TABLE "{CATALOG_NAME}"."{NAMESPACE}"."{TABLE_NAME}" 
                    EXECUTE add_files(
                        location => '{folder_location}',
                        format => 'PARQUET'
                    )
                """
                
                logger.info(f"Executing query: {query}")
                cursor.execute(query)
                result = cursor.fetchall()
                
                logger.info(f"Successfully registered folder: {folder}")
                
            except Exception as e:
                logger.error(f"Error registering folder {folder}: {e}")
                continue
        
        logger.info("Registration process finished.")
        
    except Exception as e:
        logger.error(f"Error in registration process: {e}")
        raise
        
    finally:
        trino_client.close()

if __name__ == "__main__":
    # For local testing
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
    MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password")
    
    register_iceberg_trino_table(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
