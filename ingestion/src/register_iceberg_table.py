import os
import logging
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table.name_mapping import MappedField
from pyiceberg.io.pyarrow import pyarrow_to_schema
import pyarrow.parquet as pq
import pyarrow.fs as fs


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "iceberg")
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "bronze")
TABLE_NAME = os.getenv("ICEBERG_TABLE_NAME", "meter_data_raw")

def register_files(minio_endpoint, minio_access_key, minio_secret_key, trino_host=None, trino_port=None, trino_user=None):
    """
    Scans MinIO bronze bucket for new files and registers them to Iceberg table using PyIceberg.
    """
    # 1. Configure DuckDB for S3 access
    con = duckdb.connect(database=':memory:', read_only=False)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{minio_endpoint.replace('http://', '')}';")
    con.execute(f"SET s3_access_key_id='{minio_access_key}';")
    con.execute(f"SET s3_secret_access_key='{minio_secret_key}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';") # Important for MinIO

    # 2. Configure Iceberg catalog
    s3_warehouse_path = os.getenv("ICEBERG_WAREHOUSE_PATH", "s3a://bronze/warehouse") # Corresponds to the 'warehouse' directory in MinIO
    catalog = load_catalog(
        CATALOG_NAME,
        **{
            "type": "sql",
            "uri": f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
            "warehouse": s3_warehouse_path,
            "s3.endpoint": minio_endpoint,
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
            "s3.path-style-access": "true",
            "s3.region": os.getenv("S3_REGION", "us-east-1") # Assuming a default region
        }
    )

    # 3. Ensure namespace exists
    logger.info(f"Ensuring Iceberg namespace '{NAMESPACE}' exists...")
    catalog.create_namespace_if_not_exists(NAMESPACE)

    # 4. List Parquet files in the 'raw/' directory
    s3_raw_path = os.getenv("ICEBERG_RAW_PATH", "s3://bronze/raw/*.parquet")
    logger.info(f"Scanning for parquet files in {s3_raw_path}...")
    try:
        result = con.sql(f"SELECT distinct filename FROM read_parquet('{s3_raw_path}', filename = true);").fetchall()
        all_files = [row[0] for row in result]
        logger.info(f"Found {len(all_files)} parquet files in the raw directory.")
    except Exception as e:
        logger.error(f"Error listing files with DuckDB: {e}")
        con.close()
        raise

    if not all_files:
        logger.info("No new files to process. Exiting.")
        con.close()
        return


    # 5. Load or create the Iceberg table
    table_identifier = (NAMESPACE, TABLE_NAME)
    table_location = f"{s3_warehouse_path}/{NAMESPACE}.db/{TABLE_NAME}" # Consistent with PyIceberg's default

    first_file_path = all_files[0]
    logger.info(f"Using file '{first_file_path}' to infer schema for new table.")

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

    if first_file_path.startswith('s3://'):
        first_file_path = first_file_path.replace('s3://', '')

    pa_schema = pq.read_schema(first_file_path, filesystem=s3)

    # Convert timestamp[ns] to timestamp[us] since pyiceberg doesn't support nanosecond precision
    # Also, for time zone handling, we should use timestamptz (timestamp with time zone)
    converted_fields = []
    for field in pa_schema:
        if field.type == pa.timestamp('ns'):
            # Convert to microsecond precision (pyiceberg supports us) and make it timestamptz
            converted_fields.append(pa.field(field.name, pa.timestamp('us', tz='UTC')))
        else:
            converted_fields.append(field)
    
    converted_pa_schema = pa.schema(converted_fields)

    mapped_fields = []

    for i in range(len(converted_pa_schema.names)):
        mapped_fields.append(MappedField(field_id=i, names=[converted_pa_schema.names[i]]))

    iceberg_schema = pyarrow_to_schema(converted_pa_schema, mapped_fields)
    logger.info(f"Inferred Iceberg schema: {iceberg_schema}")

    table = catalog.create_table_if_not_exists(
        identifier=table_identifier,
        schema=iceberg_schema,
        location=table_location,
        properties={
            "write.format.default": "parquet"
        }
    )

    logger.info(f"Created Iceberg table '{NAMESPACE}.{TABLE_NAME}' at {table_location}")


    # 6. Determine and register new files
    try:
        # Get files already registered in the Iceberg table
        registered_files = set()
        # A scan can be expensive, but it's reliable for getting file lists.
        # For very large tables, a different approach might be needed.
        tasks = table.scan().plan_files()
        for task in tasks:
            registered_files.add(task.file.file_path)
        logger.info(f"Found {len(registered_files)} files already registered in the Iceberg table.")

        new_files_to_register = [f for f in all_files if f not in registered_files]
        logger.info(f"Found {len(new_files_to_register)} new files to register.")

    except Exception as e:
        logger.error(f"Error determining new files: {e}")
        con.close()
        raise

    if new_files_to_register:
        logger.info(f"Adding {len(new_files_to_register)} new files to Iceberg table")
        try:
            table.add_files(new_files_to_register)
        except Exception as e:
            logger.error(f"Error registering files to Iceberg table: {e}")
            con.close()
            raise
    else:
        logger.info(f"No new files to add (table up to date)")
    logger.info("Registration process finished.")

    con.close()

if __name__ == "__main__":
    # For local testing
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
    MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password")
    ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://localhost:8181") # Default for local testing
    os.environ['PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE'] = 'true'
    register_files(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
