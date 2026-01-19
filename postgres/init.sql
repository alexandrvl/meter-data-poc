GRANT ALL PRIVILEGES ON DATABASE iceberg TO admin;


CREATE TABLE public.iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(5500),
    property_value VARCHAR(5500),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

CREATE TABLE public.iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(5500),
    previous_metadata_location VARCHAR(5500),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);