{{ config(
    materialized='incremental',
    schema='silver',
    unique_key=['objectId', 'measurementTime'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    properties = {
        "format": "'PARQUET'",
        "partitioning": ["day(measurementTime)", "bucket(objectId, 20)"],
        "sorted_by": ["objectId", "measurementTime"],
        "max_commit_retry": "100"
    },
    incremental_predicates=["DBT_INTERNAL_DEST.measurementTime >= current_date - interval '14' day"],
    post_hook=[
        "ALTER TABLE {{ this }} EXECUTE optimize(file_size_threshold => '128MB')",
        "ALTER TABLE {{ this }} EXECUTE expire_snapshots(retention_threshold => '7d')"
    ]
) }}

WITH source_data AS (
    SELECT
        objectId,
        customerIdentifier,
        country,
        dataDirection,
        intervalgranylarity,
        dataQuality,
        measurementTime,
        readingTime,
        systemTime,
        amount
    FROM {{ source('iceberg', 'meter_data_raw') }}

    {% if is_incremental() %}
    WHERE systemTime > (select max(systemTime) from {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY objectId, measurementTime
            ORDER BY systemTime DESC
        ) as rn
    FROM source_data
)

SELECT
    objectId,
    customerIdentifier,
    country,
    measurementTime,
    readingTime,
    systemTime,
    amount,
    {{ classify_data_direction('dataDirection') }} as dataDirection_id,
    {{ classify_country('country') }} as country_id,
    {{ classify_interval_granularity('intervalgranylarity') }} as intervalGranularity_id,
    {{ classify_data_quality('dataQuality') }} as dataQuality_id,
    {{ classify_amount('amount') }} as usage_class
FROM deduplicated
WHERE rn = 1
