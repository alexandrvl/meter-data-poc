{{ config(
    materialized='incremental',
    unique_key='objectId',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('iceberg', 'meter_data_raw') }}
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY objectId ORDER BY systemTime DESC) as rn
    FROM source_data
)

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
    amount,
    CASE 
        WHEN amount > 50 THEN 'High'
        WHEN amount > 20 THEN 'Medium'
        ELSE 'Low'
    END as usage_class
FROM deduplicated
WHERE rn = 1

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  -- (uses systemTime to find new records)
  AND systemTime > (select max(systemTime) from {{ this }})
{% endif %}
