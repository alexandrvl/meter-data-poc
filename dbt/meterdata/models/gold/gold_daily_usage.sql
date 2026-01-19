WITH silver_data AS (
    SELECT * FROM {{ ref('silver_meter_data') }}
)

SELECT
    country,
    date(measurementTime) as reading_date,
    usage_class,
    count(*) as record_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
FROM silver_data
GROUP BY 1, 2, 3
