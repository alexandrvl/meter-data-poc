{% macro classify_interval_granularity(column_name) %}
    CASE
        WHEN {{ column_name }} = 'PT15M' THEN 1
        WHEN {{ column_name }} = 'PT1H' THEN 2
        ELSE 0
    END
{% endmacro %}
