{% macro classify_data_quality(column_name) %}
    CASE
        WHEN {{ column_name }} = 'VALID' THEN 1
        WHEN {{ column_name }} = 'ESTIMATED' THEN 2
        WHEN {{ column_name }} = 'INVALID' THEN 3
        ELSE 0
    END
{% endmacro %}
