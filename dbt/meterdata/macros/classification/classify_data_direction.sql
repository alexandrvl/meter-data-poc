{% macro classify_data_direction(column_name) %}
    CASE
        WHEN {{ column_name }} = 'IMPORT' THEN 1
        WHEN {{ column_name }} = 'EXPORT' THEN 2
        ELSE 0
    END
{% endmacro %}
