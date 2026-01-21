{% macro classify_country(column_name) %}
    CASE
        WHEN {{ column_name }} = 'EST' THEN 1
        WHEN {{ column_name }} = 'LAT' THEN 2
        WHEN {{ column_name }} = 'LIT' THEN 3
        ELSE 0
    END
{% endmacro %}
