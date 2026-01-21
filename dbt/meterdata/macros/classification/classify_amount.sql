{% macro classify_amount(column_name) %}
    CASE
        WHEN {{ column_name }} > 50 THEN 'High'
        WHEN {{ column_name }} > 20 THEN 'Medium'
        ELSE 'Low'
    END
{% endmacro %}
