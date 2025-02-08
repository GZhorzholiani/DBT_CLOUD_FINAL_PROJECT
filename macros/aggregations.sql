{% macro count_distinct(column_name) %}
    COUNT(DISTINCT {{ column_name }})
{% endmacro %}

{% macro min_timestamp(column_name) %}
    MIN({{ column_name }})
{% endmacro %}

{% macro max_timestamp(column_name) %}
    MAX({{ column_name }})
{% endmacro %}
