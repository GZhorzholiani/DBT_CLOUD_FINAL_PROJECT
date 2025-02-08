{% macro left_join(left_table, right_table, left_key, right_key) %}
LEFT JOIN {{ right_table }} AS {{ right_table }} ON {{ left_table }}.{{ left_key }} = {{ right_table }}.{{ right_key }}
{% endmacro %}
