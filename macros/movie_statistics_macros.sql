{% macro clean_price(price_column) %}
    CASE 
        WHEN {{ price_column }} IS NULL OR {{ price_column }} = '' THEN NULL  
        ELSE TRY_CAST(REPLACE(REPLACE({{ price_column }}, '$', ''), ',', '') AS FLOAT)  
    END
{% endmacro %}

{% macro join_rating_mapping(movie_table, rating_table, movie_rating_column, mapping_rating_column) %}
    LEFT JOIN {{ rating_table }} rm
    ON {{ movie_table }}.{{ movie_rating_column }} = rm.{{ mapping_rating_column }}
{% endmacro %}

{% macro left_join_movies(table1, column1, table2, column2) %}
    LEFT JOIN {{ table2 }} AS {{ table2 }}
    ON {{ table1 }}.{{ column1 }} = {{ table2 }}.{{ column2 }}
{% endmacro %}

{% macro sum_column_movies(case_when_condition, column_name) %}
    SUM(CASE 
        WHEN {{ case_when_condition }} THEN {{ column_name }}
        ELSE 0 
    END)
{% endmacro %}

{% macro count_distinct_column(column_name) %}
    COUNT(DISTINCT {{ column_name }})
{% endmacro %}

{% macro create_rating_mapping() %}
WITH source AS (
    SELECT DISTINCT AUDIENCERATING
    FROM {{ source('FINAL_PROJECT', 'MOVIES_CATALOG_ENRICHED') }}
)

SELECT 
    AUDIENCERATING,
    ROW_NUMBER() OVER (ORDER BY AUDIENCERATING) - 1 AS rating_code  -- Assign a unique number starting from 0
FROM source
{% endmacro %}
