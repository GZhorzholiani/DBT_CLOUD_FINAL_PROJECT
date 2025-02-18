{{ config(
    materialized='table'
) }}

WITH 
    -- Aggregate Movie Views
    movie_views AS (
        SELECT 
            item_view_item_id AS movie_id,
            COUNT(*) AS total_views
        FROM {{ ref('stg_item_view') }}
        GROUP BY item_view_item_id
    ),

    -- Aggregate Movie Sales
    movie_sales AS (
        SELECT 
            check_out_cart_id AS cart_id,
            check_out_user_id AS user_id,
            ac.add_to_cart_item_id AS movie_id,
            COUNT(*) AS total_sales,
            SUM(CASE WHEN TRY_CAST(m.movie_price AS FLOAT) IS NOT NULL THEN TRY_CAST(m.movie_price AS FLOAT) ELSE 0 END) AS total_revenue
        FROM FINAL_PROJECT.DBT_SCHEMA_FINAL_PROJECT.stg_check_out co
        LEFT JOIN FINAL_PROJECT.DBT_SCHEMA_FINAL_PROJECT.stg_add_to_cart ac 
            ON co.check_out_cart_id = ac.add_to_cart_cart_id
        LEFT JOIN FINAL_PROJECT.DBT_SCHEMA_FINAL_PROJECT.stg_movie_statistics m 
            ON ac.add_to_cart_item_id = m.movie_id
        GROUP BY check_out_cart_id, check_out_user_id, ac.add_to_cart_item_id
    )

-- Final Movie Statistics Table with DISTINCT on movie_id to ensure uniqueness
SELECT DISTINCT
    m.movie_id,
    m.movie_title,
    m.movie_view_timestamp,
    m.movie_checkout_timestamp,
    COALESCE(mv.total_views, 0) AS views_per_movie,
    COALESCE(ms.total_sales, 0) AS sales_per_movie,
    COALESCE(ms.total_revenue, 0) AS revenue_per_movie
FROM {{ ref('stg_movie_statistics') }} m
LEFT JOIN movie_views mv ON m.movie_id = mv.movie_id
LEFT JOIN movie_sales ms ON m.movie_id = ms.movie_id
