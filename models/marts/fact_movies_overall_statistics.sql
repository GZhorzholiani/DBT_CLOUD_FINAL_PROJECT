-- models/marts/fact_movie_average_rating.sql

{{ config(
    materialized='table'
) }}

WITH final_data AS (
    SELECT
        mv.movie_rating_code
    FROM {{ ref('stg_movie_statistics') }} mv
),

rating_avg AS (
    SELECT 
        AVG(movie_rating_code) AS avg_rating
    FROM final_data
),

total_revenue AS (
    SELECT 
        SUM(revenue_per_movie) AS total_revenue
    FROM {{ ref('fact_movie_statistics') }}
),

total_views AS (
    SELECT 
        SUM(views_per_movie) AS total_views
    FROM {{ ref('fact_movie_statistics') }}
),

total_movies_sold AS (
    SELECT 
        SUM(sales_per_movie) AS total_movies_sold
    FROM {{ ref('fact_movie_statistics') }}
)

SELECT 
    rm_decoded.AUDIENCERATING AS MOVIES_AVERAGE_RATING,  -- Only return the decoded rating (text)
    tr.total_revenue,  
    tv.total_views,
    ts.total_movies_sold
FROM rating_avg ra
LEFT JOIN {{ ref('rating_mapping') }} rm_decoded
    ON ROUND(ra.avg_rating) = rm_decoded.rating_code  
LEFT JOIN total_revenue tr  
LEFT JOIN total_views tv  
LEFT JOIN total_movies_sold ts  
LIMIT 1
