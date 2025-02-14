-- models/staging/stg_movie_statistics.sql

WITH source AS (
    SELECT
        ITEM_ID,
        TITLE,
        AUDIENCERATING,
        {{ clean_price('LISTPRICE') }} AS movie_price
    FROM {{ source('FINAL_PROJECT', 'MOVIES_CATALOG_ENRICHED') }}
    WHERE ITEM_ID IS NOT NULL
),

raw_item_view AS (
    SELECT
        item_view_user_id AS user_id,
        item_view_item_id AS item_id,
        item_view_timestamp AS view_timestamp
    FROM {{ ref('stg_item_view') }}
    WHERE item_view_item_id IS NOT NULL
),

raw_checkout AS (
    SELECT
        check_out_user_id AS user_id,
        check_out_cart_id AS item_id,
        check_out_timestamp AS checkout_timestamp
    FROM {{ ref('stg_check_out') }}
    WHERE check_out_cart_id IS NOT NULL
),

final_data AS (
    SELECT
        source.ITEM_ID AS movie_id,
        source.TITLE AS movie_title,
        rm.rating_code AS movie_rating_code,  -- Encoded Rating
        source.movie_price,  -- Use movie_price from the 'source' CTE
        iv.view_timestamp AS movie_view_timestamp,
        co.checkout_timestamp AS movie_checkout_timestamp
    FROM source

    LEFT JOIN rating_mapping rm
    ON source.AUDIENCERATING = rm.AUDIENCERATING

    LEFT JOIN raw_item_view iv
    ON source.ITEM_ID = iv.item_id

    LEFT JOIN raw_checkout co
    ON iv.user_id = co.user_id
)

SELECT 
    fd.*
FROM final_data fd
