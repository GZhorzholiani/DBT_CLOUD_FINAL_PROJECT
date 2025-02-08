WITH user_activity AS (
    SELECT 
        user_id,
        total_sign_ins,
        total_sign_outs,
        total_items_viewed,
        distinct_items_viewed,
        total_items_added_to_cart,
        distinct_items_added_to_cart,
        total_purchases,
        distinct_cart_purchases,
        first_activity_timestamp,
        last_purchase_timestamp
    FROM {{ ref('fact_user_activity') }}
)

SELECT * FROM user_activity
