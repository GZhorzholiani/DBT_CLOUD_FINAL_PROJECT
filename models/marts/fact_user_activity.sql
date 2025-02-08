WITH user_registration AS (
    SELECT 
        user_registration_user_id AS user_id,
        user_registration_age,
        user_registration_masked_email,
        user_registration_preferred_language,
        {{ min_timestamp('user_registration_timestamp') }} AS first_activity_timestamp
    FROM {{ ref('stg_user_registration') }}
    GROUP BY user_registration_user_id, 
             user_registration_age, 
             user_registration_masked_email, 
             user_registration_preferred_language
),

sign_in AS (
    SELECT 
        sign_in_user_id AS user_id,
        {{ count_distinct('sign_in_timestamp') }} AS total_sign_ins
    FROM {{ ref('stg_sign_in') }}
    GROUP BY sign_in_user_id
),

sign_out AS (
    SELECT 
        sign_out_user_id AS user_id,
        {{ count_distinct('sign_out_timestamp') }} AS total_sign_outs
    FROM {{ ref('stg_sign_out') }}
    GROUP BY sign_out_user_id
),

item_view AS (
    SELECT 
        item_view_user_id AS user_id,
        {{ count_distinct('item_view_timestamp') }} AS total_items_viewed,
        {{ count_distinct('item_view_item_id') }} AS distinct_items_viewed
    FROM {{ ref('stg_item_view') }}
    GROUP BY item_view_user_id
),

add_to_cart AS (
    SELECT 
        add_to_cart_user_id AS user_id,
        {{ count_distinct('add_to_cart_timestamp') }} AS total_items_added_to_cart,
        {{ count_distinct('add_to_cart_item_id') }} AS distinct_items_added_to_cart
    FROM {{ ref('stg_add_to_cart') }}
    GROUP BY add_to_cart_user_id
),

checkout AS (
    SELECT 
        check_out_user_id AS user_id,
        {{ count_distinct('check_out_timestamp') }} AS total_purchases,
        {{ count_distinct('check_out_cart_id') }} AS distinct_cart_purchases,
        {{ max_timestamp('check_out_timestamp') }} AS last_purchase_timestamp
    FROM {{ ref('stg_check_out') }}
    GROUP BY check_out_user_id
)

SELECT 
    ur.user_id,
    ur.user_registration_age,
    ur.user_registration_masked_email,
    ur.user_registration_preferred_language,
    ur.first_activity_timestamp,

    COALESCE(sign_in.total_sign_ins, 0) AS total_sign_ins,
    COALESCE(sign_out.total_sign_outs, 0) AS total_sign_outs,
    COALESCE(item_view.total_items_viewed, 0) AS total_items_viewed,
    COALESCE(item_view.distinct_items_viewed, 0) AS distinct_items_viewed,
    COALESCE(add_to_cart.total_items_added_to_cart, 0) AS total_items_added_to_cart,
    COALESCE(add_to_cart.distinct_items_added_to_cart, 0) AS distinct_items_added_to_cart,
    COALESCE(checkout.total_purchases, 0) AS total_purchases,
    COALESCE(checkout.distinct_cart_purchases, 0) AS distinct_cart_purchases,
    COALESCE(checkout.last_purchase_timestamp, NULL) AS last_purchase_timestamp

FROM user_registration ur
{{ left_join('ur', 'sign_in', 'user_id', 'user_id') }}
{{ left_join('ur', 'sign_out', 'user_id', 'user_id') }}
{{ left_join('ur', 'item_view', 'user_id', 'user_id') }}
{{ left_join('ur', 'add_to_cart', 'user_id', 'user_id') }}
{{ left_join('ur', 'checkout', 'user_id', 'user_id') }}
