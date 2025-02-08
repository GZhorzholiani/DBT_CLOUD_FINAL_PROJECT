WITH source AS (
    SELECT * FROM {{ source('FINAL_PROJECT', 'ADD_TO_CART') }}
)

SELECT
    "EVENT_NAME" AS add_to_cart_event_name,
    "USER_ID" AS add_to_cart_user_id,
    "ITEM_ID" AS add_to_cart_item_id,
    "CART_ID" AS add_to_cart_cart_id,
    "TIMESTAMP"::TIMESTAMP AS add_to_cart_timestamp
FROM source
