WITH source AS (
    SELECT * FROM {{ source('FINAL_PROJECT', 'CHECK_OUT') }}
)

SELECT
    "EVENT_NAME" AS check_out_event_name,
    "USER_ID" AS check_out_user_id,
    "CART_ID" AS check_out_cart_id,
    "PAYMENT_METHOD" AS check_out_payment_method,
    "TIMESTAMP"::TIMESTAMP AS check_out_timestamp
FROM source
