WITH source AS (
    SELECT * FROM {{ source('FINAL_PROJECT', 'ITEM_VIEW') }}
)

SELECT
    "EVENT_NAME" AS item_view_event_name,
    "USER_ID" AS item_view_user_id,
    "ITEM_ID" AS item_view_item_id,
    "TIMESTAMP"::TIMESTAMP AS item_view_timestamp
FROM source
