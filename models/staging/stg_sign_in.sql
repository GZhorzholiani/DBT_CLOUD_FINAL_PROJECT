WITH source AS (
    SELECT * FROM {{ source('FINAL_PROJECT', 'SIGN_IN') }}
)

SELECT
    "EVENT_NAME" AS sign_in_event_name,
    "USER_ID" AS sign_in_user_id,
    "TIMESTAMP"::TIMESTAMP AS sign_in_timestamp
FROM source
