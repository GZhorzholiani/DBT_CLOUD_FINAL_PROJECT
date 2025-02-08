WITH source AS (
    SELECT * FROM {{ source('FINAL_PROJECT', 'SIGN_OUT') }}
)

SELECT
    "EVENT_NAME" AS sign_out_event_name,
    "USER_ID" AS sign_out_user_id,
    "TIMESTAMP"::TIMESTAMP AS sign_out_timestamp
FROM source
