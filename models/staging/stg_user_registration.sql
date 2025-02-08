WITH source AS (
    SELECT * FROM {{ source('FINAL_PROJECT', 'USER_REGISTRATION') }}
),

deduplicated AS (
    SELECT 
        "USER_ID" AS user_registration_user_id,
        "AGE" AS user_registration_age,
        "MASKED_EMAIL" AS user_registration_masked_email,
        "PREFERRED_LANGUAGE" AS user_registration_preferred_language,
        "TIMESTAMP"::TIMESTAMP AS user_registration_timestamp,
        ROW_NUMBER() OVER (PARTITION BY "USER_ID" ORDER BY "TIMESTAMP" ASC) AS row_num
    FROM source
)

SELECT 
    user_registration_user_id,
    user_registration_age,
    user_registration_masked_email,
    user_registration_preferred_language,
    user_registration_timestamp
FROM deduplicated
WHERE row_num = 1
