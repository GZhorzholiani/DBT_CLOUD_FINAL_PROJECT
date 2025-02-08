WITH user_activity AS (
    SELECT * FROM {{ ref('fact_user_activity_aggregated') }}
)

SELECT 
    user_id,
    {{ purchase_segment() }} AS purchase_segment,
    {{ viewing_segment() }} AS viewing_segment,
    {{ engagement_segment() }} AS engagement_segment
FROM user_activity
