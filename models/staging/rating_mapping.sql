WITH rating_mapping AS (
    {{ create_rating_mapping() }}
)

SELECT * 
FROM rating_mapping
