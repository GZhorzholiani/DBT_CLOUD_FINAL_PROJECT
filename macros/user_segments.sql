{% macro purchase_segment() %}
    CASE 
        WHEN total_purchases > 10 THEN 'High Value'
        WHEN total_purchases BETWEEN 5 AND 10 THEN 'Medium Value'
        WHEN total_purchases BETWEEN 1 AND 4 THEN 'Low Value'
        ELSE 'Inactive' 
    END
{% endmacro %}

{% macro viewing_segment() %}
    CASE 
        WHEN total_items_viewed > 50 THEN 'Frequent Viewer'
        WHEN total_items_viewed BETWEEN 20 AND 50 THEN 'Casual Viewer'
        ELSE 'Rare Viewer' 
    END
{% endmacro %}

{% macro engagement_segment() %}
    CASE 
        WHEN total_sign_ins > 20 THEN 'Highly Engaged'
        WHEN total_sign_ins BETWEEN 10 AND 20 THEN 'Moderately Engaged'
        ELSE 'Less Engaged' 
    END
{% endmacro %}
