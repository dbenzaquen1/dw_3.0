{% test valid_email_format(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE NOT SAFE.REGEXP_CONTAINS({{ column_name }}, r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')

{% endtest %}
