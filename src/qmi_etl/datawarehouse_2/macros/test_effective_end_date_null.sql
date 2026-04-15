{% test effective_end_date_null(model, column_name) %}

with validation_errors as (
    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} is null
        or {{ column_name }} = ''
)

select *
from validation_errors

{% endtest %} 