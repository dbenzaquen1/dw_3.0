with source as (
    select * from {{ ref('base_AR_journal') }}
),
customer as (
    select customer_id, due_days, discount_days from {{ ref('stg_customer') }}
)

select
    source.*,
    DATE_ADD(source.transaction_date, INTERVAL customer.due_days DAY) as due_date,
    DATE_ADD(source.transaction_date, INTERVAL customer.discount_days DAY) as discount_date
    
from source
left join customer on source.customer_id = customer.customer_id 