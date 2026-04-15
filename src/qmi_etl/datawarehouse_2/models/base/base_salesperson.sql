with base_salesperson as (
    select * from {{ source('sql_server', 'SalesPerson') }}
),
reduced_salesperson as (
    select
        -- ids
        salespersonid as salesperson_id,
        -- fact
        creditmanagerlimit as credit_limit,
        -- dim
        username,
        fname as first_name,
        lname as last_name,
        fname || ' ' || lname as full_name,
        email as email,
        -- flags 
        inactive as is_active
    from base_salesperson
)

select *
from reduced_salesperson
