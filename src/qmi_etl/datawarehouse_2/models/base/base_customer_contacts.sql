with customer_contact_export as (
    select * 
    from {{ source('sql_server', 'Contacts') }}
)

select * from customer_contact_export