with export_customer_terms as (
select * from {{ source('sql_server', 'Terms') }}
)

select 
    TermsID as terms_id,
    
    Days as discount_days,
    DueDays as due_days,
    DiscountPercent as discount_percent,
    Terms as terms_name
    from export_customer_terms

