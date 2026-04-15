with base_customer as (
select * from {{ source('sql_server', 'Customer') }}
)
, flattened_customer as (
    select 
            -- ids
            CustomerID as customer_id,
            RouteID as route_id,
            RegionID as region_id,
            TaxCode1 as tax_code,
            AddressID as address_id,
            TaxCode1Exempt as exempt_reason_code,
            TaxCode1 as tax_id,
            Terms as terms_id,

            -- fact
            case when balance is null then 0 else balance end as balance,
            -- dim
            
            Fax as customer_fax,
            Name as customer_name,
            phone as customer_phone,
            eMail as customer_email,
            Website as customer_website,
            Comments as customer_comments,
            FaxInvoice as fax_invoice,
            eMailInvoice as email_invoice, 
            CreditLimit as credit_limit,
            CustomerCode as customer_code,
            Salesperson1 as salesperson_1,
            Salesperson2 as salesperson_2,
            SalesComments as sales_comments,





            -- dates
            cast(CustCreateDate as date) as customer_create_date,
            -- flags 
            Statements as has_statements,


    from  base_customer
)
select * from flattened_customer