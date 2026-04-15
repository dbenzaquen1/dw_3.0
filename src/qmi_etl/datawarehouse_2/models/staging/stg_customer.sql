with
    base_customer as (select * from {{ ref("base_customer") }}),
    base_tax_exempt_reasons as (select * from {{ ref("base_tax_exempt_reason") }}),
    base_tax_code as (select * from {{ ref("base_tax_code") }}),
    base_addresses as (select * from {{ ref("base_addresses") }}),
    base_customer_terms as (select * from {{ ref("base_customer_terms") }})

select
    base_customer.customer_id,
    base_customer.route_id,
    base_customer.tax_code,
    base_customer.address_id,
    base_customer.exempt_reason_code,
    base_customer.tax_id,
    base_customer.balance,
    base_customer.customer_fax,
    base_customer.customer_name,
    base_customer.customer_phone,
    base_customer.customer_email,
    base_customer.customer_website,
    base_customer.customer_comments,
    base_customer.fax_invoice,
    base_customer.email_invoice,
    base_customer.credit_limit,
    base_customer.customer_code,
    base_customer.salesperson_1,
    base_customer.salesperson_2,
    base_customer.sales_comments,
    base_customer.customer_create_date,
    base_customer.has_statements,
    base_tax_code.total_tax_rate,
    base_tax_code.tax_description,
    'usa' as country,
    base_addresses.state,
    base_addresses.city,
    base_addresses.zip_code,
    base_addresses.address_line_1,
    base_addresses.address_line_2,
    base_customer_terms.discount_days,
    base_customer_terms.due_days,
    base_customer_terms.discount_percent,
    base_customer_terms.terms_name,

    case
        when base_customer.exempt_reason_code = 1 then true else false
    end as is_taxable,
    tax_exemption_reason
from base_customer
left join
    base_tax_exempt_reasons
    on base_tax_exempt_reasons.exempt_reason_code = base_customer.exempt_reason_code
left join base_tax_code on base_tax_code.tax_id = base_customer.tax_id
left join base_addresses on base_addresses.address_id = base_customer.address_id
left join base_customer_terms on base_customer_terms.terms_id = base_customer.terms_id