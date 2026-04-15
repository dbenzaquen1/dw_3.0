with customer as (select * from {{ ref("customer") }})

select
    customer_id as CustomerCode,
    customer_name as OrganizationName,
    credit_limit as CreditLimit,
    balance as CurrentBalance,
    case when is_taxable then 'TRUE' else 'FALSE' end as Taxable,
    'usd' as currency
from customer
