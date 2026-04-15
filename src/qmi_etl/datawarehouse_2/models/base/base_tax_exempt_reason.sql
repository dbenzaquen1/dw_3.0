with base_tax_exempt_reasons as (
    select * from {{ source('sql_server', 'TaxExemptReasons') }}
)

select
    ExemptReasonCode as exempt_reason_code,
    Description as tax_exemption_reason
from base_tax_exempt_reasons