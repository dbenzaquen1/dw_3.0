with base_taxcodes as (
    select * from {{ source('sql_server', 'TaxCodes') }}
),
flatten as (
    select 

    TaxID as tax_id,
    TaxCode as tax_code,


    TaxRateCity as city_tax_rate,
    TaxRateCounty as county_tax_rate,
    TaxRateState as state_tax_rate,
    TaxRateTotal as  total_tax_rate,    
    GlSalesTaxPayable as gl_tax_code,
    Description as tax_description 
    from base_taxcodes
)
select * from flatten