with cru_seed as (
    select * from {{ ref('seed_cru') }}
)
, reformat_headers as (
    select 
        case when Commodity_Group = 'Steel - Carbon' then 'Steel' else null end as commodity_group,
        case 
            when Price_Detail like '%Spot price%' then 'Spot Price'
            when Price_Detail like '%Spot spread%' then 'Spot Price'
            else null 
        end as pricing_type,
        case 
            when Price_Detail like '%Hot-dipped Galvanised Coil%' then 'Hot Dipped Galvanised'
            when Price_Detail like '%Cold-rolled Coil%' then 'Cold-rolled Coil'
            when Price_Detail like '%Hot-rolled Coil%' then 'Hot-rolled Coil'
            when Price_Detail like '%Electrogalvanised Coil,%' then 'Electrogalvanised Coil,'
            else null 
        end as metal_sub_type,
        case 
            when Price_Detail like '%Coating extra%' and Price_Detail like '%Hot-dipped Galvanised Coil%' then 'Coating Incuded' 
            when Price_Detail like '%Base%' and Price_Detail like '%Hot-dipped Galvanised Coil%' then 'Base'
            else 'Standard' 
        end as hot_dipped_pricing_type,
        Price_Type as price_type,
        Market as market,
        Unit_of_Measurement as unit_of_measurement,
        PRICEID as price_id,
        Date as report_date,
        Value as price_value,
        case
            when date like '%Q%' then 'Quarterly'
            when date like '%M%' then 'Monthly'
            when NOT REGEXP_CONTAINS(date, r'[A-Za-z]') then 'Yearly'
            when date like '%W%' then 'Weekly'
            else null
        end as date_frequency
    from cru_seed
)
select * from reformat_headers

