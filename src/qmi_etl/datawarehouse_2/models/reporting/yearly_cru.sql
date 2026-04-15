with yearly_cru as (
    select * from {{ ref('stg_yearly_cru') }}
)

select * from yearly_cru 