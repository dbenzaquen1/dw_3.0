with base_gl_transactions as (
    select * from {{ ref('base_gl_view') }}
)
select * from base_gl_transactions